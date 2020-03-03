/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2019 ScyllaDB
 */

#include <seastar/core/fair_queue.hh>
#include <seastar/core/future.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/core/reactor.hh>
#include <queue>
#include <chrono>
#include <unordered_set>
#include <cmath>
#include <mutex>

#include <fmt/format.h>

namespace seastar {

void fair_queue::push_priority_class(priority_class_ptr pc) {
    if (!pc->_queued) {
        _handles.push(pc);
        pc->_queued = true;
    }
}

priority_class_ptr fair_queue::pop_priority_class() {
    if (_handles.empty()) {
        return nullptr;
    }
    auto h = _handles.top();
    _handles.pop();
    assert(h->_queued);
    h->_queued = false;
    return h;
}

float fair_queue::normalize_factor() const {
    return std::numeric_limits<float>::min();
}

void fair_queue::normalize_stats() {
    auto time_delta = std::log(normalize_factor()) * _config.tau;
    // time_delta is negative; and this may advance _base into the future
    _base -= std::chrono::duration_cast<clock_type::duration>(time_delta);
    for (auto& pc: _all_classes) {
        pc._accumulated *= normalize_factor();
    }
}

void fair_queue::add_capacity(ticket& t) {
    _config.capacity += t.quantity;
    _config.max_req_count += t.weight;
    _config.max_bytes_count += t.size;

    fmt::print("Adding capacity {}, weight {}, size {}\n", t.quantity, t.weight, t.size);
}

void fair_queue::remove_capacity(ticket& t) {
    _config.capacity -= t.quantity;
    _config.max_req_count -= t.weight;
    _config.max_bytes_count -= t.size;
}

bool fair_queue::can_dispatch() const {
    // FIXME: handle the 0-case in a different way. This may allow too much.
    return !_handles.empty() &&
           (_requests_executing.load(std::memory_order_relaxed) <= _config.capacity) &&
           (_req_count_executing.load(std::memory_order_relaxed) <= _config.max_req_count) &&
           (_bytes_count_executing.load(std::memory_order_relaxed) <= _config.max_bytes_count);
}

fair_queue::fair_queue(config cfg)
    : _config(std::move(cfg))
    , _base(std::chrono::steady_clock::now())
{
    for (size_t i = 0; i < _max_classes; ++i) {
        _available_classes.push(&_all_classes[i]);
    }
}

priority_class_ptr fair_queue::register_priority_class(uint32_t shares) {
    std::lock_guard<util::spinlock> g(_fair_queue_lock);

    if (_available_classes.empty()) {
        throw std::runtime_error("No more room for new I/O priority classes");
    }

    auto ptr = _available_classes.top();
    ptr->update_shares(shares);
    _available_classes.pop();
    return ptr;
}

void fair_queue::unregister_priority_class(priority_class_ptr pclass) {
    std::lock_guard<util::spinlock> g(_fair_queue_lock);
    _available_classes.push(pclass);
}

size_t fair_queue::waiters() const {
    return _requests_queued.load(std::memory_order_relaxed);
}

ticket fair_queue::waiting() const {
    ticket t;
    t.quantity = _requests_queued.load(std::memory_order_relaxed);
    t.weight = _req_count_queued.load(std::memory_order_relaxed);
    t.size = _bytes_count_queued.load(std::memory_order_relaxed);
    return t;
}

ticket fair_queue::prune_excess_capacity() {
    // FIXME: max_XXX is used in the req cost formula, so it should not change.
    ticket t;

    auto a = _config.natural_capacity;
    auto b = _requests_executing.load(std::memory_order_relaxed);
    auto c = _config.capacity;

    auto capacity = std::max(_config.natural_capacity,
                    _requests_executing.load(std::memory_order_relaxed));

    auto req_count = std::max(_config.natural_max_req_count,
                    _req_count_executing.load(std::memory_order_relaxed));


    auto bytes_count = std::max(_config.natural_max_bytes_count,
                    _bytes_count_executing.load(std::memory_order_relaxed));

    if (_config.capacity > capacity) {
        auto diff = _config.capacity - capacity;
        t.quantity = diff;
        _config.capacity -= diff;
    }

    if (_config.max_req_count > req_count) {
        auto diff = _config.max_req_count - req_count;
        t.weight = diff;
        _config.max_req_count -= diff;
    }

    if (_config.max_bytes_count > bytes_count) {
        auto diff = _config.max_bytes_count - bytes_count;
        t.size = diff;
        _config.max_bytes_count -= diff;
    }
    if (t.quantity || t.weight || t.size) {
        fmt::print("Pruned {} , {} {},  (quantity comp: cap {} exec {}, natural {})\n", t.quantity, t.weight, t.size, c, b, a);
    }
    return t;
}

ticket fair_queue::prune_all_capacity() {
    // FIXME: handle negative values fairly
    ticket t;

    auto capacity = _requests_executing.load(std::memory_order_relaxed);
    auto req_count = _req_count_executing.load(std::memory_order_relaxed);
    auto bytes_count = _bytes_count_executing.load(std::memory_order_relaxed);

    if (capacity <= _config.capacity) {
        t.quantity = _config.capacity - capacity;
        _config.capacity = capacity;
    }

    if (req_count > _config.max_req_count) {
        t.weight = _config.max_req_count - req_count;
        _config.max_req_count = req_count;
    }

    if (bytes_count > _config.max_bytes_count) {
        t.size = _config.max_bytes_count - bytes_count;
        _config.max_bytes_count = bytes_count;
    }

    return t;
}

size_t fair_queue::requests_currently_executing() const {
    return _requests_executing.load(std::memory_order_relaxed);
}

void fair_queue::multishard_queue(priority_class_ptr pc) {
    std::lock_guard<util::spinlock> g(_fair_queue_lock);
    push_priority_class(pc);
}

void fair_queue::multishard_register(priority_class_ptr pc, fair_queue_request_descriptor* desc) {
    pc->_queue.push(desc);
}

void fair_queue::queue(priority_class_ptr pc, fair_queue_request_descriptor* desc) {
    pc->_queue.push(desc);

    _requests_queued.fetch_add(1, std::memory_order_relaxed);
    _req_count_queued.fetch_add(desc->weight, std::memory_order_relaxed);
    _bytes_count_queued.fetch_add(desc->size, std::memory_order_relaxed);


    // We need to return a future in this function on which the caller can wait.
    // Since we don't know which queue we will use to execute the next request - if ours or
    // someone else's, we need a separate promise at this point.
    push_priority_class(pc);
}

void fair_queue::notify_requests_finished(fair_queue_request_descriptor& desc, unsigned requests) {
    _requests_executing.fetch_sub(requests, std::memory_order_relaxed);
    _req_count_executing.fetch_sub(desc.weight, std::memory_order_relaxed);
    _bytes_count_executing.fetch_sub(desc.size, std::memory_order_relaxed);
}

void fair_queue::update_cost(priority_class_ptr h, float weight, float size) {
    auto delta = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - _base);
    auto req_cost  = (weight / _config.max_req_count + size / _config.max_bytes_count) / h->_shares;
    auto cost  = expf(1.0f/_config.tau.count() * delta.count()) * req_cost;
    float next_accumulated = h->_accumulated + cost;
    while (std::isinf(next_accumulated)) {
        normalize_stats();
        // If we have renormalized, our time base will have changed. This should happen very infrequently
        delta = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - _base);
        cost  = expf(1.0f/_config.tau.count() * delta.count()) * req_cost;
        next_accumulated = h->_accumulated + cost;
    }
    h->_accumulated = next_accumulated;
}

void fair_queue::multishard_dispatch_requests() {
    auto dispatch = _fair_queue_lock.try_lock();
    if (!dispatch) {
        return;
    }

    while (can_dispatch()) {
        priority_class_ptr h;
        do {
            h = pop_priority_class();
        } while (h && h->_queue.empty());
        if (!h) {
            break;
        }

        auto* req = h->_queue.front();

        // FIXME: Maybe defer it until it actually uses it.
        ticket avail;
        avail.quantity = _config.capacity - _requests_executing.load(std::memory_order_relaxed);
        avail.weight = _config.max_req_count - _req_count_executing.load(std::memory_order_relaxed);
        avail.size = _config.max_bytes_count - _bytes_count_executing.load(std::memory_order_relaxed);
        fmt::print("Dispatching requests. Availability: {} {} {}, req addr {:x}\n", avail.quantity, avail.weight, avail.size, uint64_t(req));

        // FIXME: add quantity to the request, or maybe something else
        avail.quantity = std::min(avail.quantity, int64_t(req->weight));
        avail.weight = std::min(avail.weight , int64_t(req->weight));
        avail.size = std::min(avail.size, int64_t(req->size));


        fmt::print("Dispatching requests. Updated Availability: {} {} {}\n", avail.quantity, avail.weight, avail.size);

        update_cost(h, avail.weight, avail.size);
        (*req)(avail);

        _requests_executing.fetch_add(avail.quantity, std::memory_order_relaxed);
        _req_count_executing.fetch_add(avail.weight, std::memory_order_relaxed);
        _bytes_count_executing.fetch_add(avail.size, std::memory_order_relaxed);
    }
    _fair_queue_lock.unlock();
}


size_t fair_queue::dispatch_requests() {
    auto ret = 0;

    if (waiters()) {
        fmt::print("I have waiters. Can dispatch? {}. Why ? hndl {} / R: {} {}, RR: {} {} , w: {} {}\n", can_dispatch(),
                _handles.size(), 
           _requests_executing.load(std::memory_order_relaxed) ,  _config.capacity,
           _req_count_executing.load(std::memory_order_relaxed) , _config.max_req_count,
           _bytes_count_executing.load(std::memory_order_relaxed) , _config.max_bytes_count
        );
    }

                
    while (can_dispatch()) {
        priority_class_ptr h;
        do {
            h = pop_priority_class();
        } while (h && h->_queue.empty());
        if (!h) {
            break;
        }

        ret++;
        auto* req = h->_queue.front();
        _requests_queued.fetch_sub(1, std::memory_order_relaxed);
        _req_count_queued.fetch_sub(req->weight, std::memory_order_relaxed);
        _bytes_count_queued.fetch_sub(req->size, std::memory_order_relaxed);

        update_cost(h, req->weight, req->size);

        ticket avail;
        avail.quantity = _config.capacity - _requests_executing.load(std::memory_order_relaxed);
        avail.weight = _config.max_req_count - _req_count_executing.load(std::memory_order_relaxed);
        avail.size = _config.max_bytes_count - _bytes_count_executing.load(std::memory_order_relaxed);
        auto t = (*req)(avail);

        h->_queue.pop();

        if (!h->_queue.empty()) {
            push_priority_class(h);
        }

        _requests_executing.fetch_add(t.quantity, std::memory_order_relaxed);
        _req_count_executing.fetch_add(t.weight, std::memory_order_relaxed);
        _bytes_count_executing.fetch_add(t.size, std::memory_order_relaxed);
    }
    return ret;
}

void fair_queue::update_shares(priority_class_ptr pc, uint32_t new_shares) {
    pc->update_shares(new_shares);
}

}

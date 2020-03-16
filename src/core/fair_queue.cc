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
    assert(!_handles.empty());
    auto h = _handles.top();
    _handles.pop();
    assert(h->_queued);
    h->_queued = false;
    return static_cast<priority_class_ptr>(h);
}

float basic_fair_queue::normalize_factor() const {
    return std::numeric_limits<float>::min();
}

void basic_fair_queue::normalize_stats() {
    auto time_delta = std::log(normalize_factor()) * _tau;
    // time_delta is negative; and this may advance _base into the future
    _base -= std::chrono::duration_cast<clock_type::duration>(time_delta);
    for (auto& pc: _registered_classes) {
        pc->_accumulated *= normalize_factor();
    }
}

void fair_queue::add_capacity(ticket& t) {
    _current_capacity += t.quantity;
    _current_max_req_count += t.weight;
    _current_max_bytes_count += t.size;
}

void fair_queue::remove_capacity(ticket& t) {
    _current_capacity -= t.quantity;
    _current_max_req_count -= t.weight;
    _current_max_bytes_count -= t.size;
}

bool basic_fair_queue::can_dispatch() const {
    // FIXME: handle the 0-case in a different way. This may allow too much.
    return !_handles.empty() &&
           (_requests_executing <= _current_capacity) &&
           (_req_count_executing <= _current_max_req_count) &&
           (_bytes_count_executing <= _current_max_bytes_count);
}

fair_queue::fair_queue(basic_fair_queue::config cfg)
    : basic_fair_queue(std::move(cfg))
{
    for (size_t i = 0; i < _max_classes; ++i) {
        _available_classes.push(&_all_classes[i]);
    }
}

fair_queue::fair_queue(multishard_fair_queue* parent)
    : basic_fair_queue(parent->_config) 
    // FIXME: if this comes inside the fair queue, it will have to be a whole more generic than it
    // currently is. For instance g_m_p assumes one per shard.
    , _multishard_fq(parent)
    , _multishard_pclass(_multishard_fq->get_multishard_priority_class())
{
    for (size_t i = 0; i < _max_classes; ++i) {
        _available_classes.push(&_all_classes[i]);
    }
}

basic_fair_queue::basic_fair_queue(config cfg)
    : _base(std::chrono::steady_clock::now())
    , _config(std::move(cfg))
{}

multishard_fair_queue::multishard_fair_queue(multishard_fair_queue::config cfg)
    : basic_fair_queue(std::move(cfg)) 
{}

multishard_priority_class_ptr
multishard_fair_queue::get_multishard_priority_class() {
    std::lock_guard<util::spinlock> g(_dispatch_lock);
    auto ptr = &_all_classes[engine().cpu_id()];
    _registered_classes.insert(ptr);
    return ptr;
}

priority_class_ptr fair_queue::register_priority_class(uint32_t shares) {
    if (_available_classes.empty()) {
        throw std::runtime_error("No more room for new I/O priority classes");
    }

    auto ptr = _available_classes.top();
    ptr->update_shares(shares);
    _available_classes.pop();
    _registered_classes.insert(ptr);
    return ptr;
}

void fair_queue::unregister_priority_class(priority_class_ptr pclass) {
    _registered_classes.erase(pclass);
    _available_classes.push(pclass);
}

size_t fair_queue::waiters() const {
    return _requests_queued;
}

ticket fair_queue::waiting() const {
    ticket t;
    t.quantity = _requests_queued;
    t.weight = _req_count_queued;
    t.size = _bytes_count_queued;
    return t;
}

ticket fair_queue::prune_excess_capacity() {
    ticket t;

    if (_current_capacity > _requests_executing) {
        auto diff = _current_capacity - _requests_executing;
        t.quantity = diff;
        _current_capacity -= diff;
    }

    if (_current_max_req_count > _req_count_executing) {
        auto diff = _config.max_req_count - _req_count_executing;
        t.weight = diff;
        _current_max_req_count -= diff;
    }

    if (_current_max_bytes_count > _bytes_count_executing) {
        auto diff = _config.max_bytes_count - _bytes_count_executing;
        t.size = diff;
        _current_max_bytes_count -= diff;
    }
    return t;
}

ticket fair_queue::prune_all_capacity() {
    ticket t;

    if (_requests_executing <= _current_capacity) {
        t.quantity = _current_capacity - _requests_executing;
        _config.capacity = _requests_executing;
    }

    if (_req_count_executing > _config.max_req_count) {
        t.weight = _current_max_req_count - _req_count_executing;
        _current_max_req_count = _req_count_executing;
    }

    if (_bytes_count_executing > _current_max_bytes_count) {
        t.size = _current_max_bytes_count - _bytes_count_executing;
        _current_max_bytes_count = _bytes_count_executing;
    }

    return t;
}

size_t fair_queue::requests_currently_executing() const {
    return _requests_executing;
}

void fair_queue::queue(priority_class_ptr pc, fair_queue_request_descriptor* desc) {
    pc->_queue.push(desc);

    _requests_queued++;
    _req_count_queued += desc->weight;
    _bytes_count_queued += desc->size;

    // We need to return a future in this function on which the caller can wait.
    // Since we don't know which queue we will use to execute the next request - if ours or
    // someone else's, we need a separate promise at this point.
    push_priority_class(pc);
}

void fair_queue::notify_requests_finished(fair_queue_request_descriptor& desc, unsigned requests) {
    _requests_executing -= requests;
    _req_count_executing -= desc.weight;
    _bytes_count_executing -= desc.size;
}

void basic_fair_queue::update_cost(basic_priority_class* h, float weight, float size) {
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

void fair_queue::dispatch_requests() {
    if (!_multishard_fq) {
        do_dispatch_requests();
        return;
    }

    // Step 1: Set our limits back to our natural quota. If we don't do that, once we
    // dispatch a lot we will keep dispatching a lot and will be unfair to the other
    // shards.
    auto extra = prune_excess_capacity();
    _multishard_fq->notify_requests_finished(extra);

    // Step 2: Dispatch the requests we currently have, within our quota.
    do_dispatch_requests();

    // Step 3: If we still have waiters, grab capacity from the I/O queue
    // if possible and try to dispatch. Maybe capacity that we requested
    // in the last poll period only got ready now and we try with that first.
    if (waiters()) {
        auto t = _multishard_pclass->receive();
        add_capacity(t);
        do_dispatch_requests();
    }

    // Step 3: If we still have waiters, try to acquire more capacity from
    // the I/O queue. If there is extra capacity we will be able to dispatch
    // more right away. If not, we may consume it in the next poll period.
    if (waiters()) {
        extra = waiting();
        _multishard_pclass->ask_for(extra);

        // FIXME: merge with dispatch
        _multishard_fq->queue(_multishard_pclass);
        _multishard_fq->dispatch_requests();
        do_dispatch_requests();
    }
    
    // Step 4: if we have no more waiters, give back the capacity we grabbed
    // but didn't use
    if (!waiters()) {
        extra = prune_all_capacity();
        _multishard_fq->notify_requests_finished(extra);
    }

}

void fair_queue::do_dispatch_requests() {
    while (can_dispatch()) {
        priority_class_ptr h;
        do {
            h = pop_priority_class();
        } while (h->_queue.empty());

        auto* req = h->_queue.front();
        _requests_queued--;
        _req_count_queued -= req->weight;
        _bytes_count_queued -= req->size;

        update_cost(h, req->weight, req->size);

        ticket avail;
        avail.quantity = _config.capacity - _requests_executing;
        avail.weight = _config.max_req_count - _req_count_executing;
        avail.size = _config.max_bytes_count - _bytes_count_executing;
        auto t = (*req)(avail);

        h->_queue.pop();

        if (!h->_queue.empty()) {
            push_priority_class(h);
        }

        _requests_executing += t.quantity;
        _req_count_executing += t.weight;
        _bytes_count_executing += t.size;
    }
}

void fair_queue::update_shares(priority_class_ptr pc, uint32_t new_shares) {
    pc->update_shares(new_shares);
}

void multishard_priority_class::grant(ticket& t) {
    _quantity.fetch_sub(t.quantity, std::memory_order_relaxed);
    _weight.fetch_sub(t.weight, std::memory_order_relaxed);
    _size.fetch_sub(t.size, std::memory_order_release);

    _pending_quantity.fetch_add(t.quantity, std::memory_order_relaxed);
    _pending_weight.fetch_add(t.weight, std::memory_order_relaxed);
    _pending_size.fetch_add(t.size, std::memory_order_release);
}

ticket multishard_priority_class::receive() {
    ticket t;
    t.quantity = _pending_quantity.exchange(0, std::memory_order_relaxed);
    t.weight = _pending_weight.exchange(0, std::memory_order_relaxed);
    t.size = _pending_size.exchange(0, std::memory_order_release);
    return t;
}

void multishard_priority_class::ask_for(ticket& t) {
    _size.fetch_add(t.size, std::memory_order_acquire);
    _quantity.fetch_add(t.quantity, std::memory_order_relaxed);
    _weight.fetch_add(t.weight, std::memory_order_relaxed);
}

void multishard_fair_queue::queue(multishard_priority_class_ptr pc) {
    if (pc->_dirty.exchange(true, std::memory_order_release)) {
        _dirty.push(pc);
    }
}

void multishard_fair_queue::dispatch_requests() {
    auto dispatch = _dispatch_lock.try_lock();
    if (!dispatch) {
        return;
    }

    _dirty.consume_all([this] (multishard_priority_class_ptr ptr) {
        if (ptr) {
            push_priority_class(ptr);
            ptr->_dirty.store(false, std::memory_order_acquire);
        }
    });

    while (can_dispatch()) {
        multishard_priority_class_ptr h;
        do {
            h = pop_priority_class();
        } while (h);

        if (!h) {
            break;
        }

        // FIXME: Maybe defer it until it actually uses it.
        ticket avail;
        avail.quantity = _config.capacity - _requests_executing;
        avail.weight = _config.max_req_count - _req_count_executing;
        avail.size = _config.max_bytes_count - _bytes_count_executing;

        // FIXME: add quantity to the request, or maybe something else
        avail.size = std::min(avail.size, h->_size.load(std::memory_order_acquire));
        avail.quantity = std::min(avail.quantity, h->_quantity.load(std::memory_order_relaxed));
        avail.weight = std::min(avail.weight , h->_weight.load(std::memory_order_relaxed));

        update_cost(h, avail.weight, avail.size);
        h->grant(avail);

        _requests_executing += avail.quantity;
        _req_count_executing += avail.weight;
        _bytes_count_executing += avail.size;
    }
    _dispatch_lock.unlock();
}

// FIXME: candidates for common class
void multishard_fair_queue::push_priority_class(basic_priority_class* pc) {
    if (!pc->_queued) {
        _handles.push(pc);
        pc->_queued = true;
    }
}

// FIXME : candidate for common class.
multishard_priority_class_ptr multishard_fair_queue::pop_priority_class() {
    if (_handles.empty()) {
        return nullptr;
    }
    auto h = _handles.top();
    _handles.pop();
    assert(h->_queued);
    h->_queued = false;
    return static_cast<multishard_priority_class_ptr>(h);
}
}

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
#include <queue>
#include <chrono>
#include <unordered_set>
#include <cmath>
#include <mutex>

namespace seastar {

void basic_fair_queue::push_priority_class(basic_priority_class* pc) {
    if (!pc->_queued) {
        _handles.push(pc);
        pc->_queued = true;
    }
}

basic_priority_class* basic_fair_queue::pop_priority_class() {
    assert(!_handles.empty());
    auto h = _handles.top();
    _handles.pop();
    assert(h->_queued);
    h->_queued = false;
    return h;
}

float basic_fair_queue::normalize_factor() const {
    return std::numeric_limits<float>::min();
}

void basic_fair_queue::normalize_stats() {
    auto time_delta = std::log(normalize_factor()) * _config.tau;
    // time_delta is negative; and this may advance _base into the future
    _base -= std::chrono::duration_cast<clock_type::duration>(time_delta);
    for (auto& pc: _registered_classes) {
        pc->_accumulated *= normalize_factor();
    }
}

bool basic_fair_queue::can_dispatch() const {
    return !_handles.empty() &&
           (_requests_executing < _current_capacity) &&
           (_req_count_executing < _current_max_req_count) &&
           (_bytes_count_executing < _current_max_bytes_count);
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

size_t basic_fair_queue::requests_currently_executing() const {
    return _requests_executing;
}

void basic_fair_queue::add_capacity(fair_queue_credit& c) {
    if (((c.quantity + _current_capacity) > _config.capacity) ||
       ((c.weight + _current_max_req_count) > _config.max_req_count) ||
       ((c.size + _current_max_bytes_count) > _config.max_bytes_count)) {
        throw std::out_of_range("Trying to add to basic_fair_queue more capacity than it can handle");
    }

    _current_capacity += c.quantity;
    _current_max_req_count += c.weight;
    _current_max_bytes_count += c.size;
}

void basic_fair_queue::remove_capacity(fair_queue_credit& c) {
    if ((c.quantity > _current_capacity) ||
       (c.weight > _current_max_req_count) ||
       (c.size > _current_max_bytes_count)) {
        throw std::out_of_range("Trying to remove to basic_fair_queue more capacity than it has");
    }

    _current_capacity -= c.quantity;
    _current_max_req_count -= c.weight;
    _current_max_bytes_count -= c.size;
}

fair_queue_credit basic_fair_queue::prune_unused_capacity() {
    fair_queue_credit c;
    assert(_current_capacity >= _requests_executing);
    assert(_current_max_req_count >= _req_count_executing);
    assert(_current_max_bytes_count >= _bytes_count_executing);

    c.quantity = _current_capacity - _requests_executing;
    c.weight = _current_max_req_count - _req_count_executing;
    c.size = _current_max_bytes_count - _bytes_count_executing;
    _current_capacity = _requests_executing;
    _current_max_req_count = _req_count_executing;
    _current_max_bytes_count = _bytes_count_executing;
    return c;
}

void basic_fair_queue::return_capacity(fair_queue_credit& c) {
    _requests_executing -= c.quantity;
    _req_count_executing -= c.weight;
    _bytes_count_executing -= c.size;

    if (_reservoir) {
        remove_capacity(c);
        _reservoir->return_capacity(c);
    }
}

void bulk_priority_class::receive_grant(fair_queue_credit& c) {
    _quantity.fetch_sub(c.quantity, std::memory_order_relaxed);
    _weight.fetch_sub(c.weight, std::memory_order_relaxed);
    _size.fetch_sub(c.size, std::memory_order_release);

    _pending_quantity.fetch_add(c.quantity, std::memory_order_relaxed);
    _pending_weight.fetch_add(c.weight, std::memory_order_relaxed);
    _pending_size.fetch_add(c.size, std::memory_order_release);
}

fair_queue_credit bulk_priority_class::consume_credit() {
    fair_queue_credit c;
    c.quantity = _pending_quantity.exchange(0, std::memory_order_relaxed);
    c.weight = _pending_weight.exchange(0, std::memory_order_relaxed);
    c.size = _pending_size.exchange(0, std::memory_order_release);
    return c;
}

void bulk_priority_class::request_credit(fair_queue_credit& c) {
    _size.fetch_add(c.size, std::memory_order_acquire);
    _quantity.fetch_add(c.quantity, std::memory_order_relaxed);
    _weight.fetch_add(c.weight, std::memory_order_relaxed);
}

bulk_fair_queue::bulk_fair_queue(config cfg)
    : basic_fair_queue(std::move(cfg))
{
    std::lock_guard<util::spinlock> g(_lock);
    for (auto& c : _all_classes) {
        _available_classes.push(&c);
    }
}

bulk_priority_class_ptr
bulk_fair_queue::register_priority_class(uint32_t shares) {
    std::lock_guard<util::spinlock> g(_lock);
    auto ptr = _available_classes.top();
    ptr->_shares = shares;
    _available_classes.pop();
    _registered_classes.insert(ptr);
    return ptr;
}

void
bulk_fair_queue::unregister_priority_class(bulk_priority_class_ptr ptr) {
    std::lock_guard<util::spinlock> g(_lock);
    _available_classes.push(ptr);
    _registered_classes.erase(ptr);
}

void bulk_fair_queue::queue(bulk_priority_class_ptr pc) {
    // It is expected that push can happen in one shard while another
    // shard has the bulk_fair_queue locked. So we need to do as little
    // as possible at queue time. We just push this into a queue so that
    // the dispatcher will know there are others waiting.
    if (!pc->_dirty.exchange(true, std::memory_order_release)) {
        _dirty.push(pc);
    }
}

void bulk_fair_queue::reap_queued_classes() {
    _dirty.consume_all([this] (bulk_priority_class_ptr ptr) {
        if (ptr) {
            push_priority_class(ptr);
            ptr->_dirty.store(false, std::memory_order_acquire);
        }
    });
}

void bulk_fair_queue::dispatch_requests() {
    auto dispatch = _lock.try_lock();
    if (!dispatch) {
        return;
    }

    reap_queued_classes();
    while (can_dispatch()) {
        bulk_priority_class_ptr h = static_cast<bulk_priority_class_ptr>(pop_priority_class());

        fair_queue_credit avail;
        avail.quantity = _config.capacity - _requests_executing;
        avail.weight = _config.max_req_count - _req_count_executing;
        avail.size = _config.max_bytes_count - _bytes_count_executing;

        avail.size = std::min(avail.size, h->_size.load(std::memory_order_acquire));
        avail.quantity = std::min(avail.quantity, h->_quantity.load(std::memory_order_relaxed));
        avail.weight = std::min(avail.weight , h->_weight.load(std::memory_order_relaxed));

        update_cost(h, avail.weight, avail.size);
        h->receive_grant(avail);

        _requests_executing += avail.quantity;
        _req_count_executing += avail.weight;
        _bytes_count_executing += avail.size;

        reap_queued_classes();
    }
    _lock.unlock();
}

basic_fair_queue::basic_fair_queue(bulk_fair_queue& fq)
    : _current_capacity(0)
    , _current_max_req_count(0)
    , _current_max_bytes_count(0)
    , _reservoir(&fq)
    , _rptr(_reservoir->register_priority_class(1))
{
}

priority_class_ptr fair_queue::register_priority_class(uint32_t shares) {
    priority_class_ptr pclass = make_lw_shared<priority_class>(shares);
    _all_classes.insert(pclass);
    _registered_classes.insert(&*pclass);
    return pclass;
}

void fair_queue::unregister_priority_class(priority_class_ptr pclass) {
    assert(pclass->_queue.empty());
    _all_classes.erase(pclass);
    _registered_classes.erase(&*pclass);
}

size_t fair_queue::waiters() const {
    return _requests_queued;
}

fair_queue_credit fair_queue::credit_waiting() const {
    fair_queue_credit c;
    c.quantity = _requests_queued;
    c.weight = _req_count_queued;
    c.size = _bytes_count_queued;
    return c;
}

void fair_queue::queue(priority_class_ptr pc, fair_queue_request_descriptor desc, noncopyable_function<void()> func) {
    // We need to return a future in this function on which the caller can wait.
    // Since we don't know which queue we will use to execute the next request - if ours or
    // someone else's, we need a separate promise at this point.
    push_priority_class(&*pc);
    assert(desc.size);
    assert(desc.weight);

    pc->_queue.push_back(priority_class::request{std::move(func), std::move(desc)});
    _req_count_queued += desc.weight;
    _bytes_count_queued += desc.size;
    _requests_queued++;
}

void fair_queue::notify_requests_finished(fair_queue_request_descriptor& desc) {
    _requests_executing--;
    _req_count_executing -= desc.weight;
    _bytes_count_executing -= desc.size;
}

void fair_queue::do_dispatch_requests() {
    while (can_dispatch()) {
        priority_class* h;
        do {
            h = static_cast<priority_class*>(pop_priority_class());
        } while (h->_queue.empty());

        auto req = std::move(h->_queue.front());
        h->_queue.pop_front();
        _requests_executing++;
        _req_count_executing += req.desc.weight;
        _bytes_count_executing += req.desc.size;
        _requests_queued--;

        update_cost(h, req.desc.weight, req.desc.size);

        if (!h->_queue.empty()) {
            push_priority_class(&*h);
        }
        req.func();
    }
}

void fair_queue::dispatch_requests() {
    if (!_reservoir) {
        do_dispatch_requests();
        return;
    }

    // Step 1: Give up our limits. If we don't do that, once we
    // dispatch a lot we will keep dispatching a lot and will be unfair to the other
    // fair_queues connected to the same reservoir.
    auto credit = prune_unused_capacity();
    _reservoir->return_capacity(credit);

    // Step 2: Dispatch the requests we currently have, within our quota.
    do_dispatch_requests();

    // Step 3: If we still have waiters, grab capacity from the reservoir
    // if possible and try to dispatch. Maybe capacity that we requested
    // in the last poll period only got ready now and we try with that first.
    if (waiters()) {
        auto t = _rptr->consume_credit();
        add_capacity(t);
        do_dispatch_requests();
    }

    // Step 3: If we still have waiters, try to acquire more capacity from
    // the reservoir. If there is extra capacity we will be able to dispatch
    // more right away. If not, we may consume it in the next poll period.
    if (waiters()) {
        credit = credit_waiting();
        _rptr->request_credit(credit);

        _reservoir->queue(_rptr);
        _reservoir->dispatch_requests();

        auto t = _rptr->consume_credit();
        add_capacity(t);
        do_dispatch_requests();
    }

    // Step 4: if we have no more waiters, give back the capacity we grabbed
    // but didn't use. We want to do this now instead of waiting for the next
    // tick, because maybe we allow requests in 3 dimensions. Maybe we grabbed
    // too much of one of them (for instance, req_count), but couldn't
    // dispatch because we were limited by another (for instance bytes_count)
    if (!waiters()) {
        credit = prune_unused_capacity();
        _reservoir->return_capacity(credit);
    }
}

void fair_queue::update_shares(priority_class_ptr pc, uint32_t new_shares) {
    pc->update_shares(new_shares);
}

}

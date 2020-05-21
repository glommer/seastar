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
#include <seastar/util/defer.hh>
#include <queue>
#include <chrono>
#include <unordered_set>
#include <cmath>
#include <mutex>

#include "fmt/format.h"
#include "fmt/ostream.h"

namespace seastar {

static_assert(sizeof(fair_queue_ticket) == sizeof(uint64_t), "unexpected fair_queue_ticket size");

fair_queue_ticket::fair_queue_ticket(uint32_t weight, uint32_t size) noexcept
    : _weight(weight)
    , _size(size)
{}

float fair_queue_ticket::normalize(fair_queue_ticket denominator) const {
    return float(_weight) / denominator._weight + float(_size) / denominator._size;
}

void fair_queue_ticket::clamp(fair_queue_ticket lo, fair_queue_ticket hi) {
    _weight = std::clamp(_weight, lo._weight, hi._weight);
    _size = std::clamp(_size, lo._size, hi._size);
}

fair_queue_ticket fair_queue_ticket::operator+(fair_queue_ticket desc) const {
    return fair_queue_ticket(_weight + desc._weight, _size + desc._size);
}

fair_queue_ticket& fair_queue_ticket::operator+=(fair_queue_ticket desc) {
    _weight += desc._weight;
    _size += desc._size;
    return *this;
}

fair_queue_ticket fair_queue_ticket::operator-(fair_queue_ticket desc) const {
    return fair_queue_ticket(_weight - desc._weight, _size - desc._size);
}

fair_queue_ticket& fair_queue_ticket::operator-=(fair_queue_ticket desc) {
    _weight -= desc._weight;
    _size -= desc._size;
    return *this;
}

bool fair_queue_ticket::operator<(fair_queue_ticket rhs) const {
    return (_weight < rhs._weight) && (_size < rhs._size);
}

fair_queue_ticket::operator bool() const {
    return (_weight > 0) || (_size > 0);
}

std::ostream& operator<<(std::ostream& os, fair_queue_ticket t) {
    return os << t._weight << ":" << t._size;
}

basic_fair_queue::basic_fair_queue(config cfg, basic_fair_queue* reservoir, uint32_t shares)
    : _config(cfg)
    , _maximum_capacity(_config.max_req_count, _config.max_bytes_count)
    , _base(std::chrono::steady_clock::now())
    , _all_classes(max_classes)
    , _reservoir(reservoir)
    , _rptr(_reservoir ? _reservoir->register_priority_class(shares) : nullptr)
{
     for (auto& pclass : _all_classes) {
         _available_classes.push(&pclass);
     }
}

basic_fair_queue::basic_fair_queue(config cfg)
    : basic_fair_queue(std::move(cfg), nullptr, 0)
{}

basic_fair_queue::basic_fair_queue(basic_fair_queue& reservoir, uint32_t shares)
    : basic_fair_queue(std::move(reservoir._config), &reservoir, shares)
{}

void basic_fair_queue::push_priority_class(priority_class_ptr pc) {
    if (!pc->_queued) {
        _handles.push(pc);
        pc->_queued = true;
    }
}

priority_class_ptr basic_fair_queue::pop_priority_class() {
    assert(!_handles.empty());
    auto h = _handles.top();
    _handles.pop();
    assert(h->_queued);
    h->_queued = false;
    return h;
}

void basic_fair_queue::notify_requests_finished(fair_queue_ticket desc) {
    release_executing_resources(desc);
    if (_reservoir) {
        remove_capacity(desc);
        _reservoir->notify_requests_finished(desc);
    }
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

bool fair_queue::can_dispatch() const {
    return _resources_queued && (_resources_executing < _current_capacity);
}

priority_class_ptr basic_fair_queue::register_priority_class(uint32_t shares) {
    std::lock_guard<util::spinlock> g(_fq_lock);
    assert(!_available_classes.empty());
    priority_class_ptr pclass = _available_classes.top();
    pclass->update_shares(shares);
    _registered_classes.insert(pclass);
    _available_classes.pop();
    return pclass;
}

void basic_fair_queue::unregister_priority_class(priority_class_ptr pclass) {
    std::lock_guard<util::spinlock> g(_fq_lock);
    assert(pclass->_queue.empty());
    _available_classes.push(pclass);
    _registered_classes.erase(pclass);
}

void basic_fair_queue::update_shares(priority_class_ptr pc, uint32_t new_shares) {
    pc->update_shares(new_shares);
}

void basic_fair_queue::consume_grant() {
    fair_queue_ticket grant = _rptr->_ticket_grant.exchange({}, std::memory_order_acquire);
    if (grant) {
        add_capacity(grant);
        dispatch_local_requests();
    }
}

void basic_fair_queue::publish_ask(priority_class_ptr pc, fair_queue_ticket desc) {
    pc->_ticket_ask.store(desc, std::memory_order_release);
}

void basic_fair_queue::dispatch_requests() {
    if (!_reservoir) {
        dispatch_local_requests();
        return;
    }

    // consume grants that are granted now from previous asks first.
    consume_grant();
    // If there is still an ask after that, we'll try to acquire a new grant
    // and dispatch right away.
    fair_queue_ticket ask = resources_currently_waiting();
    if (ask) {
        _reservoir->publish_ask(_rptr, ask);
        _reservoir->dispatch_requests();
        consume_grant();
    }
}

fair_queue::fair_queue(config cfg)
    : basic_fair_queue(std::move(cfg))
    , _current_capacity(_config.max_req_count, _config.max_bytes_count)
{}

size_t fair_queue::waiters() const {
    return _requests_queued;
}

size_t fair_queue::requests_currently_executing() const {
    return _requests_executing;
}

fair_queue_ticket fair_queue::resources_currently_waiting() const {
    return _resources_queued;
}

fair_queue_ticket fair_queue::resources_currently_executing() const {
    return _resources_executing;
}

void fair_queue::queue(priority_class_ptr pc, fair_queue_ticket desc, noncopyable_function<void()> func) {
    // We need to return a future in this function on which the caller can wait.
    // Since we don't know which queue we will use to execute the next request - if ours or
    // someone else's, we need a separate promise at this point.
    push_priority_class(pc);
    _resources_queued += desc;
    pc->_queue.push_back(priority_class::request{std::move(func), std::move(desc)});
    _requests_queued++;
}

void fair_queue::release_executing_resources(fair_queue_ticket desc) {
    _resources_executing -= desc;
}

void fair_queue::add_capacity(fair_queue_ticket t) {
    if (_maximum_capacity < (_current_capacity + t)) {
        throw std::out_of_range(fmt::format("Trying to add more capacity than the fair_queue is configured to handle. Current {}, adding {}, maximum {}",
            _current_capacity, t, _maximum_capacity));
    }
    _current_capacity += t;
}

void fair_queue::remove_capacity(fair_queue_ticket t) {
    if (_current_capacity < t) {
        throw std::out_of_range(fmt::format("Trying to remove capacity than the fair_queue has . Current {}, removing {}",
            _current_capacity, t));
    }
    _current_capacity -= t;
}

void basic_fair_queue::update_cost(priority_class_ptr h, const fair_queue_ticket& t) {
    auto delta = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now() - _base);
    auto req_cost  = t.normalize(_maximum_capacity) / h->_shares;
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

void fair_queue::dispatch_local_requests() {
    while (can_dispatch()) {
        priority_class_ptr h;
        do {
            h = pop_priority_class();
        } while (h->_queue.empty());

        auto req = std::move(h->_queue.front());
        h->_queue.pop_front();
        _resources_executing += req.desc;;
        _resources_queued -= req.desc;
        _requests_executing++;
        _requests_queued--;

        update_cost(h, req.desc);

        if (!h->_queue.empty()) {
            push_priority_class(h);
        }
        req.func();
    }
}

multishard_fair_queue::multishard_fair_queue(config cfg)
    : basic_fair_queue(std::move(cfg))
    , _current_capacity({_config.max_req_count, _config.max_bytes_count})
{}

void multishard_fair_queue::atomic_add(std::atomic<fair_queue_ticket>& t, fair_queue_ticket rhs,
        std::memory_order memory_order_success) {
    auto current = t.load(std::memory_order_relaxed);
    while (!t.compare_exchange_weak(current, current + rhs,
           memory_order_success, std::memory_order_relaxed));
}

void multishard_fair_queue::atomic_sub(std::atomic<fair_queue_ticket>& t, fair_queue_ticket rhs,
        std::memory_order memory_order_success) {
    auto current = t.load(std::memory_order_relaxed);
    while (!t.compare_exchange_weak(current, current - rhs,
           memory_order_success, std::memory_order_relaxed));
}

void multishard_fair_queue::add_capacity(fair_queue_ticket t) {
    atomic_add(_current_capacity, t);
}

void multishard_fair_queue::remove_capacity(fair_queue_ticket t) {
    atomic_sub(_current_capacity, t);
}

fair_queue_ticket multishard_fair_queue::resources_currently_waiting() const {
    return _resources_queued.load(std::memory_order_relaxed);
}

fair_queue_ticket multishard_fair_queue::available_resources() const {
    auto executing = _resources_executing.load(std::memory_order_relaxed);
    auto current = _current_capacity.load(std::memory_order_relaxed);

    executing.clamp(fair_queue_ticket{}, current);
    return current - executing;
}

void multishard_fair_queue::release_executing_resources(fair_queue_ticket t) {
    atomic_sub(_resources_executing, t);
}

void multishard_fair_queue::dispatch_local_requests() {
    auto dispatch = _fq_lock.try_lock();
    if (!dispatch) {
        return;
    }
    auto unlock = defer([this] {
        _fq_lock.unlock();
    });

    // In the fair queue we want to keep everything in the handles
    // priority queue. However for this class that would mean pushing
    // more atomic operations to queue time. The number of classes is
    // static, known, and small. Sorting here is cheap enough.
    std::vector<priority_class_ptr> classes;
    classes.reserve(_registered_classes.size());

    // Classes that do not have an ask are not included in this batch.
    // It's true that they may add an ask while we iterate the array,
    // but that would mean always scanning all registered classes even for
    // idle queues. If a priority class do not have an ask, we optimize for
    // the case where they are idle and if an ask arrives it will be included
    std::copy_if(_registered_classes.begin(), _registered_classes.end(), std::back_inserter(classes), [] (priority_class_ptr h) {
        return h->_ticket_ask.load(std::memory_order_relaxed);
    });

    // This will sort by accumulated cost, leaving the smallest element at the end.
    // We can then pop_back() from the vector and we'll be popping back in order of
    // desired execution.
    std::sort(classes.begin(), classes.end(), basic_fair_queue::class_compare());

    while (!classes.empty()) {
        // Do this separately out of the loop so we can cleanly reuse the ticket returned.
        auto avail = available_resources();
        if (!avail) {
            break;
        }
        priority_class_ptr h = classes.back();
        classes.pop_back();
        // Ask is always republished when a queue wants to run. So if we serviced it,
        // set it to zero regardless of whether or not we will serve it all or cap.
        // If we cap, once this queue runs it will repost a new ask if there is more to execute.
        auto available = h->_ticket_ask.exchange({}, std::memory_order_acquire);
        available.clamp(fair_queue_ticket{}, avail);
        // We checked already that there are available resources in the queue. The only way
        // for this to be empty is if the priority_class removed its ask. It can't happen today
        // but may happen if we support cancellations, which we want to do in the future.
        if (available) {
            atomic_add(h->_ticket_grant, available, std::memory_order_release);
            atomic_add(_resources_executing, available);
            atomic_sub(_resources_queued, available);
            update_cost(h, available);
        }
    }
}

}

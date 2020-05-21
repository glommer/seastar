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

basic_fair_queue::basic_fair_queue(config cfg)
    : _config(std::move(cfg))
    , _maximum_capacity(_config.max_req_count, _config.max_bytes_count)
    , _base(std::chrono::steady_clock::now())
    , _all_classes(max_classes)
{
    for (auto& pclass : _all_classes) {
        _available_classes.push(&pclass);
    }
}

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

void basic_fair_queue::dispatch_requests() {
    dispatch_local_requests();
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

}

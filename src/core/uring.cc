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
 * Copyright (C) 2020 ScyllaDB
 */

#include <seastar/core/internal/liburing.hh>
#include <seastar/core/align.hh>
#include <seastar/core/posix.hh>

namespace seastar {

namespace internal {

unsigned uring_context::consume_completion_queue() {
    unsigned completed = 0;

    unsigned head;
    struct io_uring_cqe* cqe;
    io_uring_for_each_cqe(&_ring, head, cqe) {
        completed++;
        _process_cqe(cqe);
    }
    io_uring_cq_advance(&_ring, completed);
    _completed += completed;
    return completed;
}

uring_context::uring_context(std::function<void(io_uring_cqe*)> process_cqe, ring_completion_method rtype, unsigned cq_entries)
    : _ring_type(rtype)
    , _process_cqe(std::move(process_cqe))
{
    struct io_uring_params p;

    memset(&p, 0, sizeof(p));
    p.flags = IORING_SETUP_CQSIZE;
    if (rtype == ring_completion_method::poll) {
        p.flags |= IORING_SETUP_IOPOLL;
    }
    p.cq_entries = cq_entries;
    // SQ ring should be smaller or equal the cq ring, but since it represents
    // the total amount of requests we can submit in a single call, we will cap
    // it at a reasonable number
    p.sq_entries = std::min(128u, cq_entries);

    auto ret = io_uring_queue_init_params(cq_entries, &_ring, &p);
    _features = p.features;
    throw_kernel_error(ret);
}

void uring_context::push_sqe(noncopyable_function<void(io_uring_sqe&)> func) {
    _waiting_submission++;
    _pending_sqe.push_back(std::move(func));
}

void uring_context::push_prio_sqe(noncopyable_function<void(io_uring_sqe&)> func) {
    _waiting_submission++;
    io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
    while (sqe == nullptr) {
        force_flush();
        sqe = io_uring_get_sqe(&_ring);
    }
    io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
    func(*sqe);
}

void uring_context::sync_wait_forever(const sigset_t* active_sigmask) {
    // There are unconsumed events: exit immediately
    if (consume_completion_queue()) {
        return;
    }
    struct io_uring_cqe *cqe = nullptr;
    io_uring_wait_cqes(&_ring, &cqe, 1, nullptr, const_cast<sigset_t*>(active_sigmask));
    // This is potentially interruptible sleep, so we if woke up on a signal we may not have a cqe.
    if (cqe) {
        _process_cqe(cqe);
        io_uring_cqe_seen(&_ring, cqe);
    }
}

void uring_context::force_flush() {
    int ret = -EBUSY;
    do {
        consume_completion_queue();
        ret = io_uring_submit(&_ring);
        if (ret > 0) {
            _submitted += ret;
        }
    } while (ret == -EBUSY);
    assert(ret >= 0);
}

unsigned uring_context::flush() {
    // Move our SQE list to the kernel
    while (!_pending_sqe.empty()) {
        io_uring_sqe* sqe = io_uring_get_sqe(&_ring);
        if (!sqe) {
            break;
        }
        auto&& func = std::move(_pending_sqe.front());
        _pending_sqe.pop_front();
        // Always do async submission and never ever block.
        // This will be guaranteed by us, as it is an
        // implementation detail that matters for seastar.
        io_uring_sqe_set_flags(sqe, IOSQE_ASYNC);
        func(*sqe);
    }

    auto ret = io_uring_submit(&_ring);
    if (ret == -EBUSY) {
        // Busy now. In the next poller, we will try again.
        return 0;
    }
    assert(ret >= 0);
    _submitted += ret;
    return ret;
}

unsigned uring_context::poll() {
    // We submit if there is anything pending for the IRQ ring. But for polled
    // rings, if we don't poll, we'll never see the result of I/O
    auto ret = 0;
    if (nr_pending() && is_poll_ring()) {
        ret = flush();
    }
    ret += consume_completion_queue();
    return 0;
}
}
}

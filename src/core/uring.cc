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
#include <seastar/util/defer.hh>

namespace seastar {

namespace internal {

unsigned uring_context::consume_completion_queue() {
    unsigned completed = 0;

    unsigned head;
    struct ::io_uring_cqe* cqe;
    io_uring_for_each_cqe(&_ring, head, cqe) {
        completed++;
        _process_cqe(cqe);
    }
    io_uring_cq_advance(&_ring, completed);
    _completed += completed;
    return completed;
}

uring_context::uring_context(noncopyable_function<void(::io_uring_cqe*)> process_cqe, ring_completion_method rtype, unsigned cq_entries)
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
    auto sq_entries = std::min(128u, cq_entries);

    auto ret = ::io_uring_queue_init_params(sq_entries, &_ring, &p);
    _features = p.features;
    throw_kernel_error(ret);
}

void uring_context::push_prio_sqe(internal::io_request req) {
    ::io_uring_sqe* sqe = ::io_uring_get_sqe(&_ring);
    while (sqe == nullptr) {
        force_flush();
        sqe = ::io_uring_get_sqe(&_ring);
    }
    _pending_submission++;
    prepare_sqe(req, *sqe);
}

void uring_context::sync_wait(const sigset_t* active_sigmask) {
    // There are unconsumed events: exit immediately
    if (consume_completion_queue()) {
        return;
    }
    struct ::io_uring_cqe *cqe = nullptr;
    io_uring_wait_cqes(&_ring, &cqe, 1, nullptr, const_cast<sigset_t*>(active_sigmask));
    // This is potentially interruptible sleep, so we if woke up on a signal we may not have a cqe.
    if (cqe) {
        _process_cqe(cqe);
        ::io_uring_cqe_seen(&_ring, cqe);
    }
}

void uring_context::force_flush() {
    int ret = -EBUSY;
    do {
        consume_completion_queue();
        ret = ::io_uring_submit(&_ring);
        if (ret > 0) {
            _submitted += ret;
        }
    } while (ret == -EBUSY);
    assert(ret >= 0);
    _pending_submission -= ret;
    assert(_pending_submission >= 0);
    _forced_flushes++;
}

void uring_context::prepare_sqe(const internal::io_request& req, io_uring_sqe& sqe) {
    switch (req.opcode()) {
    case io_request::operation::read:
        io_uring_prep_read(&sqe, req.fd(), req.address(), req.size(), req.pos());
        break;
    case io_request::operation::readv:
        io_uring_prep_readv(&sqe, req.fd(), req.iov(), req.iov_len(), req.pos());
        break;
    case io_request::operation::write:
        io_uring_prep_write(&sqe, req.fd(), req.address(), req.size(), req.pos());
        break;
    case io_request::operation::writev:
        io_uring_prep_writev(&sqe, req.fd(), req.iov(), req.iov_len(), req.pos());
        break;
    case io_request::operation::send:
        io_uring_prep_send(&sqe, req.fd(), req.address(), req.size(), req.flags());
        break;
    case io_request::operation::sendmsg:
        io_uring_prep_sendmsg(&sqe, req.fd(), req.msghdr(), req.flags());
        break;
    case io_request::operation::recv:
        io_uring_prep_recv(&sqe, req.fd(), req.address(), req.size(), req.flags());
        break;
    case io_request::operation::recvmsg:
        io_uring_prep_recvmsg(&sqe, req.fd(), req.msghdr(), req.flags());
        break;
    case io_request::operation::accept:
        io_uring_prep_accept(&sqe, req.fd(), req.posix_sockaddr(), req.socklen_ptr(), req.flags());
        break;
    case io_request::operation::connect:
        io_uring_prep_connect(&sqe, req.fd(), req.posix_sockaddr(), req.socklen());
        break;
    case io_request::operation::fdatasync:
        io_uring_prep_fsync(&sqe, req.fd(), IORING_FSYNC_DATASYNC);
        break;
    case io_request::operation::poll_add:
        io_uring_prep_poll_add(&sqe, req.fd(), req.events());
        break;
    case io_request::operation::poll_remove:
        io_uring_prep_poll_remove(&sqe, req.address());
        break;
    case io_request::operation::cancel:
        io_uring_prep_cancel(&sqe, req.address(), 0);
        break;
    }

    ::io_uring_sqe_set_data(&sqe, req.get_kernel_completion());
}

unsigned uring_context::flush() {
    _flushes++;

    // Saves a kernel round trip if there is nothing waiting in
    // the sqe list.
    if (!_pending_submission) {
        return 0;
    }

    auto ret = io_uring_submit(&_ring);
    if (ret == -EBUSY) {
        // Busy now. In the next poller, we will try again.
        return 0;
    }
    assert(ret >= 0);
    _submitted += ret;
    _pending_submission -= ret;
    assert(_pending_submission >= 0);
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

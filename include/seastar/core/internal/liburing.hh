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

#pragma once

#if SEASTAR_HAVE_URING

#include <liburing.h>
#include <seastar/core/chunked_fifo.hh>
#include <seastar/core/internal/io_request.hh>
#include <seastar/util/noncopyable_function.hh>
#include <chrono>

namespace seastar {

namespace internal {

class uring_probe {
    ::io_uring_probe *_probe = nullptr;
public:
    uring_probe(const io_uring* ring)
        : _probe(::io_uring_get_probe_ring(const_cast<io_uring*>(ring)))
    {}

    ~uring_probe() {
        ::free(_probe);
    }

    bool opcode_supported(int opcode) {
        return _probe && ::io_uring_opcode_supported(_probe, opcode);
    }

    bool opcode_supported(std::vector<int> opcodes) {
        return std::all_of(opcodes.begin(), opcodes.end(), [this] (int op) {
            return opcode_supported(op);
        });
    }
};

class uring_context {
public:
    enum class ring_completion_method { irq, poll };
private:
    ::io_uring _ring;
    int64_t _pending_submission = 0;
    uint64_t _submitted = 0;
    uint64_t _completed = 0;
    uint32_t _features = 0;
    chunked_fifo<internal::io_request> _pending_sqe;

    ring_completion_method _ring_type;
    noncopyable_function<void(::io_uring_cqe*)> _process_cqe;

    uint64_t _flushes = 0;
    uint64_t _forced_flushes = 0;

    bool is_poll_ring() const {
        return _ring_type == ring_completion_method::poll;
    }

    unsigned consume_completion_queue();

    uring_context(noncopyable_function<void(::io_uring_cqe*)> process_cqe, ring_completion_method rtype, unsigned cq_entries);

    static void prepare_sqe(const internal::io_request& req, io_uring_sqe& sqe);
public:
    ~uring_context() {
        io_uring_queue_exit(&_ring);
    }

    uint64_t total_flushes() const {
        return _flushes;
    }

    uint64_t total_forced_flushes() const {
        return _forced_flushes;
    }

    /// returns true if the features listed are supported by this ring
    bool ring_features_supported(unsigned feature_mask) const {
        return (_features & feature_mask) == feature_mask;
    }

    // returns true if the opcodes listed are supported
    bool ring_opcodes_supported(std::vector<int> opcodes) const {
        auto probe = uring_probe(&_ring);
        return probe.opcode_supported(std::move(opcodes));
    }

    // returns number of events submitted to the kernel but not yet completed
    uint64_t nr_pending() const {
        return _submitted - _completed;
    }

    // submits the current list of sqes known to the kernel and returns how many
    // sqes were submitted.
    unsigned flush();

    // reaps the completion queue. This will never go to the kernel if we are using
    // IRQ-based rings, and will go to the kernel for IOPOLL rings if there are events
    // pending.
    unsigned poll();

    // forces submission of the current list of sqes known to the kernel.
    // Usually just calling the kernel can return -EBUSY, but if force_flush
    // is used we will loop, consuming the completion queue and trying again,
    // until the call succeeds.
    void force_flush();

    // Pushes a priority sqe, that bypasses the current userspace submission queue
    void push_prio_sqe(internal::io_request req);

    // submits a request into the uring, if there is sqe space available.
    // Requests are drawn from any container that supports a front() call
    // Returns true if the request was submitted, false if not
    template <typename Queue>
    bool maybe_submit_request(const Queue& req_list) {
        ::io_uring_sqe* sqe = ::io_uring_get_sqe(&_ring);
        if (sqe) {
            auto& req = std::move(req_list.front());
            prepare_sqe(req, *sqe);
            _pending_submission++;
            return true;
        }
        return false;
    }

    // Wait synchronously until an event is available or a signal is received.
    void sync_wait_forever(const sigset_t* active_sigmask);

    // Creates an IRQ-based ring. Suitable for most types of events
    static uring_context make_irq_ring(std::function<void(::io_uring_cqe*)> process_cqe, unsigned entries = 256) {
        return uring_context(std::move(process_cqe), ring_completion_method::irq, entries);
    }

    // Creates an IOPOLL ring. Suitable for disk I/O only, on supported filesystems (XFS).
    // IOPOLL rings will not depend on device interrupts and will instead only probe the hardware
    // for completion when we active poll through our poll() function.
    static uring_context make_poll_ring(std::function<void(::io_uring_cqe*)> process_cqe, unsigned entries = 256) {
        return uring_context(std::move(process_cqe), ring_completion_method::poll, entries);
    }
};

}
}
#endif

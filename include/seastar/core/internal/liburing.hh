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

#include <liburing.h>
#include <linux/io_uring.h>
#include <fmt/format.h>
#include <unordered_set>
#include <seastar/core/posix.hh>

namespace seastar {

namespace internal {

namespace linux_abi {

struct io_uring_probe_op {
    uint8_t op;
    uint8_t resv;
    uint16_t flags;
    uint32_t resv2;
};

struct io_uring_probe {
    uint8_t last_op;
    uint8_t ops_len;
    uint16_t resv;
    uint32_t resv2[3];
    struct io_uring_probe_op ops[256];
};

static constexpr int syscall_nr_io_uring_register = 427;
static constexpr int ioring_register_probe = 8;
}

inline bool
io_uring_need_preempt(const void *preempt_var) {
    // FIXME: how to reset ?
    // test it.
    // We aren't reading anything from the ring, so we don't need
    // any barriers. This is slightly more efficient than calling
    // io_uring_cq_ready, which will add barriers.
    auto *cq = reinterpret_cast<const io_uring_cq*>(preempt_var);
    //fmt::print("need preempt? {}\n", __builtin_expect(*cq->ktail != *cq->khead, false));
    return __builtin_expect(*cq->ktail != *cq->khead, false);
}

class uring_context {
    ::io_uring _ring;
    unsigned _entries;
    uint64_t _waiting_submission = 0;
    uint64_t _completed = 0;

    int _register(unsigned opcode, const void *arg, unsigned nr_args) const {
        return syscall(linux_abi::syscall_nr_io_uring_register, _ring.ring_fd, opcode, arg, nr_args);
    }

public:
    void* completion_queue() {
        return &_ring.cq;
    }

    void* submission_queue() {
        return &_ring.cq;
    }

    io_uring_sqe* get_sqe() {
        auto* sqe = io_uring_get_sqe(&_ring);
        if (!sqe) {
            throw std::runtime_error("failed to get an sqe");
        }
        _waiting_submission++;
        return sqe;
    }

    internal::linux_abi::io_uring_probe probe() {
        internal::linux_abi::io_uring_probe probe = {};
        memset(&probe, 0, sizeof(probe));
        auto r = _register(linux_abi::ioring_register_probe, &probe, 256);
        throw_kernel_error(r);
        return probe;
    }

    io_uring_cqe* sync_wait(timespec* ts, const sigset_t* active_sigmask) {
        struct io_uring_cqe *cqe = nullptr;
        io_uring_wait_cqes(&_ring, &cqe, 1, nullptr, const_cast<sigset_t*>(active_sigmask));
        return cqe;
    }

    unsigned flush() {
        auto ret = io_uring_submit(&_ring);
        if (ret < 0) {
            throw std::runtime_error("failed to submit events to io_uring");
        }
        return ret;
    }
    template <typename func>
    unsigned poll(func&& process_cqe) {
        unsigned ret = 0;
        // polled I/O will always involve a syscall, so we don't want to even try it
        // if we're not waiting for anything.
        if (nr_pending() > 0) {
            ret = flush();

            struct io_uring_cqe *cqe[_entries];
            for (unsigned i = 0; i < io_uring_peek_batch_cqe(&_ring, cqe, _entries); ++i) {
                io_uring_cqe_seen(&_ring, cqe[i]);
                _completed++;
                process_cqe(cqe[i]);
            }
        }

        return ret;
    }

    uint64_t nr_pending() const {
        return _waiting_submission - _completed;
    }
protected:
    uring_context(int flags, unsigned entries = 256)
        : _entries(entries)
    {
        auto ret = io_uring_queue_init(entries, &_ring, flags);
        throw_kernel_error(ret);
    }

    ~uring_context() {
        io_uring_queue_exit(&_ring);
    }


};

class uring_interrupt_context : public uring_context {
public:
    uring_interrupt_context(unsigned entries = 2560) : uring_context(0, entries) {}
};

class uring_polling_context : public uring_context {
public:
    uring_polling_context(unsigned entries = 2560) : uring_context(IORING_SETUP_IOPOLL, entries) {}
};

}
}


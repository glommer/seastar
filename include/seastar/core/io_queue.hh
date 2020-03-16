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

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/metrics_registration.hh>
#include <seastar/core/shared_ptr.hh>
#include <seastar/core/future.hh>
#include <seastar/core/internal/io_request.hh>
#include <mutex>
#include <array>

namespace seastar {

/// Renames an io priority class
///
/// Renames an \ref io_priority_class previously created with register_one_priority_class().
///
/// The operation is global and affects all shards.
/// The operation affects the exported statistics labels.
///
/// \param pc The io priority class to be renamed
/// \param new_name The new name for the io priority class
/// \return a future that is ready when the io priority class have been renamed
future<>
rename_priority_class(io_priority_class pc, sstring new_name);

namespace internal {
namespace linux_abi {

struct io_event;
struct iocb;

}
}

using shard_id = unsigned;

class io_priority_class;

class io_desc_read_write final : public kernel_completion, public fair_queue_request_descriptor {
    io_queue* _ioq_ptr;
    unsigned _id;
    // FIXME: merge with fq_desc ?
    internal::io_request _req;
    promise<size_t> _pr;
    unsigned _owner;
private:
    void notify_requests_finished();
public:
    io_desc_read_write() {}
    unsigned id() {
        return _id;
    }

    io_desc_read_write(io_queue* ioq, unsigned class_id, unsigned weight, unsigned size, internal::io_request req);

    future<size_t> get_future() {
        return _pr.get_future();
    }

    void set_exception(std::exception_ptr eptr);
    virtual void complete_with(ssize_t ret) override;
    virtual ticket operator()(ticket& t) override;
};


class io_queue {
public:
    struct config {
        shard_id coordinator;
        std::vector<shard_id> io_topology;
        unsigned capacity = std::numeric_limits<unsigned>::max();
        unsigned max_req_count = std::numeric_limits<unsigned>::max();
        unsigned max_bytes_count = std::numeric_limits<unsigned>::max();
        unsigned disk_req_write_to_read_multiplier = read_request_base_count;
        unsigned disk_bytes_write_to_read_multiplier = read_request_base_count;
        sstring mountpoint = "undefined";
        multishard_fair_queue* multishard_fq;
    };
private:
    struct priority_class_data {
        priority_class_ptr ptr;
        size_t bytes;
        uint64_t ops;
        uint32_t nr_queued;
        std::chrono::duration<double> queue_time;
        metrics::metric_groups _metric_groups;
        priority_class_data(sstring name, sstring mountpoint, priority_class_ptr ptr);
        void rename(sstring new_name, sstring mountpoint);
        void free_io_desc(io_desc_read_write* desc);
        io_desc_read_write* alloc_io_desc(io_queue* ioq_ptr, unsigned id, size_t len, internal::io_request req);

        future<> wait_for_io_desc();

    private:
        static constexpr unsigned max_io_desc = 128;
        //semaphore _io_desc_sem = { max_io_desc };
        std::array<io_desc_read_write, max_io_desc> _io_desc;
        std::stack<io_desc_read_write*, boost::container::static_vector<io_desc_read_write*, max_io_desc>> _free_io_desc;

        void register_stats(sstring name, sstring mountpoint);
    };

    std::vector<lw_shared_ptr<priority_class_data>> _priority_classes;
public:
    fair_queue _fq;

    uint64_t _nr_queued = 0;

private:
    static constexpr unsigned _max_classes = 2048;
    static std::mutex _register_lock;
    static std::array<uint32_t, _max_classes> _registered_shares;
    static std::array<sstring, _max_classes> _registered_names;

    static io_priority_class register_one_priority_class(sstring name, uint32_t shares);

    priority_class_data& find_or_create_class(const io_priority_class& pc);
    friend class smp;

public:
    void free_io_desc(io_desc_read_write* desc);
    // We want to represent the fact that write requests are (maybe) more expensive
    // than read requests. To avoid dealing with floating point math we will scale one
    // read request to be counted by this amount.
    //
    // A write request that is 30% more expensive than a read will be accounted as
    // (read_request_base_count * 130) / 100.
    // It is also technically possible for reads to be the expensive ones, in which case
    // writes will have an integer value lower than read_request_base_count.
    static constexpr unsigned read_request_base_count = 128;

    io_queue(config cfg);
    ~io_queue();

    future<size_t>
    queue_request(const io_priority_class& pc, size_t len, internal::io_request req);

    size_t capacity() const {
        return _config.capacity;
    }

    size_t queued_requests() const {
        return _fq.waiters();
    }

    // How many requests are sent to disk but not yet returned.
    size_t requests_currently_executing() const {
        return _fq.requests_currently_executing();
    }

    // Inform the underlying queue about the fact that some of our requests finished
    void notify_requests_finished(fair_queue_request_descriptor& desc);

    // Dispatch requests that are pending in the I/O queue
    void poll_io_queue();

    sstring mountpoint() const {
        return _config.mountpoint;
    }

    shard_id coordinator() const {
        return _config.coordinator;
    }
    shard_id coordinator_of_shard(shard_id shard) const {
        return _config.io_topology[shard];
    }

    future<> update_shares_for_class(io_priority_class pc, size_t new_shares);
    void rename_priority_class(io_priority_class pc, sstring new_name);

    friend class reactor;
public:
    config _config;
    static basic_fair_queue::config make_fair_queue_config(config cfg);
};

}

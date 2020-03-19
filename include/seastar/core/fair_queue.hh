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
 * Copyright (C) 2016 ScyllaDB
 */
#pragma once

#include <seastar/core/shared_ptr.hh>
#include <seastar/core/circular_buffer.hh>
#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/spinlock.hh>
#include <queue>
#include <chrono>
#include <unordered_set>
#include <atomic>
#include <stack>
#include <boost/lockfree/spsc_queue.hpp>
#include <boost/container/static_vector.hpp>

namespace seastar {

/// \brief describes a request that passes through the fair queue
///
/// \related fair_queue
struct fair_queue_request_descriptor {
    unsigned weight = 1; ///< the weight of this request for capacity purposes (IOPS).
    unsigned size = 1;        ///< the effective size of this request
};

/// \addtogroup io-module
/// @{

/// \cond internal
struct basic_priority_class {
    uint32_t _shares = 1u;
    float _accumulated = 0;
    bool _queued = false;

    basic_priority_class() {}
    explicit basic_priority_class(uint32_t shares)
        : _shares(std::max(shares, 1u))
    {}
};

// Allow for a fair_queue to expand and/or contract its capacity by
// amounts described in this structure.
struct fair_queue_credit {
    uint64_t quantity = 0;
    uint64_t weight = 0;
    uint64_t size = 0;
};

class priority_class : public basic_priority_class {
    struct request {
        noncopyable_function<void()> func;
        fair_queue_request_descriptor desc;
    };
    friend class fair_queue;
    circular_buffer<request> _queue;

    friend struct shared_ptr_no_esft<priority_class>;
    explicit priority_class(uint32_t shares)
        : basic_priority_class(shares) {}

    void update_shares(uint32_t shares) {
        _shares = (std::max(shares, 1u));
    }
public:
    /// \brief return the current amount of shares for this priority class
    uint32_t shares() const {
        return _shares;
    }
};

class bulk_fair_queue;

class bulk_priority_class : public basic_priority_class {
    // Quantities we have asked for.
    // Updates will always update size last with an acquire barrier
    // Readers will know that the other quantities are at least as up2date
    // as size (new writes could have happened by the time we read)
    std::atomic<uint64_t> _quantity = { 0 };
    std::atomic<uint64_t> _weight = { 0 };
    std::atomic<uint64_t> _size = { 0 };

    // Quantities we have at our disposal.
    // Same: updates will always update size last with an acquire barrier
    std::atomic<uint64_t> _pending_quantity = { 0 };
    std::atomic<uint64_t> _pending_weight = { 0 };
    std::atomic<uint64_t> _pending_size = { 0 };

    std::atomic<bool> _dirty = { false };
    friend class bulk_fair_queue;
public:
    void receive_grant(fair_queue_credit& c);
    fair_queue_credit consume_credit();
    void request_credit(fair_queue_credit& t);
};

class bulk_fair_queue;
using bulk_priority_class_ptr = bulk_priority_class*;

/// \endcond
/// \brief Basic Fair queuing class.
///
/// Used as a base class for all the versions of the fair queue.
/// Not meant to be instantiated directly. See derived classes for intended usage.
class basic_fair_queue {
public:
    /// \brief Fair Queue configuration structure.
    ///
    /// \sets the operation parameters of a \ref fair_queue
    /// \related fair_queue
    struct config {
        unsigned capacity = std::numeric_limits<unsigned>::max();
        std::chrono::microseconds tau = std::chrono::milliseconds(100);
        unsigned max_req_count = std::numeric_limits<unsigned>::max();
        unsigned max_bytes_count = std::numeric_limits<unsigned>::max();
    };
protected:
    struct class_compare {
        bool operator() (const basic_priority_class* lhs, const basic_priority_class* rhs) const {
            return lhs->_accumulated > rhs->_accumulated;
        }
    };

    using prioq = std::priority_queue<basic_priority_class*, std::vector<basic_priority_class*>, class_compare>;
    prioq _handles;

    std::unordered_set<basic_priority_class*> _registered_classes;

    std::chrono::microseconds _tau = std::chrono::milliseconds(100);

    unsigned _requests_executing = 0;
    unsigned _current_capacity = 0;

    unsigned _req_count_executing = 0;
    unsigned _current_max_req_count = 0;

    unsigned _bytes_count_executing = 0;
    unsigned _current_max_bytes_count = 0;

    bool can_dispatch() const;
    void update_cost(basic_priority_class* h, float weight, float size);

    float normalize_factor() const;
    void normalize_stats();

    void push_priority_class(basic_priority_class* pc);
    basic_priority_class* pop_priority_class();

    using clock_type = std::chrono::steady_clock::time_point;
    config _config;
    clock_type _base;

    bulk_fair_queue* _reservoir = nullptr;
    bulk_priority_class_ptr _rptr = nullptr;

    basic_fair_queue(config cfg)
        : _current_capacity(cfg.capacity)
        , _current_max_req_count(cfg.max_req_count)
        , _current_max_bytes_count(cfg.max_bytes_count)
        , _config(std::move(cfg))
        , _base(std::chrono::steady_clock::now())
    {}

    basic_fair_queue(const basic_fair_queue& fq)
        : _config(fq._config)
        , _base(fq._base)
    {}
    basic_fair_queue(basic_fair_queue&& fq) = delete;
    basic_fair_queue& operator=(basic_fair_queue&& fq) = delete;
    basic_fair_queue& operator=(const basic_fair_queue& fq) = delete;

    fair_queue_credit prune_unused_capacity();
    void add_capacity(fair_queue_credit& t);
    void remove_capacity(fair_queue_credit& t);
public:
    // Return capacity that is no longer used to the parent queue, if a parent
    // queue is present
    void return_capacity(fair_queue_credit& c);
    /// Try to execute new requests if there is capacity left in the queue.
    virtual void dispatch_requests() = 0;

    /// \return the number of requests currently executing
    size_t requests_currently_executing() const;
};

/// \brief Bulk Fair queuing class.
///
/// Used as inner nodes in nested queue environments. Updates work in bulk.
/// Another queue that sits on top will accumulate capacity requests and at
/// a later time we can transfer bulk credits.
///
/// This class is safe to use across shards with the following restrictions:
/// - The dispatch_requests() method can be called from any shard.
/// - The queue() method can only be called, for a given \ref bulk_priority_class, from the same shard.
class bulk_fair_queue : public basic_fair_queue {
    static constexpr size_t max_classes = 1024;
    std::array<bulk_priority_class, max_classes> _all_classes;
    std::stack<bulk_priority_class_ptr,
        boost::container::static_vector<bulk_priority_class_ptr, max_classes>> _available_classes;

    using dirty_queue = boost::lockfree::spsc_queue<bulk_priority_class_ptr,
                                                 boost::lockfree::capacity<256>>;

    dirty_queue _dirty;
    util::spinlock _lock;
    void reap_queued_classes();
public:
    using config = basic_fair_queue::config;

    bulk_fair_queue(config cfg);
    bulk_priority_class_ptr register_priority_class(uint32_t shares);
    void unregister_priority_class(bulk_priority_class_ptr ptr);

    void queue(bulk_priority_class_ptr pc);
    virtual void dispatch_requests() override;
};

/// \brief Priority class, to be used with a given \ref fair_queue
///
/// An instance of this class is associated with a given \ref fair_queue. When registering
/// a class, the caller will receive a \ref lw_shared_ptr to an object of this class. All its methods
/// are private, so the only thing the caller is expected to do with it is to pass it later
/// to the \ref fair_queue to identify a given class.
///
/// \related fair_queue
using priority_class_ptr = lw_shared_ptr<priority_class>;

/// \brief Fair queuing class
///
/// This is a fair queue, allowing multiple request producers to queue requests
/// that will then be served proportionally to their classes' shares.
///
/// To each request, a weight can also be associated. A request of weight 1 will consume
/// 1 share. Higher weights for a request will consume a proportionally higher amount of
/// shares.
///
/// The user of this interface is expected to register multiple \ref priority_class
/// objects, which will each have a shares attribute.
///
/// Internally, each priority class may keep a separate queue of requests.
/// Requests pertaining to a class can go through even if they are over its
/// share limit, provided that the other classes have empty queues.
///
/// When the classes that lag behind start seeing requests, the fair queue will serve
/// them first, until balance is restored. This balancing is expected to happen within
/// a certain time window that obeys an exponential decay.
class fair_queue : public basic_fair_queue {
public:
    using config = basic_fair_queue::config;
private:
    friend priority_class;

    unsigned _requests_queued = 0;
    unsigned _req_count_queued = 0;
    unsigned _bytes_count_queued = 0;

    std::unordered_set<priority_class_ptr> _all_classes;
    void do_dispatch_requests();
public:
    /// Constructs an empty fair queue nested with a \ref bulk_fair_queue
    ///
    /// The \ref bulk_fair_queue must be preconstructed. Many fair_queues
    /// can (and should) be attached to the same \ref bulk_fair_queue
    explicit fair_queue(bulk_fair_queue& fq)
        : basic_fair_queue(fq)
    {}
    /// Constructs a fair queue with configuration parameters \c cfg.
    ///
    /// \param cfg an instance of the class \ref config
    explicit fair_queue(config cfg)
        : basic_fair_queue(std::move(cfg))
    {}

    /// Constructs a fair queue with a given \c capacity.
    ///
    /// \param capacity how many concurrent requests are allowed in this queue.
    /// \param tau the queue exponential decay parameter, as in exp(-1/tau * t)
    explicit fair_queue(unsigned capacity, std::chrono::microseconds tau = std::chrono::milliseconds(100))
        : fair_queue(config{capacity, tau}) {}

    /// Registers a priority class against this fair queue.
    ///
    /// \param shares, how many shares to create this class with
    priority_class_ptr register_priority_class(uint32_t shares);

    /// Unregister a priority class.
    ///
    /// It is illegal to unregister a priority class that still have pending requests.
    void unregister_priority_class(priority_class_ptr pclass);

    /// \return how many waiters are currently queued for all classes.
    size_t waiters() const;

    /// \return how much credit would we need to execute everything that is pending
    fair_queue_credit credit_waiting() const;
    /// Queue the function \c func through this class' \ref fair_queue, with weight \c weight
    ///
    /// It is expected that \c func doesn't throw. If it does throw, it will be just removed from
    /// the queue and discarded.
    ///
    /// The user of this interface is supposed to call \ref notify_requests_finished when the
    /// request finishes executing - regardless of success or failure.
    void queue(priority_class_ptr pc, fair_queue_request_descriptor desc, noncopyable_function<void()> func);

    /// Notifies that ont request finished
    /// \param desc an instance of \c fair_queue_request_descriptor structure describing the request that just finished.
    void notify_requests_finished(fair_queue_request_descriptor& desc);

    /// Try to execute new requests if there is capacity left in the queue.
    virtual void dispatch_requests() override;

    /// Updates the current shares of this priority class
    ///
    /// \param new_shares the new number of shares for this priority class
    static void update_shares(priority_class_ptr pc, uint32_t new_shares);
};
/// @}

}

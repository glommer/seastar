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
#include <seastar/core/sstring.hh>
#include <seastar/util/noncopyable_function.hh>
#include <queue>
#include <chrono>
#include <unordered_set>
#include <stack>
#include <boost/container/static_vector.hpp>
#include <boost/lockfree/spsc_queue.hpp>
#include <seastar/util/spinlock.hh>

namespace seastar {

struct basic_priority_class {
    uint32_t _shares = 1u;
    float _accumulated = 0;
    bool _queued = false;
};

struct ticket {
    uint64_t quantity = 0;
    uint64_t weight = 0;
    uint64_t size = 0;
};

class multishard_fair_queue;

class multishard_priority_class : public basic_priority_class {
    // Quantities we have asked for.
    std::atomic<uint64_t> _quantity = { 0 };
    std::atomic<uint64_t> _weight = { 0 };
    std::atomic<uint64_t> _size = { 0 };

    // Quantities we have at our disposal.
    std::atomic<uint64_t> _pending_quantity = { 0 };
    std::atomic<uint64_t> _pending_weight = { 0 };
    std::atomic<uint64_t> _pending_size = { 0 };

    std::atomic<bool> _dirty = { false };
    friend multishard_fair_queue;
public:
    void grant(ticket& t);

    ticket receive();
    void ask_for(ticket& t);
};

using multishard_priority_class_ptr = multishard_priority_class*;

/// \brief describes a request that passes through the fair queue
///
/// \related fair_queue
struct fair_queue_request_descriptor {
    unsigned weight = 1; ///< the weight of this request for capacity purposes (IOPS).
    unsigned size = 1;        ///< the effective size of this request
    virtual ticket operator()(ticket& t) = 0;
    fair_queue_request_descriptor() {}
    fair_queue_request_descriptor(unsigned weight, unsigned size) : weight(weight), size(size) {}
};

/// \addtogroup io-module
/// @{

/// \cond internal
class priority_class : public basic_priority_class {
public:
    static constexpr unsigned queue_capacity = 128;
private:
    using lf_queue = boost::lockfree::spsc_queue<fair_queue_request_descriptor*,
                                                 boost::lockfree::capacity<queue_capacity>>;

    friend class fair_queue;
    lf_queue _queue;

    friend struct shared_ptr_no_esft<priority_class>;

    void update_shares(uint32_t shares) {
        _shares = (std::max(shares, 1u));
    }
public:
    /// \brief return the current amount of shares for this priority class
    uint32_t shares() const {
        return _shares;
    }
};
/// \endcond

/// \brief Priority class, to be used with a given \ref fair_queue
///
/// An instance of this class is associated with a given \ref fair_queue. When registering
/// a class, the caller will receive a \ref lw_shared_ptr to an object of this class. All its methods
/// are private, so the only thing the caller is expected to do with it is to pass it later
/// to the \ref fair_queue to identify a given class.
///
/// \related fair_queue
using priority_class_ptr = priority_class*;


class basic_fair_queue {
public:
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

    using clock_type = std::chrono::steady_clock::time_point;
    clock_type _base;
    config _config;

    basic_fair_queue(config cfg);
};

class multishard_fair_queue : public basic_fair_queue {
    // FIXME: atomic needs move constructor implemented, but maybe make the max
    // property into smp
    std::array<multishard_priority_class, 1024> _all_classes;
    using dirty_queue = boost::lockfree::spsc_queue<multishard_priority_class_ptr,
                                                 boost::lockfree::capacity<256>>;

    dirty_queue _dirty;
    util::spinlock _dispatch_lock;
public:
    multishard_fair_queue(config cfg);
    multishard_priority_class_ptr get_multishard_priority_class();

    void queue(multishard_priority_class_ptr pc);
    void dispatch_requests();

    void push_priority_class(basic_priority_class* pc);
    multishard_priority_class_ptr pop_priority_class();
    void notify_requests_finished(ticket& t);
};

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
private:
    multishard_fair_queue* _multishard_fq = nullptr;
    multishard_priority_class_ptr _multishard_pclass = nullptr;

    friend priority_class;

    unsigned _requests_queued = 0;
    unsigned _req_count_queued = 0;
    unsigned _bytes_count_queued = 0;

    static constexpr unsigned _max_classes = 1024;

    std::array<priority_class, _max_classes> _all_classes;
    std::stack<priority_class_ptr, boost::container::static_vector<priority_class_ptr, _max_classes>> _available_classes;

    void push_priority_class(priority_class_ptr pc);
    priority_class_ptr pop_priority_class();

    void do_dispatch_requests();
    ticket waiting() const;
    ticket prune_excess_capacity();
    ticket prune_all_capacity();
    void add_capacity(ticket& t);
    void remove_capacity(ticket& t);
public:
    /// Constructs a fair queue with configuration parameters \c cfg, attaching it to a parent \ref multishard_fair_queue.
    ///
    /// \param cfg an instance of the class \ref config
    explicit fair_queue(multishard_fair_queue *parent);

    /// Constructs a fair queue with configuration parameters \c cfg.
    ///
    /// \param cfg an instance of the class \ref config
    explicit fair_queue(basic_fair_queue::config cfg);

    /// Constructs a fair queue with a given \c capacity.
    ///
    /// \param capacity how many concurrent requests are allowed in this queue.
    /// \param tau the queue exponential decay parameter, as in exp(-1/tau * t)
    explicit fair_queue(unsigned capacity, std::chrono::microseconds tau = std::chrono::milliseconds(100))
        : fair_queue(basic_fair_queue::config{capacity, tau}) {}

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

    /// \return the number of requests currently executing
    size_t requests_currently_executing() const;

    /// Queue the function \c func through this class' \ref fair_queue, with weight \c weight
    ///
    /// It is expected that \c func doesn't throw. If it does throw, it will be just removed from
    /// the queue and discarded.
    ///
    /// The user of this interface is supposed to call \ref notify_requests_finished when the
    /// request finishes executing - regardless of success or failure.
    void queue(priority_class_ptr pc, fair_queue_request_descriptor* desc);

    /// Notifies that ont request finished
    /// \param desc an instance of \c fair_queue_request_descriptor structure describing the request that just finished.
    /// \param requests how many requests are completing
    void notify_requests_finished(fair_queue_request_descriptor& desc, unsigned requests = 1);

    /// Try to execute new requests if there is capacity left in the queue.
    void dispatch_requests();

    /// Updates the current shares of this priority class
    ///
    /// \param new_shares the new number of shares for this priority class
    static void update_shares(priority_class_ptr pc, uint32_t new_shares);
};
/// @}

}

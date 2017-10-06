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
 * Copyright 2017 ScyllaDB.
 */

#pragma once

#include <chrono>
#include <cmath>
#include <ctgmath>

#include <atomic>

namespace seastar {

/// Exception thrown when we remove more tokens than the bucket has
class bucket_overload: public std::exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept {
        return "Bucket overload";
    }
};

template<typename Clock> class token_bucket;

/// \brief Stores information about token deficit in a token bucket
///
/// \related token_bucket
struct token_info {
    std::atomic<uint64_t> _needed = {0};
    unsigned _my_id = 0;
    unsigned _nr_sharing = 0;

    template<typename Clock> friend class token_bucket;

    void update_backlog(uint64_t reserved, uint64_t tokens) {
        uint64_t backlog = 0;
        if (reserved > tokens) {
            backlog = reserved - tokens;
        }
        _needed.store(backlog, std::memory_order_relaxed);
    }
    unsigned nr_sharing() const {
        return _nr_sharing;
    }
public:
    token_info(const token_info& t) : _needed(t._needed.load(std::memory_order_relaxed)), _my_id(t._my_id), _nr_sharing(t._nr_sharing) {}
    token_info(unsigned id, unsigned nr_sharing) : _my_id(id), _nr_sharing(nr_sharing) {}
    token_info() {}

    /// \brief how many tokens are needed by this instance.
    uint64_t needed_donation() {
        return _needed.load(std::memory_order_relaxed);
    }

    /// \brief unique identifier for this instance
    unsigned id() const {
        return _my_id;
    }
};

/// \brief implementation of the token_bucket algoritm, with multi-shard token loan
///
/// The Token Bucket algorithm allows for a process to be controlled for throughput, with
/// potentially bursty activity.
///
/// The basic idea is that at regular intervals tokens are added to a bucket, and users will
/// consume them. Because users are limited to the existing tokens, throughput of the process is
/// controlled as a result.
///
/// There are two main main interesting aspects in our implementation:
///
/// 1) updates are not periodic. That is because / this is intended to be used
///    from seastar pollers. Specially in idle systems, pollers can be run
///    at any time.
///
/// 2) Before being disposed, unused tokens are first potentially loaned to other shards.
///    For that to happen, we need to constantly publicize our token need/surplus.
///
/// The algorithm specifies a maximum burst parameter. After that period
/// passes, we stop accumulating / tokens if we are already at the maximum.
///
/// Because of the loaning mechanism, before consuming tokens the user of the interface
/// has to commit to its intended usage with the \ref reserve_tokens method - so we don't
/// loan them out.
template <typename Clock = std::chrono::steady_clock>
class token_bucket {
    typename Clock::time_point _last_update;
    typename Clock::time_point _last_update_big;
    double _tokens_per_microsecond;
    double _tokens_added_since_last_ms;
    double _tokens = 0;
    double _donated_tokens = 0;
    double _max_capacity;
    double _max_donation_capacity;
    uint64_t _reserved_tokens = 0;
    token_info *_token_info;
    static double tokens_per_microsecond(uint64_t rate_per_second) {
        return double(rate_per_second) / std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::seconds(1)).count();
    }
    token_bucket(const token_bucket&) = delete;
    float total_tokens() const {
        return _tokens + _donated_tokens;
    }
public:
    /// \brief create a token bucket limited to \c rate_per_second units per second, possibly allowing donations
    ///
    /// \param rate_per_second the rate per second that this bucket limits
    /// \param token_info a pointer to a \ref token_info instance, where this bucket will publicize its token deficits. If nullptr, no donations allowed
    /// \param burst_period the maximum period for which to accumulate unused tokens
    token_bucket(uint64_t rate_per_second, token_info* token_info, std::chrono::milliseconds burst_period = std::chrono::milliseconds(100))
        : _last_update(Clock::now())
        , _last_update_big(Clock::now())
        , _tokens_per_microsecond(tokens_per_microsecond(rate_per_second))
        , _max_capacity(_tokens_per_microsecond * std::chrono::duration_cast<std::chrono::microseconds>(burst_period).count())
        , _max_donation_capacity(token_info ?
            _tokens_per_microsecond * std::chrono::duration_cast<std::chrono::microseconds>(burst_period).count() * token_info->nr_sharing() : 0
        )
        , _token_info(token_info)
    {}

    /// \brief create a token bucket limited to \c rate_per_second units per second, disallowing donations
    ///
    /// \param rate_per_second the rate per second that this bucket limits
    /// \param burst_period the maximum period for which to accumulate unused tokens
    token_bucket(uint64_t rate_per_second, std::chrono::milliseconds burst_period = std::chrono::milliseconds(100))
        : token_bucket(rate_per_second, nullptr, burst_period)
    {}

    /// \brief Refill existing tokens
    ///
    /// Refill tokens according to the amount of time passed since last invocation.
    /// Must be called often by users of this facility.
    /// \param now the current time
    void refill_tokens(typename Clock::time_point now = Clock::now()) {
        assert(now > _last_update);
        // We want to update tokens every time we are called. This is specially important for very fast devices
        // However, there can be rounding issues that can arise from manipulating such short periods, specially
        // for low-bandwidth devices.
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(now - _last_update);
        if (delta >= std::chrono::microseconds(1)) {
            _last_update = now;
            auto new_tokens = delta.count() * _tokens_per_microsecond;
            _tokens += new_tokens;
            _tokens_added_since_last_ms += new_tokens;
        }

        // At larger periods, we verify if the token count matches what we should have done and correct.
        auto delta_big = std::chrono::duration_cast<std::chrono::microseconds>(now - _last_update_big);
        if (delta_big > std::chrono::milliseconds(1)) {
            auto should = delta_big.count() * _tokens_per_microsecond;
            if (should != _tokens_added_since_last_ms) {
                _tokens += should - _tokens_added_since_last_ms;
            }
            _tokens_added_since_last_ms = 0;
            _last_update_big = now;
        }

        if (_tokens >= _max_capacity) {
            _tokens = _max_capacity;
        }
    }

    /// \brief Reserve tokens for later usage.
    ///
    /// As an example usage, if this bucket is used to control submission of I/O, the user
    /// has to call this method when it decides to issue I/O. When I/O is allowed, then
    /// \ref acquire_tokens is called.
    ///
    /// \param reserve amount of tokens to reserve
    void reserve_tokens(uint64_t reserve) {
        _reserved_tokens += reserve;
        if (_token_info) {
            _token_info->update_backlog(_reserved_tokens, total_tokens());
        }
    }

    /// \brief Acquire \c tokens from this bucket
    ///
    /// \param tokens the amount of tokens to acquire. If taking too many tokens the bucket
    //  may go negative. If that is undesired the caller must assure it is possible to acquire
    //  by using \c can_acquire. Passing a negative number will effectively increase the capacity
    //  of the bucket.
    void acquire_tokens(uint64_t tokens) {
        if ((tokens > total_tokens()) || (tokens > _reserved_tokens)) {
            throw bucket_overload();
        }
        _reserved_tokens -= tokens;
        auto main_tokens = std::min(tokens, uint64_t(_tokens));
        _tokens -= main_tokens;
        auto delta = tokens - main_tokens;
        if (delta > 0) {
            _donated_tokens -= delta;
        }
        if (_token_info) {
            _token_info->update_backlog(_reserved_tokens, total_tokens());
        }
    }

    /// Test whether it is possible to acquire \c tokens from this bucket
    ///
    /// \param tokens the amount of tokens to test for
    bool can_acquire(uint64_t tokens) const {
        return total_tokens() >= tokens;
    }

    /// \brief amount of donatable tokens in this bucket
    ///
    /// \return the amount of tokens in this bucket that are not reserved and can be donated.
    uint64_t donatable_tokens() {
        if (_tokens > _reserved_tokens) {
            return _tokens - _reserved_tokens;
        }
        return 0;
    }

    /// \brief donate tokens
    ///
    /// \param donation the amount of tokens to donate. After this call, the capacity of this bucket
    /// is reduced accordingly.
    void donate_tokens(uint64_t donation) {
        if (_tokens < donation) {
            throw bucket_overload();
        }
        _tokens -= donation;
    }

    /// \brief receive a token donation from a foreign bucket
    ///
    /// \param donated_tokens how many tokens a foreign bucket assigned to us
    void receive_donation(uint64_t donated_tokens) {
        _donated_tokens += donated_tokens;
        if (_donated_tokens > _max_donation_capacity) {
            _donated_tokens = _max_donation_capacity;
        }
        if (_token_info) {
            _token_info->update_backlog(_reserved_tokens, total_tokens());
        }
    }
};
}

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

namespace seastar {

/// Exception thrown when we remove more tokens than the bucket has
/// \ref semaphore::broken().
class bucket_overload: public std::exception {
public:
    /// Reports the exception reason.
    virtual const char* what() const noexcept {
        return "Bucket overload";
    }
};


/// Implementation of the token_bucket algoritm
///
/// The Token Bucket algorithm allows for a process to be controlled for throughput, with
/// potentially bursty activity.
///
/// The basic idea is that at regular intervals tokens are added to a bucket, and users will
/// consume them. Because users are limited to the existing tokens, throughput of the process is
/// controlled as a result.
///
/// The main difference in our implementation is that the updates are not periodic. That is because
/// this is intended to be used from seastar pollers. Specially in idle systems, pollers can be run
/// at any time.
///
/// The algorithm specifies a maximum burst parameter. After that period passes, we stop accumulating
/// tokens if we are already at the maximum.
template <typename Clock = std::chrono::steady_clock>
class token_bucket {
    typename Clock::time_point _last_update;
    typename Clock::time_point _last_update_big;
    double _tokens_per_microsecond;
    double _tokens_added_since_last_ms;
    double _tokens = 0;
    double _max_capacity;
    static double tokens_per_microsecond(uint64_t rate_per_second) {
        return double(rate_per_second) / std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::seconds(1)).count();
    }
public:
    /// \brief create a token bucket limited to \c rate_per_second units per second.
    ///
    /// \param rate_per_second the rate per second that this bucket limits
    /// \param burst_period the maximum period for which to accumulate unused tokens
    token_bucket(uint64_t rate_per_second, std::chrono::milliseconds burst_period = std::chrono::milliseconds(100))
        : _last_update(Clock::now())
        , _last_update_big(Clock::now())
        , _tokens_per_microsecond(tokens_per_microsecond(rate_per_second))
        , _max_capacity(_tokens_per_microsecond * std::chrono::duration_cast<std::chrono::microseconds>(burst_period).count())
    {}

    /// Refill existing tokens, according to the amount of time passed since last invocation
    /// Must be called often by users of this facility.
    void refill_tokens() {
        if (_tokens >= _max_capacity) {
            return;
        }

        auto now = Clock::now();
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
    }

    /// Acquire \c tokens from this bucket
    ///
    /// \param tokens the amount of tokens to acquire. If taking too many tokens the bucket
    //  may go negative. If that is undesired the caller must assure it is possible to acquire
    //  by using \c can_acquire. Passing a negative number will effectively increase the capacity
    //  of the bucket.
    void acquire_tokens(uint64_t tokens) {
        if (tokens > _tokens) {
            throw bucket_overload();
        }
        _tokens -= tokens;
    }

    /// Test whether it is possible to acquire \c tokens from this bucket
    ///
    /// \param tokens the amount of tokens to test for
    bool can_acquire(uint64_t tokens) const {
        return _tokens >= tokens;
    }
};
}

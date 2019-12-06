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
#include <seastar/core/lowres_clock.hh>
#include <seastar/core/semaphore.hh>
#include <chrono>

#pragma once

namespace seastar {
using timeout_clock = seastar::lowres_clock;
using timeout_semaphore = seastar::basic_semaphore<seastar::default_timeout_exception_factory, timeout_clock>;
using timeout_semaphore_units = seastar::semaphore_units<seastar::default_timeout_exception_factory, timeout_clock>;
static constexpr timeout_clock::time_point no_timeout = timeout_clock::time_point::max();

class timeout_exception : public std::runtime_error {
public:
    timeout_exception(const sstring& msg) : std::runtime_error("Timed out while " + msg) {}
};
}

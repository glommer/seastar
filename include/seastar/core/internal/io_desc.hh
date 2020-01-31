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
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#pragma once

#include <seastar/core/linux-aio.hh>
#include <exception>

namespace seastar {

class kernel_completion {
public:
    virtual ~kernel_completion() = default;

    virtual void set_exception(std::exception_ptr eptr) {}
    virtual void set_value(size_t res) {}
    virtual void destroy() {}
};

class promise_io_desc : public kernel_completion {
    promise<size_t> _pr;
public:
    virtual void set_exception(std::exception_ptr eptr) {
        _pr.set_exception(std::move(eptr));
    }

    virtual void set_value(size_t res) {
        _pr.set_value(res);
    }

    future<size_t> get_future() {
        return _pr.get_future();
    }
};

}

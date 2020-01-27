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
 * Copyright 2020 ScyllaDB
 */

#pragma once

#include <seastar/core/sstring.hh>
#include <seastar/core/linux-aio.hh>
#include <seastar/core/internal/liburing.hh>

namespace seastar {
class io_queue;

namespace internal {

class io_request {
public:
    enum class operation { read, readv, write, writev, fdatasync };
    operation _op;
    int _fd;
    uint64_t _pos;
    void *_address;
    size_t _size;
    bool _iopoll_available = false;
    friend class io_queue;

    io_request(operation op, int fd, uint64_t pos, void* address, size_t size)
        : _op(op)
        , _fd(fd)
        , _pos(pos)
        , _address(address)
        , _size(size)
    {}
    io_request(operation op, int fd) : io_request(op, fd, 0, nullptr, 0) {}
public:
    bool is_read() const {
        return ((_op == operation::read) || (_op == operation::readv));
    }

    bool is_write() const {
        return ((_op == operation::write) || (_op == operation::writev));
    }

    sstring opname() const;

    operation opcode() const {
        return _op;
    }

    int fd() const {
        return _fd;
    }

    uint64_t pos() const {
        return _pos;
    }

    void* address() const {
        return _address;
    }

    size_t size() const {
        return _size;
    }

    bool device_supports_iopoll() const {
        return _iopoll_available;
    }

    static io_request make_read(int fd, uint64_t pos, void *address, size_t size) {
        return io_request(operation::read, fd, pos, address, size);
    }

    static io_request make_readv(int fd, uint64_t pos, std::vector<iovec>& iov) {
        return io_request(operation::readv, fd, pos, iov.data(), iov.size());
    }

    static io_request make_write(int fd, uint64_t pos, const void *address, size_t size) {
        return io_request(operation::write, fd, pos, const_cast<void*>(address), size);
    }

    static io_request make_writev(int fd, uint64_t pos, std::vector<iovec>& iov) {
        return io_request(operation::writev, fd, pos, iov.data(), iov.size());
    }

    static io_request make_fdatasync(int fd) {
        return io_request(operation::fdatasync, fd);
    }
};
}
}

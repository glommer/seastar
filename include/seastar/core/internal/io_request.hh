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
#include <seastar/core/internal/io_desc.hh>
#include <variant>
#include <sys/types.h>
#include <sys/socket.h>
#include <fmt/format.h>

namespace seastar {
namespace internal {

class io_request {
public:
    enum class operation { read, readv, write, writev, fdatasync, recv, recvmsg, send, sendmsg, accept, connect, poll_add, poll_remove, cancel };
private:
    operation _op;
    int _fd;
    uint64_t _attr;
    // the upper layers give us void pointers, but storing void pointers here is just
    // dangerous. The constructors seem to be happy to convert other pointers to void*,
    // even if they are marked as explicit, and then you end up losing approximately 3 hours
    // and 15 minutes (hypothetically, of course), trying to chase the weirdest bug.
    // Let's store a char* for safety, and cast it back to void* in the accessor.
    std::variant<char*, iovec*, ::msghdr*, sockaddr*> _ptr;
    // accept wants a socklen_t*, connect wants a socklen_t
    std::variant<size_t,socklen_t*,socklen_t> _size;
    kernel_completion* _kernel_completion;
    bool _force_async = false;

    explicit io_request(operation op, int fd, uint64_t attr, ::msghdr* msg)
        : _op(op)
        , _fd(fd)
        , _attr(attr)
        , _ptr(msg)
    {}

    explicit io_request(operation op, int fd, sockaddr* sa, socklen_t sl)
        : _op(op)
        , _fd(fd)
        , _ptr(sa)
        , _size(sl)
    {}

    explicit io_request(operation op, int fd, uint64_t attr, sockaddr* sa, socklen_t* sl)
        : _op(op)
        , _fd(fd)
        , _attr(attr)
        , _ptr(sa)
        , _size(sl)
    {}
    explicit io_request(operation op, int fd, uint64_t attr, char* ptr, size_t size)
        : _op(op)
        , _fd(fd)
        , _attr(attr)
        , _ptr(ptr)
        , _size(size)
    {} 

    explicit io_request(operation op, int fd, uint64_t attr, iovec* ptr, size_t size)
        : _op(op)
        , _fd(fd)
        , _attr(attr)
        , _ptr(ptr)
        , _size(size)
    {}

    explicit io_request(operation op, int fd)
        : _op(op)
        , _fd(fd)
    {}
    explicit io_request(operation op, int fd, uint64_t attr)
        : _op(op)
        , _fd(fd)
        , _attr(attr)
    {}

    explicit io_request(operation op, int fd, char *ptr)
        : _op(op)
        , _fd(fd)
        , _ptr(ptr)
    {}
public:
    bool is_read() const {
        switch (_op) {
        case operation::read:
        case operation::readv:
        case operation::recvmsg:
        case operation::recv:
            return true;
        default:
            return false;
        }
    }

    bool is_write() const {
        switch (_op) {
        case operation::write:
        case operation::writev:
        case operation::send:
        case operation::sendmsg:
            return true;
        default:
            return false;
        }
    }

    bool should_force_async() const {
        return _force_async;
    }

    void force_async() {
        _force_async = true;
    }

    sstring opname() const;

    operation opcode() const {
        return _op;
    }

    int fd() const {
        return _fd;
    }

    uint64_t pos() const {
        return _attr;
    }

    int flags() const {
        return int(_attr);
    }

    int events() const {
        return int(_attr);
    }

    void* address() const {
        return reinterpret_cast<void*>(std::get<char*>(_ptr));
    }

    iovec* iov() const {
        return std::get<iovec*>(_ptr);
    }

    ::sockaddr* posix_sockaddr() const {
        return std::get<sockaddr*>(_ptr);
    }

    ::msghdr* msghdr() const {
        return std::get<::msghdr*>(_ptr);
    }

    size_t size() const {
        return std::get<size_t>(_size);
    }

    size_t iov_len() const {
        return std::get<size_t>(_size);
    }

    socklen_t socklen() const {
        return std::get<socklen_t>(_size);
    }

    socklen_t* socklen_ptr() const {
        return std::get<socklen_t*>(_size);
    }

    void attach_kernel_completion(kernel_completion* kc) {
        _kernel_completion = kc;
    }

    kernel_completion* get_kernel_completion() const {
        return _kernel_completion;
    }

    static io_request make_read(int fd, uint64_t pos, void* address, size_t size) {
        return io_request(operation::read, fd, pos, reinterpret_cast<char*>(address), size);
    }

    static io_request make_readv(int fd, uint64_t pos, std::vector<iovec>& iov) {
        return io_request(operation::readv, fd, pos, iov.data(), iov.size());
    }

    static io_request make_recv(int fd, void* address, size_t size, int flags) {
        return io_request(operation::recv, fd, flags, reinterpret_cast<char*>(address), size);
    }

    static io_request make_recvmsg(int fd, ::msghdr* msg, int flags) {
        return io_request(operation::recvmsg, fd, flags, msg);
    }

    static io_request make_send(int fd, const void* address, size_t size, int flags) {
        return io_request(operation::send, fd, flags, const_cast<char*>(reinterpret_cast<const char*>(address)), size);
    }

    static io_request make_sendmsg(int fd, ::msghdr* msg, int flags) {
        return io_request(operation::sendmsg, fd, flags, msg);
    }

    static io_request make_write(int fd, uint64_t pos, const void* address, size_t size) {
        return io_request(operation::write, fd, pos, const_cast<char*>(reinterpret_cast<const char*>(address)), size);
    }

    static io_request make_writev(int fd, uint64_t pos, std::vector<iovec>& iov) {
        return io_request(operation::writev, fd, pos, iov.data(), iov.size());
    }

    static io_request make_fdatasync(int fd) {
        return io_request(operation::fdatasync, fd);
    }

    static io_request make_accept(int fd, struct sockaddr* addr, socklen_t* addrlen, int flags) {
        return io_request(operation::accept, fd, flags, addr, addrlen);
    }

    static io_request make_connect(int fd, struct sockaddr* addr, socklen_t addrlen) {
        return io_request(operation::connect, fd, addr, addrlen);
    }

    static io_request make_poll_add(int fd, int events) {
        return io_request(operation::poll_add, fd, events);
    }

    static io_request make_poll_remove(int fd, void *addr) {
        return io_request(operation::poll_remove, fd, reinterpret_cast<char*>(addr));
    }
    static io_request make_cancel(int fd, void *addr) {
        return io_request(operation::cancel, fd, reinterpret_cast<char*>(addr));
    }
};
}
}

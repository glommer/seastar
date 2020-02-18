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

#include <seastar/core/future.hh>
#include <seastar/core/posix.hh>
#include <vector>
#include <tuple>
#include <seastar/core/internal/io_desc.hh>
#include <boost/intrusive_ptr.hpp>

namespace seastar {

class reactor;
class pollable_fd;
class pollable_fd_state;
class socket_address;

namespace net {

class packet;

}

class pollable_fd_state;

using pollable_fd_state_ptr = boost::intrusive_ptr<pollable_fd_state>;

class pollable_fd_state {
    unsigned _refs = 0;
public:
    virtual ~pollable_fd_state() {}
    struct speculation {
        int events = 0;
        explicit speculation(int epoll_events_guessed = 0) : events(epoll_events_guessed) {}
    };
    pollable_fd_state(const pollable_fd_state&) = delete;
    void operator=(const pollable_fd_state&) = delete;
    void speculate_epoll(int events) { events_known |= events; }
    file_desc fd;
    bool events_rw = false;   // single consumer for both read and write (accept())
    bool no_more_recv = false; // For udp, there is no shutdown indication from the kernel
    bool no_more_send = false; // For udp, there is no shutdown indication from the kernel
    int events_requested = 0; // wanted by pollin/pollout promises
    int events_epoll = 0;     // installed in epoll
    int events_known = 0;     // returned from epoll

    friend class reactor;
    friend class pollable_fd;

    future<size_t> read_some(char* buffer, size_t size);
    future<size_t> read_some(uint8_t* buffer, size_t size);
    future<size_t> read_some(const std::vector<iovec>& iov);
    future<> write_all(const char* buffer, size_t size);
    future<> write_all(const uint8_t* buffer, size_t size);
    future<size_t> write_some(net::packet& p);
    future<> write_all(net::packet& p);
    future<> readable();
    future<> writeable();
    future<> readable_or_writeable();
    void abort_reader();
    void abort_writer();
    future<std::tuple<pollable_fd, socket_address>> accept();
    future<> connect(socket_address& sa);
    future<size_t> sendmsg(struct msghdr *msg);
    future<size_t> recvmsg(struct msghdr *msg);
    future<size_t> sendto(socket_address addr, const void* buf, size_t len);

    bool try_speculate_poll(int events) {
        if (events & events_known) {
            events_known &= ~events;
            return true;
        }
        return false;
    }
protected:
    explicit pollable_fd_state(file_desc fd, speculation speculate = speculation())
        : fd(std::move(fd)), events_known(speculate.events) {}

    void maybe_no_more_recv();
    void maybe_no_more_send();
    void forget(); // called on end-of-life

    friend void intrusive_ptr_add_ref(pollable_fd_state* fd) {
        ++fd->_refs;
    }
    friend void intrusive_ptr_release(pollable_fd_state* fd);
};

class pollable_fd {
public:
    using speculation = pollable_fd_state::speculation;
    pollable_fd() = default;
    pollable_fd(file_desc fd, speculation speculate = speculation());
public:
    future<size_t> read_some(char* buffer, size_t size) {
        return _s->read_some(buffer, size);
    }
    future<size_t> read_some(uint8_t* buffer, size_t size) {
        return _s->read_some(buffer, size);
    }
    future<size_t> read_some(const std::vector<iovec>& iov) {
        return _s->read_some(iov);
    }
    future<> write_all(const char* buffer, size_t size) {
        return _s->write_all(buffer, size);
    }
    future<> write_all(const uint8_t* buffer, size_t size) {
        return _s->write_all(buffer, size);
    }
    future<size_t> write_some(net::packet& p) {
        return _s->write_some(p);
    }
    future<> write_all(net::packet& p) {
        return _s->write_all(p);
    }
    future<> readable() {
        return _s->readable();
    }
    future<> writeable() {
        return _s->writeable();
    }
    future<> readable_or_writeable() {
        return _s->readable_or_writeable();
    }
    void abort_reader() {
        return _s->abort_reader();
    }
    void abort_writer() {
        return _s->abort_writer();
    }
    future<std::tuple<pollable_fd, socket_address>> accept() {
        return _s->accept();
    }
    future<> connect(socket_address& sa) {
        return _s->connect(sa);
    }
    future<size_t> sendmsg(struct msghdr *msg) {
        return _s->sendmsg(msg);
    }
    future<size_t> recvmsg(struct msghdr *msg) {
        return _s->recvmsg(msg);
    }
    future<size_t> sendto(socket_address addr, const void* buf, size_t len) {
        return _s->sendto(addr, buf, len);
    }
    file_desc& get_file_desc() const { return _s->fd; }
    void shutdown(int how);
    void close() { _s.reset(); }
    explicit operator bool() const noexcept {
        return bool(_s);
    }
protected:
    int get_fd() const { return _s->fd.get(); }
    void maybe_no_more_recv() { return _s->maybe_no_more_recv(); }
    void maybe_no_more_send() { return _s->maybe_no_more_send(); }
    friend class reactor;
    friend class readable_eventfd;
    friend class writeable_eventfd;
    friend class aio_storage_context;
private:
    pollable_fd_state_ptr _s;
};

class writeable_eventfd;

class readable_eventfd {
    pollable_fd _fd;
public:
    explicit readable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    readable_eventfd(readable_eventfd&&) = default;
    writeable_eventfd write_side();
    future<size_t> wait();
    int get_write_fd() { return _fd.get_fd(); }
private:
    explicit readable_eventfd(file_desc&& fd) : _fd(std::move(fd)) {}
    static file_desc try_create_eventfd(size_t initial);

    friend class writeable_eventfd;
};

class writeable_eventfd {
    file_desc _fd;
public:
    explicit writeable_eventfd(size_t initial = 0) : _fd(try_create_eventfd(initial)) {}
    writeable_eventfd(writeable_eventfd&&) = default;
    readable_eventfd read_side();
    void signal(size_t nr);
    int get_read_fd() { return _fd.get(); }
private:
    explicit writeable_eventfd(file_desc&& fd) : _fd(std::move(fd)) {}
    static file_desc try_create_eventfd(size_t initial);

    friend class readable_eventfd;
};

}

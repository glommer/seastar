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
#include "core/reactor_backend.hh"
#include "core/thread_pool.hh"
#include "core/syscall_result.hh"
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/util/defer.hh>
#include <seastar/util/read_first_line.hh>
#include <seastar/core/internal/liburing.hh>
#include <seastar/core/metrics.hh>
#include <chrono>
#include <sys/poll.h>
#include <sys/syscall.h>
#include <boost/intrusive/parent_from_member.hpp>

#ifdef HAVE_OSV
#include <osv/newpoll.hh>
#endif

namespace seastar {

using namespace std::chrono_literals;
using namespace internal;
using namespace internal::linux_abi;
namespace fs = seastar::compat::filesystem;

class pollable_fd_state_completion : public kernel_completion {
    promise<> _pr;
public:
    virtual void complete_with(ssize_t res) override {
        _pr.set_value();
    }
    future<> get_future() {
        return _pr.get_future();
    }
};

void prepare_iocb(io_request& req, iocb& iocb) {
    switch (req.opcode()) {
    case io_request::operation::fdatasync:
        iocb = make_fdsync_iocb(req.fd());
        break;
    case io_request::operation::write:
        iocb = make_write_iocb(req.fd(), req.pos(), req.address(), req.size());
        break;
    case io_request::operation::writev:
        iocb = make_writev_iocb(req.fd(), req.pos(), req.iov(), req.size());
        break;
    case io_request::operation::read:
        iocb = make_read_iocb(req.fd(), req.pos(), req.address(), req.size());
        break;
    case io_request::operation::readv:
        iocb = make_readv_iocb(req.fd(), req.pos(), req.iov(), req.size());
        break;
    default:
        seastar_logger.error("Invalid operation for iocb: {}", req.opname());
        std::abort();
    }
    set_user_data(iocb, req.get_kernel_completion());
}

aio_storage_context::iocb_pool::iocb_pool() {
    for (unsigned i = 0; i != max_aio; ++i) {
        _free_iocbs.push(&_iocb_pool[i]);
    }
}

aio_storage_context::aio_storage_context(reactor* r)
    : _r(r)
    , _io_context(0) {
    static_assert(max_aio >= reactor::max_queues * reactor::max_queues,
                  "Mismatch between maximum allowed io and what the IO queues can produce");
    internal::setup_aio_context(max_aio, &_io_context);
}

aio_storage_context::~aio_storage_context() {
    internal::io_destroy(_io_context);
}

inline
internal::linux_abi::iocb&
aio_storage_context::iocb_pool::get_one() {
    auto io = _free_iocbs.top();
    _free_iocbs.pop();
    return *io;
}

inline
void
aio_storage_context::iocb_pool::put_one(internal::linux_abi::iocb* io) {
    _free_iocbs.push(io);
}

inline
unsigned
aio_storage_context::iocb_pool::outstanding() const {
    return max_aio - _free_iocbs.size();
}

inline
bool
aio_storage_context::iocb_pool::has_capacity() const {
    return !_free_iocbs.empty();
}

// Returns: number of iocbs consumed (0 or 1)
size_t
aio_storage_context::handle_aio_error(linux_abi::iocb* iocb, int ec) {
    switch (ec) {
        case EAGAIN:
            return 0;
        case EBADF: {
            auto desc = reinterpret_cast<kernel_completion*>(get_user_data(*iocb));
            _iocb_pool.put_one(iocb);
            desc->complete_with(-EBADF);
            // if EBADF, it means that the first request has a bad fd, so
            // we will only remove it from _pending_io and try again.
            return 1;
        }
        default:
            ++_r->_io_stats.aio_errors;
            throw_system_error_on(true, "io_submit");
            abort();
    }
}

extern bool aio_nowait_supported;

bool
aio_storage_context::submit_work() {
    size_t pending = _r->_pending_io.size();
    size_t to_submit = 0;
    bool did_work = false;

    _submission_queue.resize(0);
    while ((pending > to_submit) && _iocb_pool.has_capacity()) {
        auto& req = _r->_pending_io[to_submit++];
        auto& io = _iocb_pool.get_one();
        prepare_iocb(req, io);

        if (_r->_aio_eventfd) {
            set_eventfd_notification(io, _r->_aio_eventfd->get_fd());
        }
        if (aio_nowait_supported) {
            set_nowait(io, true);
        }
        _submission_queue.push_back(&io);
    }

    size_t submitted = 0;
    while (to_submit > submitted) {
        auto nr = to_submit - submitted;
        auto iocbs = _submission_queue.data() + submitted;
        auto r = io_submit(_io_context, nr, iocbs);
        size_t nr_consumed;
        if (r == -1) {
            nr_consumed = handle_aio_error(iocbs[0], errno);
        } else {
            nr_consumed = size_t(r);
        }
        submitted += nr_consumed;
    }
    _r->_pending_io.erase(_r->_pending_io.begin(), _r->_pending_io.begin() + submitted);

    if (!_pending_aio_retry.empty()) {
        auto retries = std::exchange(_pending_aio_retry, {});
        // FIXME: future is discarded
        (void)_r->_thread_pool->submit<syscall_result<int>>([this, retries] () mutable {
            auto r = io_submit(_io_context, retries.size(), retries.data());
            return wrap_syscall<int>(r);
        }).then([this, retries] (syscall_result<int> result) {
            auto iocbs = retries.data();
            size_t nr_consumed = 0;
            if (result.result == -1) {
                nr_consumed = handle_aio_error(iocbs[0], result.error);
            } else {
                nr_consumed = result.result;
            }
            std::copy(retries.begin() + nr_consumed, retries.end(), std::back_inserter(_pending_aio_retry));
        });
        did_work = true;
    }
    return did_work;
}

bool aio_storage_context::reap_completions()
{
    io_event ev[max_aio];
    struct timespec timeout = {0, 0};
    auto n = io_getevents(_io_context, 1, max_aio, ev, &timeout, _r->_force_io_getevents_syscall);
    if (n == -1 && errno == EINTR) {
        n = 0;
    }
    assert(n >= 0);
    unsigned nr_retry = 0;
    for (size_t i = 0; i < size_t(n); ++i) {
        auto iocb = get_iocb(ev[i]);
        if (ev[i].res == -EAGAIN) {
            ++nr_retry;
            set_nowait(*iocb, false);
            _pending_aio_retry.push_back(iocb);
            continue;
        }
        _iocb_pool.put_one(iocb);
        auto desc = reinterpret_cast<kernel_completion*>(ev[i].data);
        desc->complete_with(ev[i].res);
    }
    return n;
}

bool aio_storage_context::can_sleep() const {
    // Because aio depends on polling, it cannot generate events to wake us up, Therefore, sleep
    // is only possible if there are no in-flight aios. If there are, we need to keep polling.
    //
    // Alternatively, if we enabled _aio_eventfd, we can always enter
    unsigned executing = _iocb_pool.outstanding();
    return executing == 0 || _r->_aio_eventfd;
}

aio_general_context::aio_general_context(size_t nr) : iocbs(new iocb*[nr]) {
    setup_aio_context(nr, &io_context);
}

aio_general_context::~aio_general_context() {
    io_destroy(io_context);
}

void aio_general_context::queue(linux_abi::iocb* iocb) {
    *last++ = iocb;
}

size_t aio_general_context::flush() {
    if (last != iocbs.get()) {
        auto nr = last - iocbs.get();
        last = iocbs.get();
        auto r = io_submit(io_context, nr, iocbs.get());
        assert(r >= 0);
        return nr;
    }
    return 0;
}

completion_with_iocb::completion_with_iocb(int fd, int events, void* user_data)
    : _iocb(make_poll_iocb(fd, events)) {
    set_user_data(_iocb, user_data);
}

void completion_with_iocb::maybe_queue(aio_general_context& context) {
    if (!_in_context) {
        _in_context = true;
        context.queue(&_iocb);
    }
}

hrtimer_aio_completion::hrtimer_aio_completion(reactor* r, file_desc& fd)
    : fd_kernel_completion(r, fd)
    , completion_with_iocb(fd.get(), POLLIN, this) {}

task_quota_aio_completion::task_quota_aio_completion(reactor* r, file_desc& fd)
    : fd_kernel_completion(r, fd)
    , completion_with_iocb(fd.get(), POLLIN, this) {}

smp_wakeup_aio_completion::smp_wakeup_aio_completion(reactor* r, file_desc& fd)
        : fd_kernel_completion(r, fd)
        , completion_with_iocb(fd.get(), POLLIN, this) {}

void
hrtimer_aio_completion::complete_with(ssize_t ret) {
    uint64_t expirations = 0;
    (void)_fd.read(&expirations, 8);
    if (expirations) {
        _r->service_highres_timer();
    }
    completion_with_iocb::completed();
}

void
task_quota_aio_completion::complete_with(ssize_t ret) {
    uint64_t v;
    (void)_fd.read(&v, 8);
    completion_with_iocb::completed();
}

void
smp_wakeup_aio_completion::complete_with(ssize_t ret) {
    uint64_t ignore = 0;
    (void)_fd.read(&ignore, 8);
    completion_with_iocb::completed();
}

void
hrtimer_uring_completion::complete_with(ssize_t ret) {
    uint64_t expirations = 0;
    (void)_fd.read(&expirations, 8);
    if (expirations) {
        _r->service_highres_timer();
    }
    set_registered(false);
}

void
smp_wakeup_uring_completion::complete_with(ssize_t ret) {
    uint64_t ignore = 0;
    (void)_fd.read(&ignore, 8);
    set_registered(false);
}

preempt_io_context::preempt_io_context(reactor* r, file_desc& task_quota, file_desc& hrtimer)
    : _r(r)
    , _task_quota_aio_completion(r, task_quota)
    , _hrtimer_aio_completion(r, hrtimer)
{}

void preempt_io_context::start_tick() {
    // Preempt whenever an event (timer tick or signal) is available on the
    // _preempting_io ring
    g_need_preempt = reinterpret_cast<const preemption_monitor*>(_context.io_context + 8);
    // preempt_io_context::request_preemption() will write to reactor::_preemption_monitor, which is now ignored
}

void preempt_io_context::stop_tick() {
    g_need_preempt = &_r->_preemption_monitor;
}

void preempt_io_context::request_preemption() {
    ::itimerspec expired = {};
    expired.it_value.tv_nsec = 1;
    // will trigger immediately, triggering the preemption monitor
    _hrtimer_aio_completion.fd().timerfd_settime(TFD_TIMER_ABSTIME, expired);

    // This might have been called from poll_once. If that is the case, we cannot assume that timerfd is being
    // monitored.
    _hrtimer_aio_completion.maybe_queue(_context);
    _context.flush();

    // The kernel is not obliged to deliver the completion immediately, so wait for it
    while (!need_preempt()) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }
}

void preempt_io_context::reset_preemption_monitor() {
    service_preempting_io();
    _hrtimer_aio_completion.maybe_queue(_context);
    _task_quota_aio_completion.maybe_queue(_context);
    flush();
}

bool preempt_io_context::service_preempting_io() {
    linux_abi::io_event a[2];
    auto r = io_getevents(_context.io_context, 0, 2, a, 0);
    assert(r != -1);
    bool did_work = r > 0;
    for (unsigned i = 0; i != unsigned(r); ++i) {
        auto desc = reinterpret_cast<kernel_completion*>(a[i].data);
        desc->complete_with(a[i].res);
    }
    return did_work;
}

file_desc reactor_backend_aio::make_timerfd() {
    return file_desc::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC|TFD_NONBLOCK);
}

bool reactor_backend_aio::await_events(int timeout, const sigset_t* active_sigmask) {
    ::timespec ts = {};
    ::timespec* tsp = [&] () -> ::timespec* {
        if (timeout == 0) {
            return &ts;
        } else if (timeout == -1) {
            return nullptr;
        } else {
            ts = posix::to_timespec(timeout * 1ms);
            return &ts;
        }
    }();
    constexpr size_t batch_size = 128;
    io_event batch[batch_size];
    bool did_work = false;
    int r;
    do {
        r = io_pgetevents(_polling_io.io_context, 1, batch_size, batch, tsp, active_sigmask);
        if (r == -1 && errno == EINTR) {
            return true;
        }
        assert(r != -1);
        for (unsigned i = 0; i != unsigned(r); ++i) {
            did_work = true;
            auto& event = batch[i];
            auto* desc = reinterpret_cast<kernel_completion*>(uintptr_t(event.data));
            desc->complete_with(event.res);
        }
        // For the next iteration, don't use a timeout, since we may have waited already
        ts = {};
        tsp = &ts;
    } while (r == batch_size);
    return did_work;
}

void reactor_backend_aio::signal_received(int signo, siginfo_t* siginfo, void* ignore) {
    engine()._signals.action(signo, siginfo, ignore);
}

reactor_backend_aio::reactor_backend_aio(reactor* r)
    : _r(r)
    , _hrtimer_timerfd(make_timerfd())
    , _storage_context(_r)
    , _preempting_io(_r, _r->_task_quota_timer, _hrtimer_timerfd)
    , _hrtimer_poll_completion(_r, _hrtimer_timerfd)
    , _smp_wakeup_aio_completion(_r, _r->_notify_eventfd)
{
    // Protect against spurious wakeups - if we get notified that the timer has
    // expired when it really hasn't, we don't want to block in read(tfd, ...).
    auto tfd = _r->_task_quota_timer.get();
    ::fcntl(tfd, F_SETFL, ::fcntl(tfd, F_GETFL) | O_NONBLOCK);

    sigset_t mask = make_sigset_mask(hrtimer_signal());
    auto e = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    assert(e == 0);
}

bool reactor_backend_aio::reap_kernel_completions() {
    bool did_work = await_events(0, nullptr);
    did_work |= _storage_context.reap_completions();
    return did_work;
}

bool reactor_backend_aio::kernel_submit_work() {
    _hrtimer_poll_completion.maybe_queue(_polling_io);
    bool did_work = _polling_io.flush();
    did_work |= _storage_context.submit_work();
    return did_work;
}

bool reactor_backend_aio::kernel_events_can_sleep() const {
    return _storage_context.can_sleep();
}

void reactor_backend_aio::wait_and_process_events(const sigset_t* active_sigmask) {
    int timeout = -1;
    bool did_work = _preempting_io.service_preempting_io();
    if (did_work) {
        timeout = 0;
    }

    _hrtimer_poll_completion.maybe_queue(_polling_io);
    _smp_wakeup_aio_completion.maybe_queue(_polling_io);
    _polling_io.flush();
    await_events(timeout, active_sigmask);
    _preempting_io.service_preempting_io(); // clear task quota timer
}

class aio_pollable_fd_state : public pollable_fd_state {
    internal::linux_abi::iocb _iocb_pollin;
    pollable_fd_state_completion _completion_pollin;

    internal::linux_abi::iocb _iocb_pollout;
    pollable_fd_state_completion _completion_pollout;
public:
    pollable_fd_state_completion* get_desc(int events) {
        if (events & POLLIN) {
            return &_completion_pollin;
        }
        return &_completion_pollout;
    }
    internal::linux_abi::iocb* get_iocb(int events) {
        if (events & POLLIN) {
            return &_iocb_pollin;
        }
        return &_iocb_pollout;
    }
    explicit aio_pollable_fd_state(file_desc fd, speculation speculate)
        : pollable_fd_state(std::move(fd), std::move(speculate))
    {}
    future<> get_completion_future(int events) {
        return get_desc(events)->get_future();
    }
};

future<> reactor_backend_aio::poll(pollable_fd_state& fd, int events) {
    try {
        if (fd.try_speculate_poll(events)) {
            return make_ready_future<>();
        }

        fd.events_rw = events == (POLLIN|POLLOUT);

        auto* pfd = static_cast<aio_pollable_fd_state*>(&fd);
        auto* iocb = pfd->get_iocb(events);
        auto* desc = pfd->get_desc(events);
        *iocb = make_poll_iocb(fd.fd.get(), events);
        *desc = pollable_fd_state_completion{};
        set_user_data(*iocb, desc);
        _polling_io.queue(iocb);
        return pfd->get_completion_future(events);
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<> reactor_backend_aio::readable(pollable_fd_state& fd) {
    return poll(fd, POLLIN);
}

future<> reactor_backend_aio::writeable(pollable_fd_state& fd) {
    return poll(fd, POLLOUT);
}

future<> reactor_backend_aio::readable_or_writeable(pollable_fd_state& fd) {
    return poll(fd, POLLIN|POLLOUT);
}

void reactor_backend_aio::forget(pollable_fd_state& fd) noexcept {
    auto* pfd = static_cast<aio_pollable_fd_state*>(&fd);
    delete pfd;
    // ?
}

future<std::tuple<pollable_fd, socket_address>>
reactor_backend_aio::accept(pollable_fd_state& listenfd) {
    return engine().do_accept(listenfd);
}

future<> reactor_backend_aio::connect(pollable_fd_state& fd, socket_address& sa) {
    return engine().do_connect(fd, sa);
}

void reactor_backend_aio::shutdown(pollable_fd_state& fd, int how) {
    fd.fd.shutdown(how);
}

future<size_t>
reactor_backend_aio::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    return engine().do_read_some(fd, buffer, len);
}

future<size_t>
reactor_backend_aio::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    return engine().do_read_some(fd, iov);
}

future<size_t>
reactor_backend_aio::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    return engine().do_write_some(fd, buffer, len);
}

future<size_t>
reactor_backend_aio::write_some(pollable_fd_state& fd, net::packet& p) {
    return engine().do_write_some(fd, p);
}

void reactor_backend_aio::start_tick() {
    _preempting_io.start_tick();
}

void reactor_backend_aio::stop_tick() {
    _preempting_io.stop_tick();
}

void reactor_backend_aio::arm_highres_timer(const ::itimerspec& its) {
    _hrtimer_timerfd.timerfd_settime(TFD_TIMER_ABSTIME, its);
}

void reactor_backend_aio::reset_preemption_monitor() {
    _preempting_io.reset_preemption_monitor();
}

void reactor_backend_aio::request_preemption() {
    _preempting_io.request_preemption();
}

void reactor_backend_aio::start_handling_signal() {
    // The aio backend only uses SIGHUP/SIGTERM/SIGINT. We don't need to handle them right away and our
    // implementation of request_preemption is not signal safe, so do nothing.
}

pollable_fd_state_ptr
reactor_backend_aio::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return pollable_fd_state_ptr(new aio_pollable_fd_state(std::move(fd), std::move(speculate)));
}

reactor_backend_epoll::reactor_backend_epoll(reactor* r)
        : _r(r)
        , _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC))
        , _storage_context(_r) {
    ::epoll_event event;
    event.events = EPOLLIN;
    event.data.ptr = nullptr;
    auto ret = ::epoll_ctl(_epollfd.get(), EPOLL_CTL_ADD, _r->_notify_eventfd.get(), &event);
    throw_system_error_on(ret == -1);

    struct sigevent sev;
    sev.sigev_notify = SIGEV_THREAD_ID;
    sev._sigev_un._tid = syscall(SYS_gettid);
    sev.sigev_signo = hrtimer_signal();
    ret = timer_create(CLOCK_MONOTONIC, &sev, &_steady_clock_timer);
    assert(ret >= 0);

    _r->_signals.handle_signal(hrtimer_signal(), [r = _r] {
        r->service_highres_timer();
    });
}

reactor_backend_epoll::~reactor_backend_epoll() {
    timer_delete(_steady_clock_timer);
}

void reactor_backend_epoll::start_tick() {
    _task_quota_timer_thread = std::thread(&reactor::task_quota_timer_thread_fn, _r);

    ::sched_param sp;
    sp.sched_priority = 1;
    auto sched_ok = pthread_setschedparam(_task_quota_timer_thread.native_handle(), SCHED_FIFO, &sp);
    if (sched_ok != 0 && _r->_id == 0) {
        seastar_logger.warn("Unable to set SCHED_FIFO scheduling policy for timer thread; latency impact possible. Try adding CAP_SYS_NICE");
    }
}

void reactor_backend_epoll::stop_tick() {
    _r->_dying.store(true, std::memory_order_relaxed);
    _r->_task_quota_timer.timerfd_settime(0, seastar::posix::to_relative_itimerspec(1ns, 1ms)); // Make the timer fire soon
    _task_quota_timer_thread.join();
}

void reactor_backend_epoll::arm_highres_timer(const ::itimerspec& its) {
    auto ret = timer_settime(_steady_clock_timer, TIMER_ABSTIME, &its, NULL);
    throw_system_error_on(ret == -1);
}

bool
reactor_backend_epoll::wait_and_process(int timeout, const sigset_t* active_sigmask) {
    std::array<epoll_event, 128> eevt;
    int nr = ::epoll_pwait(_epollfd.get(), eevt.data(), eevt.size(), timeout, active_sigmask);
    if (nr == -1 && errno == EINTR) {
        return false; // gdb can cause this
    }
    assert(nr != -1);
    for (int i = 0; i < nr; ++i) {
        auto& evt = eevt[i];
        auto pfd = reinterpret_cast<pollable_fd_state*>(evt.data.ptr);
        if (!pfd) {
            char dummy[8];
            _r->_notify_eventfd.read(dummy, 8);
            continue;
        }
        if (evt.events & (EPOLLHUP | EPOLLERR)) {
            // treat the events as required events when error occurs, let
            // send/recv/accept/connect handle the specific error.
            evt.events = pfd->events_requested;
        }
        auto events = evt.events & (EPOLLIN | EPOLLOUT);
        auto events_to_remove = events & ~pfd->events_requested;
        if (pfd->events_rw) {
            // accept() signals normal completions via EPOLLIN, but errors (due to shutdown())
            // via EPOLLOUT|EPOLLHUP, so we have to wait for both EPOLLIN and EPOLLOUT with the
            // same future
            complete_epoll_event(*pfd, events, EPOLLIN|EPOLLOUT);
        } else {
            // Normal processing where EPOLLIN and EPOLLOUT are waited for via different
            // futures.
            complete_epoll_event(*pfd, events, EPOLLIN);
            complete_epoll_event(*pfd, events, EPOLLOUT);
        }
        if (events_to_remove) {
            pfd->events_epoll &= ~events_to_remove;
            evt.events = pfd->events_epoll;
            auto op = evt.events ? EPOLL_CTL_MOD : EPOLL_CTL_DEL;
            ::epoll_ctl(_epollfd.get(), op, pfd->fd.get(), &evt);
        }
    }
    return nr;
}

class epoll_pollable_fd_state : public pollable_fd_state {
    pollable_fd_state_completion _pollin;
    pollable_fd_state_completion _pollout;

    pollable_fd_state_completion* get_desc(int events) {
        if (events & EPOLLIN) {
            return &_pollin;
        }
        return &_pollout;
    }
public:
    explicit epoll_pollable_fd_state(file_desc fd, speculation speculate)
        : pollable_fd_state(std::move(fd), std::move(speculate))
    {}
    future<> get_completion_future(int event) {
        auto desc = get_desc(event);
        *desc = pollable_fd_state_completion{};
        return desc->get_future();
    }

    void complete_with(int event) {
        get_desc(event)->complete_with(event);
    }
};

bool reactor_backend_epoll::reap_kernel_completions() {
    // epoll does not have a separate submission stage, and just
    // calls epoll_ctl everytime it needs, so this method and
    // kernel_submit_work are essentially the same. Ordering also
    // doesn't matter much. wait_and_process is actually completing,
    // but we prefer to call it in kernel_submit_work because the
    // reactor register two pollers for completions and one for submission,
    // since completion is cheaper for other backends like aio. This avoids
    // calling epoll_wait twice.
    //
    // We will only reap the io completions
    return _storage_context.reap_completions();
}

bool reactor_backend_epoll::kernel_submit_work() {
    _storage_context.submit_work();
    if (_need_epoll_events) {
        return wait_and_process(0, nullptr);
    }
    return false;
}

bool reactor_backend_epoll::kernel_events_can_sleep() const {
    return _storage_context.can_sleep();
}

void reactor_backend_epoll::wait_and_process_events(const sigset_t* active_sigmask) {
    wait_and_process(-1 , active_sigmask);
}

void reactor_backend_epoll::complete_epoll_event(pollable_fd_state& pfd, int events, int event) {
    if (pfd.events_requested & events & event) {
        pfd.events_requested &= ~event;
        pfd.events_known &= ~event;
        auto* fd = static_cast<epoll_pollable_fd_state*>(&pfd);
        return fd->complete_with(event);
    }
}

void reactor_backend_epoll::signal_received(int signo, siginfo_t* siginfo, void* ignore) {
    if (engine_is_ready()) {
        engine()._signals.action(signo, siginfo, ignore);
    } else {
        reactor::signals::failed_to_handle(signo);
    }
}

future<> reactor_backend_epoll::get_epoll_future(pollable_fd_state& pfd, int event) {
    if (pfd.try_speculate_poll(event)) {
        return make_ready_future();
    }
    pfd.events_rw = event == (EPOLLIN | EPOLLOUT);
    pfd.events_requested |= event;
    if ((pfd.events_epoll & event) != event) {
        auto ctl = pfd.events_epoll ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
        pfd.events_epoll |= event;
        ::epoll_event eevt;
        eevt.events = pfd.events_epoll;
        eevt.data.ptr = &pfd;
        int r = ::epoll_ctl(_epollfd.get(), ctl, pfd.fd.get(), &eevt);
        assert(r == 0);
        _need_epoll_events = true;
    }

    auto* fd = static_cast<epoll_pollable_fd_state*>(&pfd);
    return fd->get_completion_future(event);
}

future<> reactor_backend_epoll::readable(pollable_fd_state& fd) {
    return get_epoll_future(fd, EPOLLIN);
}

future<> reactor_backend_epoll::writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, EPOLLOUT);
}

future<> reactor_backend_epoll::readable_or_writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, EPOLLIN | EPOLLOUT);
}

void reactor_backend_epoll::forget(pollable_fd_state& fd) noexcept {
    if (fd.events_epoll) {
        ::epoll_ctl(_epollfd.get(), EPOLL_CTL_DEL, fd.fd.get(), nullptr);
    }
    auto* efd = static_cast<epoll_pollable_fd_state*>(&fd);
    delete efd;
}

future<std::tuple<pollable_fd, socket_address>>
reactor_backend_epoll::accept(pollable_fd_state& listenfd) {
    return engine().do_accept(listenfd);
}

future<> reactor_backend_epoll::connect(pollable_fd_state& fd, socket_address& sa) {
    return engine().do_connect(fd, sa);
}

void reactor_backend_epoll::shutdown(pollable_fd_state& fd, int how) {
    fd.fd.shutdown(how);
}

future<size_t>
reactor_backend_epoll::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    return engine().do_read_some(fd, buffer, len);
}

future<size_t>
reactor_backend_epoll::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    return engine().do_read_some(fd, iov);
}

future<size_t>
reactor_backend_epoll::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    return engine().do_write_some(fd, buffer, len);
}

future<size_t>
reactor_backend_epoll::write_some(pollable_fd_state& fd, net::packet& p) {
    return engine().do_write_some(fd, p);
}

void
reactor_backend_epoll::request_preemption() {
    _r->_preemption_monitor.head.store(1, std::memory_order_relaxed);
}

void reactor_backend_epoll::start_handling_signal() {
    // The epoll backend uses signals for the high resolution timer. That is used for thread_scheduling_group, so we
    // request preemption so when we receive a signal.
    request_preemption();
}

pollable_fd_state_ptr
reactor_backend_epoll::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return pollable_fd_state_ptr(new epoll_pollable_fd_state(std::move(fd), std::move(speculate)));
}

void reactor_backend_epoll::reset_preemption_monitor() {
    _r->_preemption_monitor.head.store(0, std::memory_order_relaxed);
}

#ifdef HAVE_OSV
reactor_backend_osv::reactor_backend_osv() {
}

bool
reactor_backend_osv::reap_kernel_completions() {
    _poller.process();
    // osv::poller::process runs pollable's callbacks, but does not currently
    // have a timer expiration callback - instead if gives us an expired()
    // function we need to check:
    if (_poller.expired()) {
        _timer_promise.set_value();
        _timer_promise = promise<>();
    }
    return true;
}

reactor_backend_osv::kernel_submit_work() {
}

void
reactor_backend_osv::wait_and_process_events(const sigset_t* sigset) {
    return process_events_nowait();
}

future<>
reactor_backend_osv::readable(pollable_fd_state& fd) {
    std::cerr << "reactor_backend_osv does not support file descriptors - readable() shouldn't have been called!\n";
    abort();
}

future<>
reactor_backend_osv::writeable(pollable_fd_state& fd) {
    std::cerr << "reactor_backend_osv does not support file descriptors - writeable() shouldn't have been called!\n";
    abort();
}

void
reactor_backend_osv::forget(pollable_fd_state& fd) noexcept {
    std::cerr << "reactor_backend_osv does not support file descriptors - forget() shouldn't have been called!\n";
    abort();
}

future<std::tuple<pollable_fd, socket_address>>
reactor_backend_osv::accept(pollable_fd_state& listenfd) {
    return engine().do_accept(listenfd);
}

future<> reactor_backend_osv::connect(pollable_fd_state& fd, socket_address& sa) {
    return engine().do_connect(fd, sa);
}

void reactor_backend_osv::shutdown(pollable_fd_state& fd, int how) {
    fd.fd.shutdown(how);
}

future<size_t>
reactor_backend_osv::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    return engine().do_read_some(fd, buffer, len);
}

future<size_t>
reactor_backend_osv::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    return engine().do_read_some(fd, iov);
}

future<size_t>
reactor_backend_osv::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    return engine().do_write_some(fd, buffer, len);
}

future<size_t>
reactor_backend_osv::write_some(pollable_fd_state& fd, net::packet& p) {
    return engine().do_write_some(fd, p);
}

void
reactor_backend_osv::enable_timer(steady_clock_type::time_point when) {
    _poller.set_timer(when);
}

pollable_fd_state_ptr
reactor_backend_osv::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    std::cerr << "reactor_backend_osv does not support file descriptors - make_pollable_fd_state() shouldn't have been called!\n";
    abort();
}
#endif

static bool detect_aio_poll() {
    auto fd = file_desc::eventfd(0, 0);
    aio_context_t ioc{};
    setup_aio_context(1, &ioc);
    auto cleanup = defer([&] { io_destroy(ioc); });
    linux_abi::iocb iocb = internal::make_poll_iocb(fd.get(), POLLIN|POLLOUT);
    linux_abi::iocb* a[1] = { &iocb };
    auto r = io_submit(ioc, 1, a);
    if (r != 1) {
        return false;
    }
    uint64_t one = 1;
    fd.write(&one, 8);
    io_event ev[1];
    // We set force_syscall = true (the last parameter) to ensure
    // the system call exists and is usable. If IOCB_CMD_POLL exists then
    // io_pgetevents() will also exist, but some versions of docker
    // have a syscall whitelist that does not include io_pgetevents(),
    // which causes it to fail with -EPERM. See
    // https://github.com/moby/moby/issues/38894.
    r = io_pgetevents(ioc, 1, 1, ev, nullptr, nullptr, true);
    return r == 1;
}

bool reactor_backend_selector::has_enough_aio_nr() {
    auto aio_max_nr = read_first_line_as<unsigned>("/proc/sys/fs/aio-max-nr");
    auto aio_nr = read_first_line_as<unsigned>("/proc/sys/fs/aio-nr");
    /* reactor_backend_selector::available() will be execute in early stage,
     * it's before io_setup() issued, and not per-cpu basis.
     * So this method calculates:
     *  Available AIO on the system - (request AIO per-cpu * ncpus)
     */
    if (aio_max_nr - aio_nr < reactor::max_aio * smp::count) {
        return false;
    }
    return true;
}

using namespace seastar::internal;

static bool detect_uring();

std::unique_ptr<reactor_backend> reactor_backend_selector::create(reactor* r) {
    if (_name == "uring") {
        return std::make_unique<reactor_backend_uring>(r);
    } else if (_name == "linux-aio") {
        return std::make_unique<reactor_backend_aio>(r);
    } else if (_name == "epoll") {
        return std::make_unique<reactor_backend_epoll>(r);
    }
    throw std::logic_error("bad reactor backend");
}

reactor_backend_selector reactor_backend_selector::default_backend() {
    return available()[0];
}

std::vector<reactor_backend_selector> reactor_backend_selector::available() {
    std::vector<reactor_backend_selector> ret;
    if (detect_uring()) {
        ret.push_back(reactor_backend_selector("uring"));
    }

    if (detect_aio_poll() && has_enough_aio_nr()) {
        ret.push_back(reactor_backend_selector("linux-aio"));
    }
    ret.push_back(reactor_backend_selector("epoll"));
    return ret;
}
#ifndef SEASTAR_HAVE_URING
static bool detect_uring() {
    return false;
}
#else
static bool detect_uring() {
    try {
        std::vector<int> opcodes_of_interest = {
            IORING_OP_READV,
            IORING_OP_READ,
            IORING_OP_RECV,
            IORING_OP_RECVMSG,
            IORING_OP_WRITEV,
            IORING_OP_WRITE,
            IORING_OP_SEND,
            IORING_OP_SENDMSG,
            IORING_OP_CONNECT,
            IORING_OP_ACCEPT,
            IORING_OP_ASYNC_CANCEL,
            IORING_OP_FSYNC,
            IORING_OP_POLL_ADD,
            IORING_OP_POLL_REMOVE,
        };

        uring_context ctx = uring_context::make_irq_ring([] (auto* dummy) {});
        return ctx.ring_features_supported(IORING_FEAT_NODROP | IORING_FEAT_FAST_POLL) &&
               ctx.ring_opcodes_supported(std::move(opcodes_of_interest));
     } catch (std::system_error& e) {
         return false;
     }
}

class uring_pollable_fd_state;

// We will use this class to do reference counting for the pollable_fd_state.  We
//
// Reference counting is needed because deletion can only happen after everything
// that is submitted in relationship to this uring_pollable_fd_state completes.
// Events do not necessarily appear in the completion queue in the order they were
// submitted, but in the order they were completed. Completion order is then,
// unpredictable and deletion could happen first.
class uring_pollable_fd_refcnt {
    int _refcnt = 1;
    void destroy_pollable_fd();
public:
    uring_pollable_fd_refcnt() {}
    uring_pollable_fd_refcnt(const uring_pollable_fd_refcnt&) = delete;
    uring_pollable_fd_refcnt& operator=(const uring_pollable_fd_refcnt&) = delete;
    uring_pollable_fd_refcnt(uring_pollable_fd_refcnt&&) = default;
    uring_pollable_fd_refcnt& operator=(uring_pollable_fd_refcnt&&) = default;

    void acquire() {
        _refcnt++;
    }
    void release() {
        if (!--_refcnt) {
            destroy_pollable_fd();
        }
    }
};

class uring_network_completion : public kernel_completion {
private:
    promise<size_t> _pr;
    ::msghdr _msghdr = {};
    int _speculate_event = 0;
    size_t _expected_size = 0;

    friend class uring_pollable_fd_state;

    static void maybe_speculate(uring_pollable_fd_state* fd, size_t size, size_t expected, int event);
public:
    uring_network_completion() {}
    uring_network_completion(int event, size_t expected)
        : _speculate_event(event)
        , _expected_size(expected)
    {}

    static future<size_t> complete_with(uring_pollable_fd_state* fd, ssize_t res, size_t expected, int event);
    virtual void complete_with(ssize_t res) override;

    future<size_t> get_future() {
        return _pr.get_future();
    }

    ::msghdr& msg() {
        return _msghdr;
    }
};

class uring_connect_completion : public kernel_completion {
private:
    promise<> _pr;
    friend class uring_pollable_fd_state;
    // sa is passed as a reference to engine().connect(), but historically
    // there were no strict rules about its lifetime. The callers won't deal
    // with it, which is natural since the pre-uring implementation of connect
    // issued a synchronous call.
    // We will just keep a copy instead of burdening all callers
    socket_address _sa;
public:
    uring_connect_completion() {}
    uring_connect_completion(const socket_address& sa) : _sa(sa) {}

    socket_address& sockaddr() {
        return _sa;
    }

    virtual void complete_with(ssize_t res) override;
    future<> get_future() {
        return _pr.get_future();
    }
};

class uring_accept_completion : public kernel_completion {
private:
    promise<std::tuple<pollable_fd, socket_address>> _pr;
    socket_address _sa = {};

    friend class uring_pollable_fd_state;
public:
    sockaddr* sa() {
        return &_sa.as_posix_sockaddr();
    }

    socklen_t* sl() {
        return &_sa.addr_length;
    }

    static future<std::tuple<pollable_fd, socket_address>>
    complete_with(uring_pollable_fd_state *fd, file_desc, socket_address sa);
    virtual void complete_with(ssize_t res) override;

    future<std::tuple<pollable_fd, socket_address>> get_future() {
        return _pr.get_future();
    }
};

class uring_poll_remove_completion : public kernel_completion {
};

class uring_pollin_remove_completion final : public uring_poll_remove_completion {
public:
    virtual void complete_with(ssize_t res) override;
};

class uring_pollout_remove_completion final : public uring_poll_remove_completion {
public:
    virtual void complete_with(ssize_t res) override;
};

class uring_poll_add_completion : public kernel_completion {
protected:
    promise<> _pr;
public:
    future<> get_future() {
        return _pr.get_future();
    }
};

class uring_pollin_add_completion final : public uring_poll_add_completion {
public:
    virtual void complete_with(ssize_t res) override;
};

class uring_pollout_add_completion final : public uring_poll_add_completion {
public:
    virtual void complete_with(ssize_t res) override;
};

class uring_pollable_fd_state : public pollable_fd_state {
    uring_pollin_add_completion _pollin_add;
    uring_pollout_add_completion _pollout_add;
    uring_network_completion _network_read;
    uring_network_completion _network_write;

    uring_connect_completion _connect;
    uring_accept_completion _accept;

    uring_pollin_remove_completion _pollin_remove;
    uring_pollout_remove_completion _pollout_remove;

    uring_pollable_fd_refcnt _refcnt;

    friend uring_accept_completion;
    friend uring_network_completion;
    friend uring_connect_completion;
    friend uring_pollin_remove_completion;
    friend uring_pollout_remove_completion;
    friend uring_pollin_add_completion;
    friend uring_pollout_add_completion;
    friend uring_pollable_fd_refcnt;

    // The method below will be used when we initiate a new operation.
    // We need to refresh the existing completion object, because it is a
    // new operation, and bump the reference count.
    template <typename T, typename... Args>
    T* get_completion(T* ptr, Args... args) {
        T* desc = ptr;
        *desc = T(std::forward<Args>(args)...);
        _refcnt.acquire();
        return desc;
    }

    unsigned _fd_flags;
public:
    using pollable_fd_state::maybe_no_more_recv;
    using pollable_fd_state::maybe_no_more_send;

    void forget() {
        _refcnt.release();
    }

    uring_connect_completion* get_connect_desc(const socket_address& sa) {
        return get_completion(&_connect, sa);
    }

    uring_accept_completion* get_accept_desc() {
        return get_completion(&_accept);
    }

    uring_poll_remove_completion* get_poll_remove_desc(int events) {
        if (events & POLLIN) {
            return get_completion(&_pollin_remove);
        } else {
            return get_completion(&_pollout_remove);
        }
    }

    uring_poll_add_completion* get_poll_add_desc(int events) {
        if (events & POLLIN) {
            return get_completion(&_pollin_add);
        } else {
            return get_completion(&_pollout_add);
        }
    }

    uring_network_completion* get_network_desc(int events, size_t len) {
        if (events & POLLIN) {
            return get_completion(&_network_read, events, len);
        } else {
            return get_completion(&_network_write, events, len);
        }
    }

    // The methods below will be used when we just want to peek at the address of a completion,
    // without initiating a new operation. For example, if we are cancelling an existing operation.
    uring_connect_completion* connect_desc_addr() {
        return &_connect;
    }

    uring_accept_completion* accept_desc_addr() {
        return &_accept;
    }

    uring_poll_remove_completion* poll_remove_desc_addr(int events) {
        if (events & POLLIN) {
            return &_pollin_remove;
        } else {
            return &_pollout_remove;
        }
    }

    uring_poll_add_completion* poll_add_addr(int events) {
        if (events & POLLIN) {
            return &_pollin_add;
        } else {
            return &_pollout_add;
        }
    }

    uring_network_completion* network_desc_addr(int events) {
        return events & POLLIN ? &_network_read : &_network_write;
    }

    explicit uring_pollable_fd_state(file_desc fdesc, speculation speculate)
        : pollable_fd_state(std::move(fdesc), std::move(speculate))
    {
        // FIXME: we use recvmsg for reading udp sockets, and both sendmsg and sendto for writing.
        // Unfortunately when using recvmsg we set the cmsg fields in the msghdr structure, and
        // io_uring doesn't like that - see Linux commit d69e07793f891524c6bbf1e75b9ae69db4450953.
        // Because we mark sockets as blocking, we need to do that only if we are not dealing with
        // udp sockets, otherwise we will block in the aio implementation.
        //
        // I am attaching this as a FIXME because I have hope that at some point Linux will change
        // its behavior here - or we will change seastar not to use cmsg. At that point we can
        // remove this and implement all the other UDP network functions within uring.
        struct stat statbuf;
        fstat(fd.get(), &statbuf);
        int type = 0;

        _fd_flags = ::fcntl(fd.get(), F_GETFL);

        if (S_ISSOCK(statbuf.st_mode)) {
            type = fd.getsockopt<int>(SOL_SOCKET, SO_TYPE);
        }
        if (S_ISSOCK(statbuf.st_mode) && (type != SOCK_DGRAM)) {
            _fd_flags &= ~O_NONBLOCK;
            // Uring will work with unblocking operations
            auto r = ::fcntl(fd.get(), F_SETFL, _fd_flags);
            assert(r == 0);
        }
    }

    void make_blocking() {
        if (_fd_flags & O_NONBLOCK) {
            _fd_flags &= ~O_NONBLOCK;
            auto r = ::fcntl(fd.get(), F_SETFL, _fd_flags);
            assert(r == 0);
        }
    }

    void make_nonblocking() {
        if (!(_fd_flags & O_NONBLOCK)) {
            _fd_flags |= O_NONBLOCK;
            auto r = ::fcntl(fd.get(), F_SETFL, _fd_flags);
            assert(r == 0);
        }
    }

    uring_pollable_fd_state() = delete;

    uring_pollable_fd_state(uring_pollable_fd_state&&) = delete;
    uring_pollable_fd_state(const uring_pollable_fd_state&) = delete;

    uring_pollable_fd_state& operator=(uring_pollable_fd_state&&) = delete;
    uring_pollable_fd_state& operator=(const uring_pollable_fd_state&) = delete;
};

void
uring_pollable_fd_refcnt::destroy_pollable_fd() {
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_refcnt);
    delete pfd;
}

void uring_network_completion::maybe_speculate(uring_pollable_fd_state* pfd, size_t size, size_t expected_size, int speculate_event) {
    if (size == expected_size) {
        pfd->speculate_epoll(speculate_event);
    }
}

void uring_accept_completion::complete_with(ssize_t res) {
    uring_pollable_fd_state *listen_pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_accept);
    try {
        throw_kernel_error(res);
        auto fd = file_desc::from_fd(res);
        complete_with(listen_pfd, std::move(fd), std::move(_sa)).forward_to(std::move(_pr));
    } catch (...) {
        _pr.set_exception(std::current_exception());
    }

    listen_pfd->forget();
}

void uring_connect_completion::complete_with(ssize_t res) {
    try {
        throw_kernel_error(res);
        _pr.set_value();
    } catch (...) {
        _pr.set_exception(std::current_exception());
    }
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_connect);
    pfd->forget();
}

void uring_pollin_remove_completion::complete_with(ssize_t res) {
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_pollin_remove);
    pfd->forget();
}

void uring_pollout_remove_completion::complete_with(ssize_t res) {
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_pollout_remove);
    pfd->forget();
}

void uring_pollin_add_completion::complete_with(ssize_t res) {
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_pollin_add);
    _pr.set_value();
    pfd->forget();
}

void uring_pollout_add_completion::complete_with(ssize_t res) {
    auto pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_pollout_add);
    _pr.set_value();
    pfd->forget();
}

void uring_network_completion::complete_with(ssize_t res) {
    // We don't expect any errors, since our reads are blocking
    // and EAGAIN is not a thing. Anything that returns a negative
    // value means something went wrong
    uring_pollable_fd_state *pfd;

    if (_speculate_event == POLLIN) {
        pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_network_read);
    } else {
        pfd = boost::intrusive::get_parent_from_member(this, &uring_pollable_fd_state::_network_write);
    }
    complete_with(pfd, res, _expected_size, _speculate_event).forward_to(std::move(_pr));
    pfd->forget();
}

future<std::tuple<pollable_fd, socket_address>>
uring_accept_completion::complete_with(uring_pollable_fd_state* listenfd, file_desc fd, socket_address sa) {
    listenfd->speculate_epoll(POLLIN);
    pollable_fd pfd(std::move(fd), pollable_fd::speculation(POLLOUT));
    return make_ready_future<std::tuple<pollable_fd, socket_address>>(std::make_tuple(std::move(pfd), std::move(sa)));
}

future<size_t>
uring_network_completion::complete_with(uring_pollable_fd_state* pfd, ssize_t res, size_t expected, int event) {
    try {
        throw_kernel_error(res);
        size_t size = size_t(res);
        maybe_speculate(pfd, size, expected, event);
        return make_ready_future<size_t>(size);
    } catch (...) {
        return make_exception_future<size_t>(std::current_exception());
    }
}

bool reactor_backend_uring::kernel_events_can_sleep() const {
    return true;
}

bool reactor_backend_uring::kernel_submit_work() {
    // Move our SQE list to the kernel. We want this here,
    // and not inside flush(), because in the future we may choose
    // which ring to push to (poll vs irq).
    while (!_r->_pending_io.empty()) {
        if (!_irq_ctx.maybe_submit_request(_r->_pending_io)) {
            break;
        }
        _r->_pending_io.pop_front();
    }
    _irq_ctx.flush();
    _timer_aio_context.reset_preemption_monitor();
    // The work that we do here never generate new tasks. Only when we
    // reap completions. We will return true (meaning don't go to sleep)
    // if there are no more pending tasks to send to the kernel. If we managed
    // to send everything in this round, then we can safely sleep.
    return !_r->_pending_io.empty();
}

bool reactor_backend_uring::reap_kernel_completions() {
    bool did_work = _irq_ctx.poll();
    did_work |= _timer_aio_context.service_preempting_io();
    return did_work;
}

void reactor_backend_uring::process_one_cqe(io_uring_cqe* cqe) {
    kernel_completion* desc = reinterpret_cast<kernel_completion*>(cqe->user_data);
    desc->complete_with(size_t(cqe->res));
}

bool reactor_backend_uring::register_uring_listener(uring_listener_completion* desc) {
    if (!desc->is_registered()) {
        auto req = io_request::make_poll_add(desc->fd().get(), POLLIN);
        req.attach_kernel_completion(desc);
        _irq_ctx.push_prio_sqe(std::move(req));
        desc->set_registered(true);
        return true;
    }
    return false;
}

static constexpr unsigned cq_ring_size = 256;

reactor_backend_uring::reactor_backend_uring(reactor* r)
    : _r(r)
    , _hrtimer_timerfd(file_desc::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC|TFD_NONBLOCK))
    , _hrtimer_completion(_r, _hrtimer_timerfd)
    , _smp_wakeup_completion(_r, _r->_notify_eventfd)
    , _irq_ctx(uring_context::make_irq_ring([this] (io_uring_cqe* cqe) { return process_one_cqe(cqe); }, cq_ring_size))
    , _timer_aio_context(_r, _r->_task_quota_timer, _hrtimer_timerfd)
{
    register_uring_listener(&_smp_wakeup_completion);
    _irq_ctx.force_flush();
    // Protect against spurious wakeups - if we get notified that the timer has
    // expired when it really hasn't, we don't want to block in read(tfd, ...).
    auto tfd = _r->_task_quota_timer.get();
    ::fcntl(tfd, F_SETFL, ::fcntl(tfd, F_GETFL) | O_NONBLOCK);

    sigset_t mask = make_sigset_mask(hrtimer_signal());
    auto e = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    assert(e == 0);

    namespace sm = seastar::metrics;

    auto ring_group = sm::label("ring");
    auto main_irq_ring = ring_group("main-irq");

    _metrics.add_group("reactor_backend_uring", {
        sm::make_derive("total_flushes", [this] {
                return _irq_ctx.total_flushes();
            }, sm::description("Total number of times the ring was flushed"), {main_irq_ring}),
        sm::make_derive("total_forced_flushes", [this] {
                return _irq_ctx.total_forced_flushes();
            }, sm::description("Total number of times the ring was force-flushed"), {main_irq_ring}),
    });
}

void reactor_backend_uring::signal_received(int signo, siginfo_t* siginfo, void* ignore) {
    engine()._signals.action(signo, siginfo, ignore);
}

void reactor_backend_uring::arm_highres_timer(const ::itimerspec& its) {
    _hrtimer_timerfd.timerfd_settime(TFD_TIMER_ABSTIME, its);
}

void reactor_backend_uring::wait_and_process_events(const sigset_t* active_sigmask) {
    auto exit = defer([this] {
        bool need_flush = false;
        // If true, means at some point we processed a wakeup. Re-register
        need_flush |=  register_uring_listener(&_smp_wakeup_completion);
        if (need_flush) {
            _irq_ctx.force_flush();
        }
        _timer_aio_context.reset_preemption_monitor();
        // Before we wake up, consume any events that may have been generated
        // so we have plenty to do in the next task quota.
        _irq_ctx.poll();
    });

    bool did_work = _timer_aio_context.service_preempting_io();
    if (did_work) {
        return;
    }

    // Before we go to sleep we must register the hrtimer into the uring.
    // If we don't do that, we won't wake up if it fires. During normal
    // operations we don't need to do anything: the aio timer will deal with
    // it, and even if it is registered in both places it is not a big deal.
    register_uring_listener(&_hrtimer_completion);
    // If we don't need flush here that means that the hrtimer completion
    // was already in the ring and something is very seriously wrong.
    _irq_ctx.force_flush();
    if (!_smp_wakeup_completion.is_registered()) {
        return;
    }
    _irq_ctx.sync_wait_forever(active_sigmask);
}

future<> reactor_backend_uring::poll(pollable_fd_state& fd, int events) {
    try {
        if (fd.try_speculate_poll(events)) {
            return make_ready_future<>();
        }

        fd.events_rw = events == (POLLIN|POLLOUT);
        auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
        auto* desc = pfd->get_poll_add_desc(events);
        auto req = io_request::make_poll_add(fd.fd.get(), events);
        _r->submit_io(desc, std::move(req));
        return desc->get_future();
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<> reactor_backend_uring::readable(pollable_fd_state& fd) {
    return poll(fd, POLLIN);
}

future<> reactor_backend_uring::writeable(pollable_fd_state& fd) {
    return poll(fd, POLLOUT);
}

future<> reactor_backend_uring::readable_or_writeable(pollable_fd_state& fd) {
    return poll(fd, POLLIN|POLLOUT);
}

// FIXME: submit_io can allocate. Do something different
//
void reactor_backend_uring::forget(pollable_fd_state& fd) noexcept {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    // This pfd was never successfully opened
    if (fd.fd.get() != -1) {
        auto ops = { POLLIN, POLLOUT };
        for (auto& op : ops) {
            auto* op_desc = pfd->poll_add_addr(op);
            auto req = io_request::make_poll_remove(pfd->fd.get(), op_desc);
            auto* desc = pfd->get_poll_remove_desc(op);
            _r->submit_io(desc, std::move(req));
        }
    }
    // POLL_REMOVE doesn't depend on the file being open. It only uses the
    // information in the original's POLL_ADD sqe to find it and remove it from
    // the list. So we can rely on io_uring to do it in the pollers later, and we
    // can close the file now.
    //
    // It's important that we do: this is called from destructors, and if we don't
    // close the file now, we can end up trying to open a file that is not closed
    // yet, but the application believe it is. The file is closed from forget(),
    // which also decrements the reference count
    pfd->forget();
}

future<std::tuple<pollable_fd, socket_address>>
reactor_backend_uring::accept(pollable_fd_state& listenfd) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&listenfd);
    try {
        // POLLIN for accept, POLLOUT for shutdown
        if (listenfd.try_speculate_poll(POLLIN|POLLOUT)) {
            pfd->maybe_no_more_recv();
            socket_address sa;

            pfd->make_nonblocking();
            // SOCK_NONBLOCK is absent:
            // blocking reads or writes, so we don't have to poll. io_uring
            // magically makes it asynchronous
            auto maybe_fd = listenfd.fd.try_accept(sa, SOCK_CLOEXEC);
            if (maybe_fd) {
                return uring_accept_completion::complete_with(pfd, std::move(*maybe_fd), std::move(sa));
            }
        }

        // This is the most frustrating part of working with uring:
        // accept/connect don't take flags. If we dispatch the request
        // to the ring, it has to be blocking or it will return -EAGAIN
        // and we are no better than if we poll. But we can't keep the
        // socket always blocking because of speculation. So we play
        // those silly fcntl games.
        pfd->make_blocking();

        auto* desc = pfd->get_accept_desc();
        auto req = internal::io_request::make_accept(listenfd.fd.get(), desc->sa(), desc->sl(), SOCK_CLOEXEC);
        _r->submit_io(desc, std::move(req));
        return desc->get_future();
    } catch (...) {
        return make_exception_future<std::tuple<pollable_fd, socket_address>>(std::current_exception());
    }
}

future<> reactor_backend_uring::connect(pollable_fd_state& fd, socket_address& sa) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    auto* desc = pfd->get_connect_desc(sa);
    auto req = internal::io_request::make_connect(fd.fd.get(), &desc->sockaddr().as_posix_sockaddr(), sa.length());
    _r->submit_io(desc, std::move(req));
    return desc->get_future();
}

void reactor_backend_uring::shutdown(pollable_fd_state& fd, int how) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    auto* connect_desc = pfd->connect_desc_addr();
    auto connect_req = internal::io_request::make_cancel(fd.fd.get(), connect_desc);
    _r->submit_io(&_empty_completion, std::move(connect_req));

    auto* accept_desc = pfd->accept_desc_addr();
    auto accept_req = internal::io_request::make_cancel(fd.fd.get(), accept_desc);
    _r->submit_io(&_empty_completion, std::move(accept_req));

    fd.fd.shutdown(how);
}

future<size_t>
reactor_backend_uring::read_some(pollable_fd_state& fd, void* buffer, size_t len) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    try {
        if (fd.try_speculate_poll(EPOLLIN)) {
            auto r = fd.fd.recv(buffer, len, MSG_DONTWAIT);
            if (r) {
                return uring_network_completion::complete_with(pfd, *r, len, POLLIN);
            }
        }

#if 0
        auto* desc = pfd->get_poll_add_desc(POLLIN);
        auto req = io_request::make_poll_add(fd.fd.get(), POLLIN);
        req.set_link(true);
        _r->submit_io(desc, std::move(req));
#endif

        // speculation failed, try blocking read
        auto* desc = pfd->get_network_desc(POLLIN, len);
        auto req = io_request::make_recv(fd.fd.get(), buffer, len, 0);
        _r->submit_io(desc, std::move(req));

        auto fut = desc->get_future();
        return fut;
    } catch (...) {
        return make_exception_future<size_t>(std::current_exception());
    }
}

future<size_t>
reactor_backend_uring::read_some(pollable_fd_state& fd, const std::vector<iovec>& iov) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    try {
        ::msghdr mh;
        mh.msg_iov = const_cast<iovec*>(&iov[0]);
        mh.msg_iovlen = iov.size();

        if (fd.try_speculate_poll(EPOLLIN)) {
            auto r = fd.fd.recvmsg(&mh, MSG_DONTWAIT);
            if (r) {
                return uring_network_completion::complete_with(pfd, *r, iovec_len(iov), POLLIN);
            }
        }

        auto* desc = pfd->get_network_desc(POLLIN, iovec_len(iov));
        mh = desc->msg();
        mh.msg_iov = const_cast<iovec*>(&iov[0]);
        mh.msg_iovlen = iov.size();
        auto fut = desc->get_future();
        auto req = io_request::make_recvmsg(fd.fd.get(), &mh, 0);
        _r->submit_io(desc, std::move(req));
        return fut;
    } catch (...) {
        return make_exception_future<size_t>(std::current_exception());
    }
}

future<size_t>
reactor_backend_uring::write_some(pollable_fd_state& fd, const void* buffer, size_t len) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    try {
        if (fd.try_speculate_poll(EPOLLOUT)) {
            auto r = fd.fd.send(buffer, len, MSG_DONTWAIT | MSG_NOSIGNAL);
            if (r) {
                return uring_network_completion::complete_with(pfd, *r, len, POLLOUT);
            }
        }

        // speculation failed, try blocking read from the read
        auto* desc = pfd->get_network_desc(POLLOUT, len);
        auto fut = desc->get_future();
        auto req = io_request::make_send(fd.fd.get(), buffer, len, MSG_NOSIGNAL);
        _r->submit_io(desc, std::move(req));
        return fut;
    } catch (...) {
        return make_exception_future<size_t>(std::current_exception());
    }
}

future<size_t>
reactor_backend_uring::write_some(pollable_fd_state& fd, net::packet& p) {
    auto* pfd = static_cast<uring_pollable_fd_state*>(&fd);
    try {
        ::msghdr mh = {};
        mh.msg_iov = reinterpret_cast<iovec*>(p.fragment_array());
        mh.msg_iovlen = std::min<size_t>(p.nr_frags(), IOV_MAX);

        if (fd.try_speculate_poll(EPOLLOUT)) {
            auto r = fd.fd.sendmsg(&mh, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (r) {
                return uring_network_completion::complete_with(pfd, *r, p.len(), POLLOUT);
            }
        }

        auto* desc = pfd->get_network_desc(POLLOUT, p.len());
        auto fut = desc->get_future();
        mh = desc->msg();
        mh.msg_iov = reinterpret_cast<iovec*>(p.fragment_array());
        mh.msg_iovlen = std::min<size_t>(p.nr_frags(), IOV_MAX);
        // speculation failed, try blocking operation
        auto req = io_request::make_sendmsg(fd.fd.get(), &mh, MSG_NOSIGNAL);
        _r->submit_io(desc, std::move(req));
        return fut;
    } catch (...) {
        return make_exception_future<size_t>(std::current_exception());
    }
}

void reactor_backend_uring::start_tick() {
    _timer_aio_context.start_tick();
}

void reactor_backend_uring::stop_tick() {
    _timer_aio_context.stop_tick();
    g_need_preempt = &_r->_preemption_monitor;
}

void reactor_backend_uring::reset_preemption_monitor() {
    _timer_aio_context.reset_preemption_monitor();
}

void reactor_backend_uring::request_preemption() {
    _timer_aio_context.request_preemption();
}

void reactor_backend_uring::start_handling_signal() {
    // The uring backend only uses SIGHUP/SIGTERM/SIGINT (same as aio). We don't need to handle them right away and our
    // implementation of request_preemption is not signal safe, so do nothing.
}

pollable_fd_state_ptr
reactor_backend_uring::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return std::unique_ptr<pollable_fd_state, pollable_fd_state_deleter>(new uring_pollable_fd_state(std::move(fd), std::move(speculate)));
}
#endif

}

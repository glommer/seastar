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
#include <chrono>
#include <sys/poll.h>
#include <sys/syscall.h>

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
        iocb = make_writev_iocb(req.fd(), req.pos(), reinterpret_cast<const iovec*>(req.address()), req.size());
        break;
    case io_request::operation::read:
        iocb = make_read_iocb(req.fd(), req.pos(), req.address(), req.size());
        break;
    case io_request::operation::readv:
        iocb = make_readv_iocb(req.fd(), req.pos(), reinterpret_cast<const iovec*>(req.address()), req.size());
        break;
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
            // we will only remove it from _pending_aio and try again.
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
    size_t pending = _r->_pending_aio.size();
    size_t to_submit = 0;
    bool did_work = false;

    _submission_queue.resize(0);
    while ((pending > to_submit) && _iocb_pool.has_capacity()) {
        auto& req = _r->_pending_aio[to_submit++];
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
    _r->_pending_aio.erase(_r->_pending_aio.begin(), _r->_pending_aio.begin() + submitted);

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

reactor_backend_aio::context::context(size_t nr) : iocbs(new iocb*[nr]) {
    setup_aio_context(nr, &io_context);
}

reactor_backend_aio::context::~context() {
    io_destroy(io_context);
}

void reactor_backend_aio::context::replenish(linux_abi::iocb* iocb, bool& flag) {
    if (!flag) {
        flag = true;
        queue(iocb);
    }
}

void reactor_backend_aio::context::queue(linux_abi::iocb* iocb) {
    *last++ = iocb;
}

size_t reactor_backend_aio::context::flush() {
    if (last != iocbs.get()) {
        auto nr = last - iocbs.get();
        last = iocbs.get();
        io_submit(io_context, nr, iocbs.get());
        return nr;
    }
    return 0;
}

file_desc reactor_backend_aio::make_timerfd() {
    return file_desc::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC|TFD_NONBLOCK);
}

void reactor_backend_aio::process_task_quota_timer() {
    uint64_t v;
    (void)_r->_task_quota_timer.read(&v, 8);
}

void reactor_backend_aio::process_timerfd() {
    uint64_t expirations = 0;
    _steady_clock_timer.read(&expirations, 8);
    if (expirations) {
        _r->service_highres_timer();
    }
}

void reactor_backend_aio::process_smp_wakeup() {
    uint64_t ignore = 0;
    _r->_notify_eventfd.read(&ignore, 8);
}

bool reactor_backend_aio::service_preempting_io() {
    linux_abi::io_event a[2];
    auto r = io_getevents(_preempting_io.io_context, 0, 2, a, 0);
    assert(r != -1);
    bool did_work = false;
    for (unsigned i = 0; i != unsigned(r); ++i) {
        if (get_iocb(a[i]) == &_task_quota_timer_iocb) {
            _task_quota_timer_in_preempting_io = false;
            process_task_quota_timer();
        } else if (get_iocb(a[i]) == &_timerfd_iocb) {
            _timerfd_in_preempting_io = false;
            process_timerfd();
            did_work = true;
        }
    }
    return did_work;
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
            auto iocb = get_iocb(event);
            if (iocb == &_timerfd_iocb) {
                _timerfd_in_polling_io = false;
                process_timerfd();
                continue;
            } else if (iocb == &_smp_wakeup_iocb) {
                _smp_wakeup_in_polling_io = false;
                process_smp_wakeup();
                continue;
            }
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
    , _storage_context(_r) {
    _task_quota_timer_iocb = make_poll_iocb(_r->_task_quota_timer.get(), POLLIN);
    _timerfd_iocb = make_poll_iocb(_steady_clock_timer.get(), POLLIN);
    _smp_wakeup_iocb = make_poll_iocb(_r->_notify_eventfd.get(), POLLIN);
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
    bool did_work = _polling_io.flush();
    did_work |= _storage_context.submit_work();
    return did_work;
}

bool reactor_backend_aio::kernel_events_can_sleep() const {
    return _storage_context.can_sleep();
}

void reactor_backend_aio::wait_and_process_events(const sigset_t* active_sigmask) {
    int timeout = -1;
    bool did_work = service_preempting_io();
    if (did_work) {
        timeout = 0;
    }
    _polling_io.replenish(&_timerfd_iocb, _timerfd_in_polling_io);
    _polling_io.replenish(&_smp_wakeup_iocb, _smp_wakeup_in_polling_io);
    _polling_io.flush();
    await_events(timeout, active_sigmask);
    service_preempting_io(); // clear task quota timer
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
        if (events & fd.events_known) {
            fd.events_known &= ~events;
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

void reactor_backend_aio::forget(pollable_fd_state& fd) {
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
    // Preempt whenever an event (timer tick or signal) is available on the
    // _preempting_io ring
    g_need_preempt = reinterpret_cast<const preemption_monitor*>(_preempting_io.io_context + 8);
    // reactor::request_preemption() will write to reactor::_preemption_monitor, which is now ignored
}

void reactor_backend_aio::stop_tick() {
    g_need_preempt = &_r->_preemption_monitor;
}

void reactor_backend_aio::arm_highres_timer(const ::itimerspec& its) {
    _steady_clock_timer.timerfd_settime(TFD_TIMER_ABSTIME, its);
}

void reactor_backend_aio::reset_preemption_monitor() {
    service_preempting_io();
    _preempting_io.replenish(&_timerfd_iocb, _timerfd_in_preempting_io);
    _preempting_io.replenish(&_task_quota_timer_iocb, _task_quota_timer_in_preempting_io);
    _preempting_io.flush();
}

void reactor_backend_aio::request_preemption() {
    ::itimerspec expired = {};
    expired.it_value.tv_nsec = 1;
    arm_highres_timer(expired); // will trigger immediately, triggering the preemption monitor

    // This might have been called from poll_once. If that is the case, we cannot assume that timerfd is being
    // monitored.
    _preempting_io.replenish(&_timerfd_iocb, _timerfd_in_preempting_io);
    _preempting_io.flush();

    // The kernel is not obliged to deliver the completion immediately, so wait for it
    while (!need_preempt()) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }
}

void reactor_backend_aio::start_handling_signal() {
    // The aio backend only uses SIGHUP/SIGTERM/SIGINT. We don't need to handle them right away and our
    // implementation of request_preemption is not signal safe, so do nothing.
}

pollable_fd_state*
reactor_backend_aio::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return new aio_pollable_fd_state(std::move(fd), std::move(speculate));
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
    if (pfd.events_known & event) {
        pfd.events_known &= ~event;
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

void reactor_backend_epoll::forget(pollable_fd_state& fd) {
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

pollable_fd_state*
reactor_backend_epoll::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return new epoll_pollable_fd_state(std::move(fd), std::move(speculate));
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
reactor_backend_osv::forget(pollable_fd_state& fd) {
    delete &fd;
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

pollable_fd_state*
reactor_backend_osv::make_pollable_fd_state(file_desc fd, pollable_fd::speculation speculate) {
    return new pollable_fd_state(std::move(fd), std::move(speculate));
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

std::unique_ptr<reactor_backend> reactor_backend_selector::create(reactor* r) {
    if (_name == "linux-aio") {
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
    if (detect_aio_poll() && has_enough_aio_nr()) {
        ret.push_back(reactor_backend_selector("linux-aio"));
    }
    ret.push_back(reactor_backend_selector("epoll"));
    return ret;
}

}

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
#include "core/io_desc.hh"
#include "core/thread_pool.hh"
#include "core/syscall_result.hh"
#include <seastar/core/print.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/internal/liburing.hh>
#include <seastar/util/defer.hh>
#include <chrono>
#include <sys/poll.h>
#include <sys/syscall.h>

#ifdef HAVE_OSV
#include <osv/newpoll.hh>
#endif

namespace seastar {

namespace internal {
bool io_uring_preempt = false;
}

using namespace std::chrono_literals;
using namespace internal;
using namespace internal::linux_abi;


aio_storage_context::aio_storage_context(reactor *r)
    : _r(r)
    , _io_context(0)
{
    for (unsigned i = 0; i != max_aio; ++i) {
        _free_iocbs.push(&_iocb_pool[i]);
    }

    setup_aio_context(max_aio, &_io_context);
    _sem.signal(max_aio);
}

aio_storage_context::~aio_storage_context() {
    io_destroy(_io_context);
}

future<internal::linux_abi::iocb*>
aio_storage_context::get_one() {
    return _sem.wait(1).then([this] {
        auto r = _free_iocbs.size();
        assert(r > 0);
        auto io = _free_iocbs.top();
        _free_iocbs.pop();
        return io;
    });
}

void
aio_storage_context::put_one(internal::linux_abi::iocb* io) {
    _free_iocbs.push(io);
    _sem.signal(1);
}

unsigned
aio_storage_context::outstanding() const {
    return max_aio - _free_iocbs.size();
}

extern bool aio_nowait_supported;

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
}

void
aio_storage_context::submit_io(io_desc* desc, internal::io_request req) {
    // We can ignore the future returned here, because the submitted aio will be polled
    // for and completed in process_io().
    (void)get_one().then([this, desc, req = std::move(req)] (linux_abi::iocb* iocb) mutable {
        auto& io = *iocb;
        prepare_iocb(req, io);
        if (_r->_aio_eventfd) {
            set_eventfd_notification(io, _r->_aio_eventfd->get_fd());
        }
        if (aio_nowait_supported) {
            set_nowait(io, true);
        }
        set_user_data(io, desc);
        _pending_aio.push_back(&io);
    });
}

bool aio_storage_context::process_io()
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
        put_one(iocb);
        auto desc = reinterpret_cast<io_desc*>(ev[i].data);
        try {
            _r->handle_io_result(ev[i].res);
            desc->set_value(size_t(ev[i].res));
        } catch (...) {
            desc->set_exception(std::current_exception());
        }
        delete desc;
    }
    return n;
}

// Returns: number of iocbs consumed (0 or 1)
size_t
aio_storage_context::handle_aio_error(linux_abi::iocb* iocb, int ec) {
    switch (ec) {
        case EAGAIN:
            return 0;
        case EBADF: {
            auto desc = reinterpret_cast<io_desc*>(get_user_data(*iocb));
            put_one(iocb);
            try {
                throw std::system_error(EBADF, std::system_category());
            } catch (...) {
                desc->set_exception(std::current_exception());
            }
            delete desc;
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

bool aio_storage_context::flush_pending_aio() {
    bool did_work = false;
    while (!_pending_aio.empty()) {
        auto nr = _pending_aio.size();
        auto iocbs = _pending_aio.data();
        auto r = io_submit(_io_context, nr, iocbs);
        size_t nr_consumed;
        if (r == -1) {
            nr_consumed = handle_aio_error(iocbs[0], errno);
        } else {
            nr_consumed = size_t(r);
        }

        did_work = true;
        if (nr_consumed == nr) {
            _pending_aio.clear();
        } else {
            _pending_aio.erase(_pending_aio.begin(), _pending_aio.begin() + nr_consumed);
        }
    }
    if (!_pending_aio_retry.empty()) {
        auto retries = std::exchange(_pending_aio_retry, {});
        // FIXME: future is discarded
        (void)engine()._thread_pool->submit<syscall_result<int>>([this, retries] () mutable {
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
    did_work |= process_io();
    return did_work;
}

bool aio_storage_context::aio_storage_context_pollfn::poll() {
    return _ctx->flush_pending_aio();
}

bool aio_storage_context::aio_storage_context_pollfn::pure_poll() {
    return poll(); // actually performs work, but triggers no user continuations, so okay
}

bool aio_storage_context::aio_storage_context_pollfn::try_enter_interrupt_mode() {
    // Because aio depends on polling, it cannot generate events to wake us up, Therefore, sleep
    // is only possible if there are no in-flight aios. If there are, we need to keep polling.
    //
    // Alternatively, if we enabled _aio_eventfd, we can always enter
    unsigned executing = _ctx->outstanding();
    return executing == 0 || _ctx->_r->_aio_eventfd;
}
void aio_storage_context::aio_storage_context_pollfn::exit_interrupt_mode() {
    // nothing to do
}

std::unique_ptr<pollfn> aio_storage_context::create_poller() {
    return std::make_unique<aio_storage_context_pollfn>(this);
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

void reactor_backend_aio::context::flush() {
    if (last != iocbs.get()) {
        auto nr = last - iocbs.get();
        last = iocbs.get();
        io_submit(io_context, nr, iocbs.get());
    }
}

reactor_backend_aio::io_poll_poller::io_poll_poller(reactor_backend_aio* b) : _backend(b) {
}

bool reactor_backend_aio::io_poll_poller::poll() {
    return _backend->wait_and_process(0, nullptr);
}

bool reactor_backend_aio::io_poll_poller::pure_poll() {
    return _backend->wait_and_process(0, nullptr);
}

bool reactor_backend_aio::io_poll_poller::try_enter_interrupt_mode() {
    return true;
}

void reactor_backend_aio::io_poll_poller::exit_interrupt_mode() {
}

linux_abi::iocb* reactor_backend_aio::new_iocb() {
    if (_iocb_pool.empty()) {
        return new linux_abi::iocb;
    }
    auto ret = _iocb_pool.top().release();
    _iocb_pool.pop();
    return ret;
}

void reactor_backend_aio::free_iocb(linux_abi::iocb* iocb) {
    _iocb_pool.push(std::unique_ptr<linux_abi::iocb>(iocb));
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
            auto* pr = reinterpret_cast<promise<>*>(uintptr_t(event.data));
            pr->set_value();
            free_iocb(iocb);
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
    , _aio_storage_context(_r)
{
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

bool reactor_backend_aio::wait_and_process(int timeout, const sigset_t* active_sigmask) {
    bool did_work = service_preempting_io();
    if (did_work) {
        timeout = 0;
    }
    _polling_io.replenish(&_timerfd_iocb, _timerfd_in_polling_io);
    _polling_io.replenish(&_smp_wakeup_iocb, _smp_wakeup_in_polling_io);
    _polling_io.flush();
    did_work |= await_events(timeout, active_sigmask);
    did_work |= service_preempting_io(); // clear task quota timer
    return did_work;
}

future<> reactor_backend_aio::poll(pollable_fd_state& fd, promise<> pollable_fd_state::*promise_field, int events) {
    if (!_r->_epoll_poller) {
        _r->_epoll_poller = reactor::poller(std::make_unique<io_poll_poller>(this));
    }
    try {
        if (events & fd.events_known) {
            fd.events_known &= ~events;
            return make_ready_future<>();
        }
        auto iocb = new_iocb(); // FIXME: merge with pollable_fd_state
        *iocb = make_poll_iocb(fd.fd.get(), events);
        fd.events_rw = events == (POLLIN|POLLOUT);
        auto pr = &(fd.*promise_field);
        *pr = promise<>();
        set_user_data(*iocb, pr);
        _polling_io.queue(iocb);
        return pr->get_future();
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<> reactor_backend_aio::readable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollin, POLLIN);
}

future<> reactor_backend_aio::writeable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollout, POLLOUT);
}

future<> reactor_backend_aio::readable_or_writeable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollin, POLLIN|POLLOUT);
}

void reactor_backend_aio::forget(pollable_fd_state& fd) {
    // ?
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

std::unique_ptr<pollfn> reactor_backend_aio::create_backend_poller() {
    return _aio_storage_context.create_poller();
}

void reactor_backend_aio::submit_io(io_desc* desc, internal::io_request req) {
    return _aio_storage_context.submit_io(desc, std::move(req));
}

reactor_backend_epoll::reactor_backend_epoll(reactor* r)
        : _r(r)
        , _epollfd(file_desc::epoll_create(EPOLL_CLOEXEC))
        , _aio_storage_context(_r)
{
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
            complete_epoll_event(*pfd, &pollable_fd_state::pollin, events, EPOLLIN|EPOLLOUT);
        } else {
            // Normal processing where EPOLLIN and EPOLLOUT are waited for via different
            // futures.
            complete_epoll_event(*pfd, &pollable_fd_state::pollin, events, EPOLLIN);
            complete_epoll_event(*pfd, &pollable_fd_state::pollout, events, EPOLLOUT);
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

void reactor_backend_epoll::complete_epoll_event(pollable_fd_state& pfd, promise<> pollable_fd_state::*pr,
        int events, int event) {
    if (pfd.events_requested & events & event) {
        pfd.events_requested &= ~event;
        pfd.events_known &= ~event;
        (pfd.*pr).set_value();
        pfd.*pr = promise<>();
    }
}

void reactor_backend_epoll::signal_received(int signo, siginfo_t* siginfo, void* ignore) {
    if (engine_is_ready()) {
        engine()._signals.action(signo, siginfo, ignore);
    } else {
        reactor::signals::failed_to_handle(signo);
    }
}

future<> reactor_backend_epoll::get_epoll_future(pollable_fd_state& pfd,
        promise<> pollable_fd_state::*pr, int event) {
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
        engine().start_epoll();
    }
    pfd.*pr = promise<>();
    return (pfd.*pr).get_future();
}

future<> reactor_backend_epoll::readable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN);
}

future<> reactor_backend_epoll::writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollout, EPOLLOUT);
}

future<> reactor_backend_epoll::readable_or_writeable(pollable_fd_state& fd) {
    return get_epoll_future(fd, &pollable_fd_state::pollin, EPOLLIN | EPOLLOUT);
}

void reactor_backend_epoll::forget(pollable_fd_state& fd) {
    if (fd.events_epoll) {
        ::epoll_ctl(_epollfd.get(), EPOLL_CTL_DEL, fd.fd.get(), nullptr);
    }
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

void reactor_backend_epoll::reset_preemption_monitor() {
    _r->_preemption_monitor.head.store(0, std::memory_order_relaxed);
}

std::unique_ptr<pollfn> reactor_backend_epoll::create_backend_poller() {
    return _aio_storage_context.create_poller();
}

void reactor_backend_epoll::submit_io(io_desc* desc, internal::io_request req) {
    return _aio_storage_context.submit_io(desc, std::move(req));
}

#ifdef HAVE_OSV
reactor_backend_osv::reactor_backend_osv() {
}

bool
reactor_backend_osv::wait_and_process() {
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
    std::cerr << "reactor_backend_osv does not support file descriptors - forget() shouldn't have been called!\n";
    abort();
}

void
reactor_backend_osv::enable_timer(steady_clock_type::time_point when) {
    _poller.set_timer(when);
}

std::unique_ptr<pollfn> reactor_backend_osv::create_backend_poller() {
    std::cerr << "reactor_backend_osv does not support file descriptors - create_backend_poller() shouldn't have been called!\n";
    abort();
}

void reactor_backend_osv::submit_io(io_desc* desc, internal::io_request req) {
    std::cerr << "reactor_backend_osv does not support file descriptors - submit_io() shouldn't have been called!\n";
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

using namespace seastar::internal;

static bool detect_uring() {

    linux_abi::io_uring_probe probe;
    try {
        uring_interrupt_context ctx;
        probe = ctx.probe();
    } catch (std::system_error& e) {
        return false;
    }

    std::array<uint8_t, 10> opcodes_of_interest = {
        IORING_OP_READV,
        IORING_OP_READ,
        IORING_OP_READ_FIXED,
        IORING_OP_WRITEV,
        IORING_OP_WRITE,
        IORING_OP_WRITE_FIXED,
        IORING_OP_FSYNC,
        IORING_OP_FALLOCATE,
        IORING_OP_OPENAT,
        IORING_OP_CLOSE,
    };

    for (auto& op : opcodes_of_interest) {
        if (probe.last_op < op) {
            return false;
        }
        if (~probe.ops[op].flags & 1) {
            return false;
        }
    }
    return true;
}

std::unique_ptr<reactor_backend> reactor_backend_selector::create(reactor* r) {
    if (_name == "linux-uring") {
        io_uring_preempt = true;
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
        ret.push_back(reactor_backend_selector("linux-uring"));
    }

    if (detect_aio_poll()) {
        ret.push_back(reactor_backend_selector("linux-aio"));
    }
    ret.push_back(reactor_backend_selector("epoll"));
    return ret;
}


// FIXME : make it compile without liburing
reactor_backend_uring::uring_pollfn::uring_pollfn(reactor_backend_uring* b)
    : _backend(b)
{}

static constexpr uint64_t io_desc_poll_type = (1ull << 63);
static constexpr uint64_t static_eventfd_poll_type = (1ull << 62);
static constexpr uint64_t promise_poll_type = (1ull << 61);

static constexpr uint64_t type_mask = io_desc_poll_type
                                    | static_eventfd_poll_type
                                    | promise_poll_type;

bool reactor_backend_uring::uring_pollfn::poll() {
    _backend->_must_resubmit = false;
    bool did_work = _backend->_irq_ctx.poll([this] (io_uring_cqe* cqe) {
                return _backend->process_one_cqe(cqe);
            }) |
           _backend->_poll_ctx.poll([this] (io_uring_cqe* cqe) {
                return _backend->process_one_cqe(cqe);
            });

    if (_backend->_must_resubmit) {
        _backend->_must_resubmit = false;
        _backend->_irq_ctx.flush();
    }
    // We need to not only read the current values, but issue new polls
    // so we can read completions again.
    did_work |= _backend->service_preempting_io();
    _backend->reset_preemption_monitor();
    return did_work;
}

void reactor_backend_uring::process_one_cqe(io_uring_cqe* cqe) {
    uint64_t type = cqe->user_data & type_mask;
    uint64_t addr = cqe->user_data & ~type_mask;
    fmt::print("ADdr {:x}, type: {:x}, cqe {:x}, result {}\n", addr, type, uint64_t(cqe), cqe->res);
    if (type == io_desc_poll_type) {
        io_desc* desc = reinterpret_cast<io_desc*>(addr);
        try {
            _r->handle_io_result(cqe->res);
            desc->set_value(size_t(cqe->res));
        } catch (...) {
            desc->set_exception(std::current_exception());
        }
    } else if (type == static_eventfd_poll_type) {
        _must_resubmit = true;
        if (int(addr) == _steady_clock_timer.get()) {
            process_timerfd();
        }
        if (int(addr) == _r->_notify_eventfd.get()) {
            // My idea for re-registering here is that we always should have a poller for this,
            // but just one. We don't poll before going to sleep, so just re-set here.
            process_smp_wakeup();
            register_poll(_r->_notify_eventfd.get());
        }
#if 0
        if (int(addr) == _r->_task_quota_timer.get()) {
            process_task_quota_timer();
        }
#endif
    } else if (type == promise_poll_type) {
        if (addr) {
            promise<>* pr = reinterpret_cast<promise<>*>(addr);
            pr->set_value();
        }
    } else {
        std::abort();
    }
}

void reactor_backend_uring::register_poll(int fd) {
    auto new_sqe = _irq_ctx.get_sqe();
    io_uring_prep_poll_add(new_sqe, fd, POLLIN);
    uint64_t data = fd;
    data |= static_eventfd_poll_type;
    io_uring_sqe_set_data(new_sqe, reinterpret_cast<void*>(data));
}

void reactor_backend_uring::unregister_poll(int fd) {
    auto sqe = _irq_ctx.get_sqe();
    io_uring_prep_poll_remove(sqe, reinterpret_cast<void*>(fd));
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(static_eventfd_poll_type));
}

bool reactor_backend_uring::uring_pollfn::pure_poll() {
    return _backend->_poll_ctx.nr_pending() > 0;
}
bool reactor_backend_uring::uring_pollfn::try_enter_interrupt_mode() {
    return _backend->_poll_ctx.nr_pending() == 0;
}

void reactor_backend_uring::uring_pollfn::exit_interrupt_mode() {
}

reactor_backend_uring::reactor_backend_uring(reactor* r)
    : _r(r)
{
//    fmt::print("Created uring backend, notify fd {:x}\n", _r->_notify_eventfd.get());
//    replenish(_steady_clock_timer.get());
    register_poll(_r->_notify_eventfd.get());

    setup_aio_context(2, &_io_context);
    _task_quota_timer_iocb = make_poll_iocb(_r->_task_quota_timer.get(), POLLIN);
    _timerfd_iocb = make_poll_iocb(_steady_clock_timer.get(), POLLIN);
//    replenish(_r->_task_quota_timer.get());

    // Protect against spurious wakeups - if we get notified that the timer has
    // expired when it really hasn't, we don't want to block in read(tfd, ...).
    auto tfd = _r->_task_quota_timer.get();
    ::fcntl(tfd, F_SETFL, ::fcntl(tfd, F_GETFL) | O_NONBLOCK);

    sigset_t mask = make_sigset_mask(hrtimer_signal());
    auto e = ::pthread_sigmask(SIG_BLOCK, &mask, NULL);
    assert(e == 0);
}

void reactor_backend_uring::process_timerfd() {
    uint64_t expirations = 0;
    _steady_clock_timer.read(&expirations, 8);
    if (expirations) {
        _r->service_highres_timer();
    }
}

void reactor_backend_uring::process_smp_wakeup() {
    uint64_t ignore = 0;
    _r->_notify_eventfd.read(&ignore, 8);
}

void reactor_backend_uring::process_task_quota_timer() {
    uint64_t v;
    (void)_r->_task_quota_timer.read(&v, 8);
}

file_desc reactor_backend_uring::make_timerfd() {
    return file_desc::timerfd_create(CLOCK_MONOTONIC, TFD_CLOEXEC|TFD_NONBLOCK);
}

void reactor_backend_uring::signal_received(int signo, siginfo_t* siginfo, void* ignore) {
    engine()._signals.action(signo, siginfo, ignore);
}

bool reactor_backend_uring::wait_and_process(int timeout, const sigset_t* active_sigmask) {
    bool did_work = service_preempting_io();
    if (did_work) {
        timeout = 0;
    }

    // FIXME: confirm: because this is just a go-to-sleep routine for io_uring, we know that the
    // pollers were called already and there is no need to call any io_uring_submit before we wait -
    // also make sure that maybe sync_wait is doing that already anyway.
    timespec ts {};
    if (timeout) {
       ts = posix::to_timespec(timeout * 1ms);
    }

    register_poll(_steady_clock_timer.get());
    // tell the kernel we're now waiting for the timer eventfd in our
    // interface
    _irq_ctx.flush();

    // FIXME: we probably have to wait for the timer here, because otherwise we'd have to wait
    // synchronously on two things
//    io_uring_cqe *cqe = _irq_ctx.sync_wait(timeout > 0 ? &ts : nullptr, active_sigmask);
    _irq_ctx.sync_wait(timeout > 0 ? &ts : nullptr, active_sigmask);
    // FIXME: cqe can be null here, understand why and also if we really can rely on just the next
    // poll
//    assert(cqe);
    // may benefit from further cleanup, need to understand this a bit better
    // FIXME: may not be needed if we don't call peek cqe;
    _irq_ctx.poll([this] (io_uring_cqe* cqe) {
        return process_one_cqe(cqe);
    });
//    fmt::print("poll returned\n");
    _poll_ctx.poll([this] (io_uring_cqe* cqe) {
       return process_one_cqe(cqe);
    });

    // since we're awake, the timer is once more the responsibility of the
    // getevents interface
    unregister_poll(_steady_clock_timer.get());
    reset_preemption_monitor(); // clear task quota timer
//    fmt::print("irq returned\n");
    return true;
}

future<> reactor_backend_uring::poll(pollable_fd_state& fd, promise<> pollable_fd_state::*promise_field, int events) {
    try {
        if (events & fd.events_known) {
            fd.events_known &= ~events;
            return make_ready_future<>();
        }

        auto sqe = _irq_ctx.get_sqe();
        io_uring_prep_poll_add(sqe, fd.fd.get(), events);
        fd.events_rw = events == (POLLIN|POLLOUT);

        auto pr = &(fd.*promise_field);
        *pr = promise<>();

        auto data = reinterpret_cast<uint64_t>(pr) | promise_poll_type;
        io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(data));
        return pr->get_future();
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

future<> reactor_backend_uring::readable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollin, POLLIN);
}

future<> reactor_backend_uring::writeable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollout, POLLOUT);
}

future<> reactor_backend_uring::readable_or_writeable(pollable_fd_state& fd) {
    return poll(fd, &pollable_fd_state::pollin, POLLIN|POLLOUT);
}

void reactor_backend_uring::forget(pollable_fd_state& fd) {
    auto sqe = _irq_ctx.get_sqe();
    io_uring_prep_poll_remove(sqe, reinterpret_cast<void*>(fd.fd.get()));
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(promise_poll_type));
}

void reactor_backend_uring::start_tick() {
    // see aio implementation for details
    g_need_preempt = reinterpret_cast<const preemption_monitor*>(_io_context + 8);
}

void reactor_backend_uring::stop_tick() {
    g_need_preempt = &_r->_preemption_monitor;
}

void reactor_backend_uring::arm_highres_timer(const ::itimerspec& its) {
    _steady_clock_timer.timerfd_settime(TFD_TIMER_ABSTIME, its);
}

// FIXME: find a way to share this code
bool reactor_backend_uring::service_preempting_io() {
    linux_abi::io_event a[2];
    auto r = io_getevents(_io_context, 0, 2, a, 0);
    assert(r != -1);
    bool did_work = false;
    for (unsigned i = 0; i != unsigned(r); ++i) {
        if (get_iocb(a[i]) == &_task_quota_timer_iocb) {
            //fmt::print("got a task quota\n");
            _task_quota_timer_in_preempting_io = false;
            process_task_quota_timer();
        } else if (get_iocb(a[i]) == &_timerfd_iocb) {
            //fmt::print("got a timerfd\n");
            _timerfd_in_preempting_io = false;
            process_timerfd();
            did_work = true;
        }
    }
    return did_work;
}

void reactor_backend_uring::reset_preemption_monitor() {
    service_preempting_io();
    iocb* submit_queue[2];
    int nr = 0;
//    fmt::print("preempting reseting\n");
    if (!_timerfd_in_preempting_io) {
//        fmt::print("first\n");
        _timerfd_in_preempting_io = true;
        submit_queue[nr++] = &_timerfd_iocb;
    }

    if (!_task_quota_timer_in_preempting_io) {
  //      fmt::print("second\n");
        _task_quota_timer_in_preempting_io = true;
        submit_queue[nr++] = &_task_quota_timer_iocb;
    }

    if (nr) {
        //fmt::print("io submit on {}\n", nr);
        // FIXME: check return
        io_submit(_io_context, nr, submit_queue);
    }
}

void reactor_backend_uring::request_preemption() {
    // FIXME: ioring NOP ? Will that go to the kernel ?
    // Maybe we should indeed read everything in a 64-bit integer, and
    // then request preemption just changes the addr of g_need_preempt to
    // some variable of ours
    // FIXME: tomorrow I will do this with a syscall, just to get the whole
    // thing working. Then send a patch to adjust need_preempt
    //fmt::print("request preempt: ignored\n");
    // FIXME: share the code below somehow

    ::itimerspec expired = {};
    expired.it_value.tv_nsec = 1;
    arm_highres_timer(expired); // will trigger immediately, triggering the preemption monitor

    // This might have been called from poll_once. If that is the case, we cannot assume that timerfd is being
    // monitored.
    if (!_timerfd_in_preempting_io) {
        iocb* submit_queue[1];
        _task_quota_timer_in_preempting_io = true;
        // FIXME: check return
        io_submit(_io_context, 1, submit_queue);
    }

    // The kernel is not obliged to deliver the completion immediately, so wait for it
    while (!need_preempt()) {
        std::atomic_signal_fence(std::memory_order_seq_cst);
    }
}

void reactor_backend_uring::start_handling_signal() {
    // The uring backend only uses SIGHUP/SIGTERM/SIGINT (same as aio). We don't need to handle them right away and our
    // implementation of request_preemption is not signal safe, so do nothing.
}

std::unique_ptr<pollfn> reactor_backend_uring::create_backend_poller() {
    return std::make_unique<uring_pollfn>(this);
}

void prepare_sqe(io_request& req, io_uring_sqe& sqe) {
    switch (req.opcode()) {
    case io_request::operation::write:
        io_uring_prep_write(&sqe, req.fd(), req.address(), req.size(), req.pos());
        break;
    case io_request::operation::writev:
        io_uring_prep_writev(&sqe, req.fd(), reinterpret_cast<const iovec*>(req.address()), req.size(), req.pos());
        break;
    case io_request::operation::read:
        io_uring_prep_read(&sqe, req.fd(), req.address(), req.size(), req.pos());
        break;
    case io_request::operation::readv:
        io_uring_prep_readv(&sqe, req.fd(), reinterpret_cast<const iovec*>(req.address()), req.size(), req.pos());
        break;
    case io_request::operation::fdatasync:
        io_uring_prep_fsync(&sqe, req.fd(), IORING_FSYNC_DATASYNC);
        break;
    }
}

void reactor_backend_uring::submit_io(io_desc* desc, internal::io_request req) {
    io_uring_sqe *sqe;

    // FIXME: may need a semaphore here protecting the sqe list.
    // Axboe:
    // n. Normally an application would ask for a ring of a given size, and the
    // assumption may be that this size corresponds directly to how many requests the application can have pending in the
    // kernel. However, since the sqe lifetime is only that of the actual submission of it, it's possible for the application to
    // drive a higher pending request count than the SQ ring size would indicate. The application must take care not to do so,
    // or it could risk overflowing the CQ ring. By default, the CQ ring is twice the size of the SQ ring. This allows the
    // application some amount of flexibility in managing this aspect, but it doesn't completely remove the need to do so. If
    // the application does violate this restriction, it will be tracked as an overflow condition in the CQ ring. More details on
    // that later.
    //
    // Summary: there is no need to make sure there is room in the sqe queue if we submit often
    // enough, but must make sure the cqe ring won't overflow.
    if (req.device_supports_iopoll() && (req.is_read() || req.is_write())) {
        sqe = _poll_ctx.get_sqe();
    } else {
        sqe = _irq_ctx.get_sqe();
    }

    prepare_sqe(req, *sqe);
    auto data = reinterpret_cast<uint64_t>(desc) | io_desc_poll_type;
    fmt::print("Submitting I/O {:x}\n", data);
    io_uring_sqe_set_data(sqe, reinterpret_cast<void*>(data));
}

}

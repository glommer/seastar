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


#include <seastar/core/future-util.hh>
#include <seastar/core/file.hh>
#include <seastar/core/fair_queue.hh>
#include <seastar/core/io_queue.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics.hh>
#include <seastar/core/linux-aio.hh>
#include <seastar/core/internal/io_desc.hh>
#include <chrono>
#include <mutex>
#include <array>
#include <fmt/format.h>
#include <fmt/ostream.h>

namespace seastar {

using namespace std::chrono_literals;
using namespace internal::linux_abi;

void io_desc_read_write::notify_requests_finished() {
    _ioq_ptr->notify_requests_finished(*this);
}

void io_desc_read_write::set_exception(std::exception_ptr eptr) {
    notify_requests_finished();
    _pr.set_exception(eptr);
    _ioq_ptr->free_io_desc(this);
}

void io_desc_read_write::complete_with(ssize_t ret) {
    notify_requests_finished();

    if (engine().cpu_id() == _owner) {
        try {
            engine().handle_io_result(ret);
            _pr.set_value(ret);
            _ioq_ptr->free_io_desc(this);
        } catch (...) {
            set_exception(std::current_exception());
        }
    } else {
    }
}

ticket io_desc_read_write::operator()(ticket& t) {
    try {
#if 0
        pclass.nr_queued--;
        pclass.ops++;
        pclass.bytes += len;
        pclass.queue_time = std::chrono::duration_cast<std::chrono::duration<double>>(std::chrono::steady_clock::now() - start);
#endif
        smp::_reactors[_owner]->submit_io(this, &_req);
        return ticket{1, weight, size};
    } catch (...) {
        set_exception(std::current_exception());
        return ticket{0, 0, 0};
    }
}

io_desc_read_write::io_desc_read_write(io_queue* ioq, unsigned class_id, unsigned weight, unsigned size, internal::io_request req)
   : fair_queue_request_descriptor(weight, size)
   , _ioq_ptr(ioq)
   , _id(class_id)
   , _req(std::move(req))
   , _owner(engine().cpu_id())
{}

fair_queue::config io_queue::make_fair_queue_config(config iocfg) {
    fair_queue::config cfg;
    cfg.capacity = std::min(iocfg.capacity, reactor::max_aio_per_queue);
    cfg.max_req_count = iocfg.max_req_count;
    cfg.max_bytes_count = iocfg.max_bytes_count;
    return cfg;
}

struct multishard_queue_request : public fair_queue_request_descriptor {
    io_queue* ioq_ptr;
    std::atomic<int64_t> pending_quantity = { 0 };
    std::atomic<int64_t> pending_weight = { 0 };
    std::atomic<int64_t> pending_size = { 0 };

    virtual ticket operator()(ticket& t) override {
        pending_quantity.fetch_add(t.quantity, std::memory_order_relaxed);
        pending_weight.fetch_add(t.weight, std::memory_order_relaxed);
        pending_size.fetch_add(t.size, std::memory_order_release);
        fmt::print("Got a grant of {}, {}, {}\n", t.quantity, t.weight, t.size);
        return t;
    }

    ticket get() {
        ticket t;
        t.size = pending_size.exchange(0, std::memory_order_acquire);
        t.quantity = pending_quantity.exchange(0, std::memory_order_relaxed);
        t.weight = pending_weight.exchange(0, std::memory_order_relaxed);

        fmt::print("Using a grant of {}, {}, {}\n", t.quantity, t.weight, t.size);
        return t;
    }
};

static thread_local multishard_queue_request hack;


io_queue::io_queue(io_queue::config cfg)
    : _priority_classes()
    , _fq(new fair_queue(io_queue::make_fair_queue_config(cfg)))
    , _multishard_fq(cfg.fq)
    , _shard_ptr(_multishard_fq->register_priority_class(1))
    , _config(std::move(cfg)) {

    fmt::print("Registering {:x}\n", uint64_t(&hack));
    _multishard_fq->multishard_register(_shard_ptr, &hack);
}

void io_queue::poll_io_queue() {
    // Step 1: Set our limits back to our natural quota. If we don't do that, once we
    // dispatch a lot we will keep dispatching a lot and will be unfair to the other
    // shards.
    auto extra = _fq->prune_excess_capacity();
    if (extra.quantity > 0 || extra.weight > 0 || extra.size > 0) {
        multishard_queue_request fq;
        fq.weight = extra.weight;
        fq.size = extra.size;
        _multishard_fq->notify_requests_finished(fq, extra.quantity);
    }

    auto had = _fq->waiters();
    // Step 2: Dispatch the requests we currently have, within our quota.
    auto ret = _fq->dispatch_requests();
    if (ret || had) {
        fmt::print("Dispatched {} requests f2, prev waiters {}\n", ret, had);
    }

    // Step 3: If we still have waiters, grab capacity from the I/O queue
    // if possible and try to dispatch. Maybe capacity that we requested
    // in the last poll period only got ready now and we try with that first.
    if (_fq->waiters()) {
        auto t = hack.get();
        _fq->add_capacity(t);
        auto ret = _fq->dispatch_requests();
        fmt::print("Dispatched {} requests f3\n", ret);
    }

    // Step 3: If we still have waiters, try to acquire more capacity from
    // the I/O queue. If there is extra capacity we will be able to dispatch
    // more right away. If not, we may consume it in the next poll period.
    if (_fq->waiters()) {
        extra = _fq->waiting();
        fmt::print("Asking for a grant of {} {} {}. Waiters {}\n", extra.quantity, extra.weight, extra.size, _fq->waiters());
        // FIXME: the fact that this forces the request to be atomic is good enough reason to make
        // those two queues separately.
        hack.weight = extra.quantity;
        hack.size = extra.size;

        // FIXME: merge with dispatch
        _multishard_fq->multishard_queue(_shard_ptr);
        _multishard_fq->multishard_dispatch_requests();
        auto ret = _fq->dispatch_requests();
        fmt::print("Dispatched {} requests f4\n", ret);
    }
    
    // Step 4: if we have no more waiters, give back the capacity we grabbed
    // but didn't use
    if (!_fq->waiters()) {
        extra = _fq->prune_all_capacity();
        multishard_queue_request fq;
        fq.weight = extra.weight;
        fq.size = extra.size;
        _multishard_fq->notify_requests_finished(fq, extra.quantity);
    }
}

void io_queue::notify_requests_finished(fair_queue_request_descriptor& desc) {
    _fq->notify_requests_finished(desc);
}

void io_queue::free_io_desc(io_desc_read_write* desc) {
    _priority_classes[desc->id()]->free_io_desc(desc);
}

io_queue::~io_queue() {
    // It is illegal to stop the I/O queue with pending requests.
    // Technically we would use a gate to guarantee that. But here, it is not
    // needed since this is expected to be destroyed only after the reactor is destroyed.
    //
    // And that will happen only when there are no more fibers to run. If we ever change
    // that, then this has to change.
    for (auto&& pc_data : _priority_classes) {
        if (pc_data) {
            _fq->unregister_priority_class(pc_data->ptr);
        }
    }
}

std::mutex io_queue::_register_lock;
std::array<uint32_t, io_queue::_max_classes> io_queue::_registered_shares;
// We could very well just add the name to the io_priority_class. However, because that
// structure is passed along all the time - and sometimes we can't help but copy it, better keep
// it lean. The name won't really be used for anything other than monitoring.
std::array<sstring, io_queue::_max_classes> io_queue::_registered_names;

io_priority_class io_queue::register_one_priority_class(sstring name, uint32_t shares) {
    std::lock_guard<std::mutex> lock(_register_lock);
    for (unsigned i = 0; i < _max_classes; ++i) {
        if (!_registered_shares[i]) {
            _registered_shares[i] = shares;
            _registered_names[i] = std::move(name);
        } else if (_registered_names[i] != name) {
            continue;
        } else {
            // found an entry matching the name to be registered,
            // make sure it was registered with the same number shares
            // Note: those may change dynamically later on in the
            // fair queue priority_class_ptr
            assert(_registered_shares[i] == shares);
        }
        return io_priority_class(i);
    }
    throw std::runtime_error("No more room for new I/O priority classes");
}

seastar::metrics::label io_queue_shard("ioshard");

io_queue::priority_class_data::priority_class_data(sstring name, sstring mountpoint, priority_class_ptr ptr)
    : ptr(ptr)
    , bytes(0)
    , ops(0)
    , nr_queued(0)
    , queue_time(1s)
{
    register_stats(name, mountpoint);

    for (auto i = 0u; i < max_io_desc; ++i) {
        _free_io_desc.push(&_io_desc[i]);
    }
}

void io_queue::priority_class_data::free_io_desc(io_desc_read_write* desc) {
    _free_io_desc.push(desc);
//    _io_desc_sem.signal(1);
}

io_desc_read_write* io_queue::priority_class_data::alloc_io_desc(io_queue* ioq_ptr, unsigned id, size_t len, internal::io_request req) {
    unsigned weight;
    size_t size;

    if (req.is_write()) {
        weight = ioq_ptr->_config.disk_req_write_to_read_multiplier;
        size = ioq_ptr->_config.disk_bytes_write_to_read_multiplier * len;
    } else if (req.is_read()) {
        weight = io_queue::read_request_base_count;
        size = io_queue::read_request_base_count * len;
    } else {
        throw std::runtime_error(fmt::format("Unrecognized request passing through I/O queue {}", req.opname()));
    }

    // FIXME: check capacity
//    _io_desc_sem.consume(1);

    auto desc = _free_io_desc.top();
    _free_io_desc.pop();

    *desc = io_desc_read_write(ioq_ptr, id, weight, size, std::move(req));
    return desc;
}

void
io_queue::priority_class_data::rename(sstring new_name, sstring mountpoint) {
    try {
        register_stats(new_name, mountpoint);
    } catch (metrics::double_registration &e) {
        // we need to ignore this exception, since it can happen that
        // a class that was already created with the new name will be
        // renamed again (this will cause a double registration exception
        // to be thrown).
    }

}

void
io_queue::priority_class_data::register_stats(sstring name, sstring mountpoint) {
    seastar::metrics::metric_groups new_metrics;
    namespace sm = seastar::metrics;
    auto shard = sm::impl::shard();
    auto owner = engine().cpu_id();

    auto ioq_group = sm::label("mountpoint");
    auto mountlabel = ioq_group(mountpoint);

    auto class_label_type = sm::label("class");
    auto class_label = class_label_type(name);
    new_metrics.add_group("io_queue", {
            sm::make_derive("total_bytes", bytes, sm::description("Total bytes passed in the queue"), {io_queue_shard(shard), sm::shard_label(owner), mountlabel, class_label}),
            sm::make_derive("total_operations", ops, sm::description("Total bytes passed in the queue"), {io_queue_shard(shard), sm::shard_label(owner), mountlabel, class_label}),
            // Note: The counter below is not the same as reactor's queued-io-requests
            // queued-io-requests shows us how many requests in total exist in this I/O Queue.
            //
            // This counter lives in the priority class, so it will count only queued requests
            // that belong to that class.
            //
            // In other words: the new counter tells you how busy a class is, and the
            // old counter tells you how busy the system is.

            sm::make_queue_length("queue_length", nr_queued, sm::description("Number of requests in the queue"), {io_queue_shard(shard), sm::shard_label(owner), mountlabel, class_label}),
            sm::make_gauge("delay", [this] {
                return queue_time.count();
            }, sm::description("total delay time in the queue"), {io_queue_shard(shard), sm::shard_label(owner), mountlabel, class_label}),
            sm::make_gauge("shares", [this] {
                return this->ptr->shares();
            }, sm::description("current amount of shares"), {io_queue_shard(shard), sm::shard_label(owner), mountlabel, class_label})
    });
    _metric_groups = std::exchange(new_metrics, {});
}

io_queue::priority_class_data& io_queue::find_or_create_class(const io_priority_class& pc) {
    auto id = pc.id();

    if (id >= _priority_classes.size()) {
        _priority_classes.resize(id + 1);
    }

    if (!_priority_classes[id]) {
        auto shares = _registered_shares.at(id);
        sstring name;
        {
            std::lock_guard<std::mutex> lock(_register_lock);
            name = _registered_names.at(id);
        }

        // A note on naming:
        //
        // We could just add the owner as the instance id and have something like:
        //  io_queue-<class_owner>-<counter>-<class_name>
        //
        // However, when there are more than one shard per I/O queue, it is very useful
        // to know which shards are being served by the same queue. Therefore, a better name
        // scheme is:
        //
        //  io_queue-<queue_owner>-<counter>-<class_name>, shard=<class_owner>
        //  using the shard label to hold the owner number
        //
        // This conveys all the information we need and allows one to easily group all classes from
        // the same I/O queue (by filtering by shard)
        auto pc_ptr = _fq->register_priority_class(shares);
        auto pc_data = make_lw_shared<priority_class_data>(name, mountpoint(), pc_ptr);

        _priority_classes[id] = pc_data;
    }
    return *_priority_classes[id];
}

future<size_t>
io_queue::queue_request(const io_priority_class& pc, size_t len, internal::io_request req) {
    //auto start = std::chrono::steady_clock::now();
    auto& pclass = find_or_create_class(pc);

    pclass.nr_queued++;
    auto desc = pclass.alloc_io_desc(this, pc.id(), len, std::move(req));

    _nr_queued++;

    _fq->queue(pclass.ptr, desc);
    return desc->get_future();
}

future<>
io_queue::update_shares_for_class(const io_priority_class pc, size_t new_shares) {
    return smp::submit_to(coordinator(), [this, pc, owner = engine().cpu_id(), new_shares] {
        auto& pclass = find_or_create_class(pc);
        _fq->update_shares(pclass.ptr, new_shares);
    });
}

void
io_queue::rename_priority_class(io_priority_class pc, sstring new_name) {
    if (_priority_classes.size() > pc.id() &&
            _priority_classes[pc.id()]) {
        _priority_classes[pc.id()]->rename(new_name, _config.mountpoint);
    }
}

}

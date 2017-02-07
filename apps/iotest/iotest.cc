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
 * Copyright (C) 2017 ScyllaDB
 */
#include "core/app-template.hh"
#include "core/distributed.hh"
#include "core/reactor.hh"
#include "core/future.hh"
#include "core/shared_ptr.hh"
#include "core/file.hh"
#include "core/sleep.hh"
#include "core/align.hh"
#include "core/timer.hh"
#include <chrono>
#include <boost/range/irange.hpp>
#include <boost/algorithm/string.hpp>
#include <iomanip>
#include <random>
#include <boost/array.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/extended_p_square_quantile.hpp>

using namespace std::chrono_literals;

static auto random_seed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
static std::default_random_engine random_generator(random_seed);

using namespace boost::accumulators;
using accumulator_type = accumulator_set<double, stats<tag::mean, tag::max, tag::extended_p_square_quantile>>;
using latency_clock = std::chrono::steady_clock;

struct io_parms {
    static constexpr size_t file_size = 1ull << 30;

    size_t _bytes_transferred = 0;
    size_t _iop = 0;
    size_t _size;
    size_t _align;
    unsigned _iodepth;
    std::chrono::microseconds _delay_between_ops;
    ::io_priority_class _priority;
    accumulator_type _latencies;
    file _fq;
    std::unique_ptr<char[], free_deleter> _buf;
    std::uniform_int_distribution<uint32_t> _pos_distribution;

    latency_clock::duration _real_duration;

    virtual void fill_buffer(char *buf) {}
protected:
    virtual sstring id() = 0;
    virtual future<size_t> iofunc(size_t pos) = 0;

    io_parms(size_t size, size_t align, const char *name, unsigned iodepth, std::chrono::microseconds delay)
        : _size(size)
        , _align(align)
        , _iodepth(iodepth)
        , _delay_between_ops(delay)
        , _priority(engine().register_one_priority_class(name, 100))
        , _latencies(extended_p_square_probabilities = boost::array<double, 5>({0.50, 0.90, 0.95, 0.99, 0.999}))
        , _buf(allocate_aligned_buffer<char>(size, align))
        , _pos_distribution(0, file_size / _size)
        {}

    future<> start(sstring dir) {
        fill_buffer(_buf.get());

        auto name = sprint("%s/test-queue-%d", dir, engine().cpu_id());
        return open_file_dma(name, open_flags::rw | open_flags::create | open_flags::truncate).then([this, name] (auto f) {
            _fq = f;
            return remove_file(name);
        });
    }
public:
    future<> do_test(latency_clock::time_point test_start, std::chrono::seconds duration) {
        auto iodepth = boost::irange(0u, _iodepth);
        auto test_end = test_start + duration;

        return parallel_for_each(iodepth.begin(), iodepth.end(), [this, test_end] (auto idx) {
            return do_until([this, test_end] { return latency_clock::now() >= test_end; }, [this] () mutable {
                auto req_start = latency_clock::now();
                auto pos = _pos_distribution(random_generator) * _size;
                return this->iofunc(pos).then([req_start = std::move(req_start), this] (size_t size) {
                    _bytes_transferred += size;
                    _iop++;
                    auto lat = latency_clock::now() - req_start;
                    _latencies(std::chrono::duration_cast<std::chrono::microseconds>(lat).count());
                    return sleep(this->_delay_between_ops);
                });
            });
        }).then([this, test_start] {
            _real_duration = latency_clock::now() - test_start;
        });
    }

    void report_stats() {
        std::stringstream ss;
        ss << std::fixed << std::setprecision(2);
        auto bandwidth = (_bytes_transferred >> 10) / std::chrono::duration_cast<std::chrono::duration<double>>(_real_duration).count();
        auto iops = _iop / std::chrono::duration_cast<std::chrono::duration<double>>(_real_duration).count();
        ss << id() << ":" <<std::endl;
        ss << "Bandwidth (KB/s): " << std::setw(16) << bandwidth << std::endl;
        ss << "IOPS            : " << std::setw(16) << iops << std::endl;
        ss << "latencies (us)  : " << std::endl;
        ss << "  50th          : " << std::setw(16) << quantile(_latencies, quantile_probability = 0.50) << std::endl;
        ss << "  90th          : " << std::setw(16) << quantile(_latencies, quantile_probability = 0.90) << std::endl;
        ss << "  95th          : " << std::setw(16) << quantile(_latencies, quantile_probability = 0.95) << std::endl;
        ss << "  99th          : " << std::setw(16) << quantile(_latencies, quantile_probability = 0.99) << std::endl;
        ss << "  999th         : " << std::setw(16) << quantile(_latencies, quantile_probability = 0.999) << std::endl;
        ss << "  avg           : " << std::setw(16) << mean(_latencies) << std::endl;
        ss << "  max           : " << std::setw(16) << max(_latencies) << std::endl;
        ss << std::endl;
        std::cout << ss.str();
    }
};

class write_parms: public io_parms {
    virtual future<size_t> iofunc(size_t pos) override {
        return _fq.dma_write(pos, _buf.get(), _size, _priority);
    }
    virtual sstring id() override {
        return sprint("Writer-%02d", engine().cpu_id());
    }
    virtual void fill_buffer(char *buf) override {
        std::uniform_int_distribution<char> fill('@', '~');
        memset(buf, fill(random_generator), this->_size);
    }
public:
    write_parms(size_t size, unsigned iodepth, std::chrono::microseconds delay) : io_parms(size, 4096, "write-priority", iodepth, delay) {}

    future<> start(sstring dir) {
        return io_parms::start(std::move(dir));
    }
};

class read_parms: public io_parms {
    virtual future<size_t> iofunc(size_t pos) override {
        return _fq.dma_read(pos, _buf.get(), _size, _priority);
    }

    virtual sstring id() override {
        return sprint("Reader-%02d", engine().cpu_id());
    }
public:
    read_parms(size_t size, unsigned iodepth, std::chrono::microseconds delay) : io_parms(size, 512, "read-priority", iodepth, delay) {}

    future<> start(sstring dir) {
        return io_parms::start(std::move(dir)).then([this] {
            return do_with(size_t(0), [this] (auto& pos) {
                auto bufsz = size_t(128) << 10;
                auto idx = boost::irange(size_t(0), file_size / bufsz);
                return parallel_for_each(idx.begin(), idx.end(), [this, bufsz] (auto idx) {
                    auto pos = idx * bufsz;
                    auto bufptr = allocate_aligned_buffer<char>(bufsz, 4096);
                    auto buf = bufptr.get();
                    std::uniform_int_distribution<char> fill('@', '~');
                    memset(buf, fill(random_generator), bufsz);
                    return _fq.dma_write(pos, buf, bufsz).then([bufsz, b = std::move(bufptr)] (size_t s) {
                        assert(s == bufsz);
                        return make_ready_future<>();
                    });
                });
            });
        });
    }
};

class context {
    sstring _dir;
    read_parms _reader;
    write_parms _writer;

    std::chrono::seconds _duration;
    bool _use_cpu_hog = false;
public:
    context(sstring dir, size_t read_size, size_t write_size, unsigned read_iodepth, unsigned write_iodepth,
            std::chrono::microseconds read_delay, std::chrono::microseconds write_delay, std::chrono::seconds duration, bool use_cpu_hog)
            : _dir(dir)
            , _reader(read_size, read_iodepth, read_delay)
            , _writer(write_size, write_iodepth, write_delay)
            , _duration(duration)
            , _use_cpu_hog(use_cpu_hog)
    {
    }

    future<> stop() { return make_ready_future<>(); }
    future<> start() {
        return _reader.start(_dir).then([this] {
            return _writer.start(_dir);
        });
    }

    future<> do_test() {
        auto test_start = latency_clock::now();

        auto cpu_hog = make_ready_future<>();
        if (_use_cpu_hog) {
            cpu_hog = do_until([this, test_end = test_start + _duration] { return latency_clock::now() >= test_end; }, [this] {
                return make_ready_future<>();
            });
        }
        auto reader = _reader.do_test(test_start, _duration);
        auto writer = _writer.do_test(test_start, _duration);
        return when_all(std::move(reader), std::move(writer), std::move(cpu_hog)).discard_result();
    }

    future<> print_reader_stats() {
        _reader.report_stats();
        return make_ready_future<>();
    }

    future<> print_writer_stats() {
        _writer.report_stats();
        return make_ready_future<>();
    }
};

int main(int ac, char** av) {
    namespace bpo = boost::program_options;

    app_template app;
    auto opt_add = app.add_options();
    opt_add
        ("directory", bpo::value<sstring>()->default_value("."), "directory where to execute the test")
        ("read-iodepth", bpo::value<unsigned>()->default_value(1), "read operation concurrency (iodepth) per shard")
        ("write-iodepth", bpo::value<unsigned>()->default_value(10), "write operation concurrency (iodepth) per shard")
        ("read-size", bpo::value<size_t>()->default_value(4 << 10), "size of each read request")
        ("write-size", bpo::value<size_t>()->default_value(128 << 10), "size of each write request")
        ("delay-between-reads", bpo::value<unsigned>()->default_value(0), "time in microseconds to wait between issuing a new read request")
        ("delay-between-writes", bpo::value<unsigned>()->default_value(0), "time in microseconds to wait between issuing a new write request")
        ("duration", bpo::value<unsigned>()->default_value(60), "duration of the test, in seconds")
        ("cpu-hog", bpo::value<bool>()->default_value(false), "if set to true will add a CPU hog competing for resources in each shard")
    ;


    distributed<context> ctx;
    return app.run(ac, av, [&] {
        auto& opts = app.configuration();
        auto& directory = opts["directory"].as<sstring>();
        auto read_iodepth = opts["read-iodepth"].as<unsigned>();
        auto write_iodepth = opts["write-iodepth"].as<unsigned>();
        auto read_size = opts["read-size"].as<size_t>();
        auto write_size = opts["write-size"].as<size_t>();
        auto read_delay = opts["delay-between-reads"].as<unsigned>() * 1us;
        auto write_delay = opts["delay-between-writes"].as<unsigned>() * 1us;
        auto duration = opts["duration"].as<unsigned>() * 1s;
        auto cpu_hog = opts["cpu-hog"].as<bool>();

        return ctx.start(directory, read_size, write_size, read_iodepth,
                         write_iodepth, read_delay, write_delay, duration, cpu_hog).then([&ctx] {
            engine().at_exit([&ctx] {
                return ctx.stop();
            });
            return ctx.invoke_on_all([] (auto& c) {
                return c.start();
            }).then([&ctx] {
                return ctx.invoke_on_all([] (auto& c) {
                    return c.do_test();
                });
            }).then([&ctx] {
                return ctx.invoke_on_all([] (auto& c) {
                    return c.print_writer_stats();
                });
            }).then([&ctx] {
                return ctx.invoke_on_all([] (auto& c) {
                    return c.print_reader_stats();
                });
            }).or_terminate();
        });
    });
}

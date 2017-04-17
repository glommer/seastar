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
 *
 * The goal of this program is to allow a user to properly configure the Seastar I/O
 * scheduler.
 */
#include <chrono>
#include <random>
#include <memory>
#include <vector>
#include <cmath>
#include <sys/vfs.h>
#include <boost/filesystem.hpp>
#include <boost/range/irange.hpp>
#include <boost/program_options.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <mutex>
#include <deque>
#include <queue>
#include <fstream>
#include <future>
#include "core/sstring.hh"
#include "core/posix.hh"
#include "core/resource.hh"
#include "core/aligned_buffer.hh"
#include "core/sharded.hh"
#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include "util/defer.hh"
#include "util/log.hh"

seastar::logger logger("iotune");
using namespace std::chrono_literals;
using iotune_clock = std::chrono::steady_clock;

bool filesystem_has_good_aio_support(sstring directory, bool verbose);
static thread_local std::default_random_engine random_generator(std::chrono::duration_cast<std::chrono::nanoseconds>(iotune_clock::now().time_since_epoch()).count());

struct directory {
    sstring name;
    file_desc file;
    uint64_t max_iodepth = 128;
    directory(const directory& dir) : name(dir.name)
                                    , file(dir.file.dup())
                                    , max_iodepth(dir.max_iodepth)
    {}
    directory(sstring name) : name(name)
                            , file(file_desc::open(name.c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY))
    {}
};

class test_file {
    sstring _name;
    uint64_t _file_size;
    file _file;
    std::uniform_int_distribution<uint64_t> _pos_distribution;

    float to_gb(auto b) {
        return float(b) / (1ull << 30);
    }

    uint64_t random_position(uint64_t buffer_size) {
        auto max_pos = (_file_size / buffer_size) - 1;
        return buffer_size * (_pos_distribution(random_generator) % max_pos);
    }
public:
    test_file(const directory& dir, uint64_t desired_size);
    future<> start() {
        return open_file_dma(_name, open_flags::rw | open_flags::create).then([this] (auto file) {
            _file = file;
            return make_ready_future<>();
//            return remove_file(_name);
        });
    }

    future<size_t> one_read(char* buf, uint64_t buffer_size) {
        return _file.dma_read(random_position(buffer_size), buf, buffer_size);
    }

    future<> generate(uint64_t buffer_size, std::chrono::seconds timeout) {
        logger.info("Generating evaluation file sized {} GB...", to_gb(_file_size));
        auto start_time = iotune_clock::now();
        return _file.truncate(_file_size).then([this, start_time, buffer_size, timeout] {
            return do_with(uint64_t(0), [this, start_time, buffer_size, timeout] (auto& pos) {
                auto bufptr = allocate_aligned_buffer<char>(buffer_size, 4096);
                auto buf = bufptr.get();
                memset(buf, 0, buffer_size);
                auto stop = [this, test_end = start_time + timeout, desired_size = _file_size] {
                    return (_file_size >= desired_size) || (iotune_clock::now() > test_end);
                };
                _file_size = 0;
                return do_until(std::move(stop), [this, &pos, buffer_size, buf] {
                    auto write_concurrency = 4;
                    auto concurrency = boost::irange<unsigned, unsigned>(0, write_concurrency, 1);
                    return parallel_for_each(concurrency.begin(), concurrency.end(), [this, &pos, buf, buffer_size] (auto idx) {
                        return _file.dma_write(pos + idx*buffer_size, buf, buffer_size).then_wrapped([this, &pos] (auto fut) {
                            if (!fut.failed()) {
                                _file_size += fut.get0();
                            } else {
                                try {
                                    std::rethrow_exception(fut.get_exception());
                                } catch (std::system_error& err) {
                                    if (err.code().value() == ENOSPC) {
                                        // FIXME: The buffer size can be cut short due to other conditions that are unrelated
                                        // to ENOSPC. We should be testing it separately.
                                        logger.warn("stopped early due to disk space issues. Will continue but accuracy may suffer.");
                                    } else {
                                        throw;
                                    }
                                }
                            }
                        });
                    }).then([this, &pos, write_concurrency, buffer_size] {
                        pos += write_concurrency * buffer_size;
                    });
                }).finally([this,alive = std::move(bufptr)] {});
            });
        }).then([this, start_time] {
            auto delta = std::chrono::duration_cast<std::chrono::seconds>(iotune_clock::now() - start_time);
            logger.info("{} GB written in {} seconds", to_gb(_file_size), delta.count());
            return make_ready_future<>();
        });
    }
};

struct run_stats_ordering;

class run_stats {
    uint64_t _concurrency;
    std::vector<std::chrono::microseconds> _latencies;
    std::experimental::optional<iotune_clock::time_point> _eff_start;
    std::experimental::optional<iotune_clock::time_point> _eff_end;

    void maybe_update_times(std::experimental::optional<iotune_clock::time_point> start,
                            std::experimental::optional<iotune_clock::time_point> end) {
        if (start && ((!_eff_start) || (*start < *_eff_start))) {
            _eff_start = *start;
        }
    
        if (end && ((!_eff_end) || (*end > *_eff_end))) {
            _eff_end = *end;
        }
    }
    friend struct run_stats_ordering;
public:
    run_stats(uint64_t conc) :  _concurrency(conc) {
        _latencies.reserve(10 << 20);
    }

    run_stats& operator+=(const struct run_stats& stats) {
        if (stats._concurrency != 0) {
            _concurrency += stats._concurrency;
            maybe_update_times(stats._eff_start, stats._eff_end);
        }
        for (auto&& l: stats._latencies) {
            _latencies.push_back(l);
        }
        std::sort(_latencies.begin(), _latencies.end());
        return *this;
    }
    void add_measure(iotune_clock::time_point start) {
        auto now = iotune_clock::now();
        maybe_update_times(start, now);
        _latencies.push_back(std::chrono::duration_cast<std::chrono::microseconds>(now - start));
    }

    uint64_t concurrency() const {
        return _concurrency;
    }

    uint64_t IOPS() const {
        return _latencies.size() / std::chrono::duration_cast<std::chrono::duration<double>>(*_eff_end - *_eff_start).count();
    }

    std::chrono::microseconds latency_quantile(float q) const {
        if (!_latencies.size()) {
            return 0us;
        }
        auto pos = size_t(_latencies.size() * q);
        return _latencies[pos];
    }
    float average_latency() const {
        if (!_latencies.size()) {
            return 0;
        }
        float x = 0;
        for (auto l: _latencies) {
            x += l.count();
        }
        return x / _latencies.size();
    }

    // FIXME: temporary
    void shrink() {
        _latencies.shrink_to_fit();
    }
};

std::ostream& operator<<(std::ostream& out, const run_stats& r) {
    return out << r.concurrency()
               << ", " << r.IOPS()
               << ", " << uint64_t(r.average_latency())
               << ", " << r.latency_quantile(0.05).count()
               << ", " << r.latency_quantile(0.95).count()
               << ", " << r.latency_quantile(0.99).count()
               << ", " << r.latency_quantile(0.999).count();
}

// Not using operator< inside run_stats, because saying that one run_stats is less than the
// other implies it carries less IOPS.
struct run_stats_ordering {
    bool operator()(const run_stats& lhs, const run_stats& rhs) const {
        return lhs._concurrency < rhs._concurrency;
    }
};

class run_stats_aggregator {
    run_stats aggregated_stats;
public:
    run_stats_aggregator() : aggregated_stats(0) {}

    future<> operator()(const run_stats& value) {
        aggregated_stats += value;
        return make_ready_future<>();
    }
    run_stats get() && noexcept {
        aggregated_stats.shrink();
        return std::move(aggregated_stats);
    }
};

class iotune_shard_context {
    directory _test_directory;
    test_file _test_file;
    std::chrono::seconds _test_duration;
public:
    iotune_shard_context(directory test_directory, uint64_t desired_file_size, std::chrono::seconds timeout)
        : _test_directory(test_directory)
        , _test_file(_test_directory, desired_file_size)
        , _test_duration(timeout)
    {
    }

    future<> stop() {
        return make_ready_future<>();
    }

    future<> write_data() {
        return _test_file.start().then([this] {
            // FIXME: use various buffer sizes so we also generate write statistics
            return _test_file.generate(128 << 10, (_test_duration * 4) / 10);
        });
    }

    future<run_stats> issue_reads(unsigned concurrency, uint64_t buffer_size, iotune_clock::time_point start, iotune_clock::time_point end) {
        auto my_concurrency = concurrency / smp::count;
        if (engine().cpu_id() < (concurrency % smp::count)) {
            my_concurrency++;
        }
        auto stop = [end] { return iotune_clock::now() >= end; };
        auto done = make_lw_shared<unsigned>(0);
        auto stats = make_lw_shared<run_stats>(my_concurrency);

        auto local_concurrency = boost::irange<unsigned, unsigned>(0, my_concurrency, 1);
        return parallel_for_each(local_concurrency.begin(), local_concurrency.end(), [this, buffer_size, start, stop, my_concurrency, done, stats] (auto idx) {
            auto buf = allocate_aligned_buffer<char>(buffer_size, 4096);
            auto cdone = make_lw_shared<bool>(0);
            while (iotune_clock::now() < start);
            return do_until([done, my_concurrency] { return *done == my_concurrency; } , [this, stop, buf = std::move(buf), buffer_size, stats, done, cdone, my_concurrency] {
                auto req_start = iotune_clock::now();
                return _test_file.one_read(buf.get(), buffer_size).then([this, stop, done, cdone, my_concurrency, stats, req_start] (auto size) {
                    if (!stop()) {
                        stats->add_measure(req_start);
                    } else if (!*cdone) {
                        *cdone = true;
                        (*done)++;
                    }
                    return make_ready_future<>();
                });
            });
        }).then([stats] {
            return make_ready_future<run_stats>(std::move(*stats));
        });
    }
};

class iotune_manager {
    directory _test_directory;
    seastar::sharded<iotune_shard_context> _iotune_shard_context;

    using run_results = std::set<run_stats, run_stats_ordering>;
    std::map<uint64_t, run_results> _raw_results;
    std::vector<float> _percentiles_of_interest = {0.5, 0.70, 0.80, 0.95};

    struct eval_result {
        float percentile;
        unsigned concurrency;
        uint64_t IOPS;
    };
    std::map<uint64_t, eval_result> _eval_results;

    struct run_params {
        std::chrono::milliseconds duration;
        uint64_t buffer_size;
    };

    template <typename Range>
    future<> explore_range(Range&& range, run_params params) {
        return do_for_each(range, [this, params = std::move(params)] (auto concurrency) {
            auto start = iotune_clock::now() + 1ms;
            auto end = start + params.duration;
            uint64_t buffer_size = params.buffer_size;
            return _iotune_shard_context.map_reduce(run_stats_aggregator(), &iotune_shard_context::issue_reads,
                    std::move(concurrency), std::move(buffer_size), std::move(start), std::move(end)).then([this, params] (auto&& r) {
                std::cout << "RESULT:" <<params.buffer_size << ":" << r << std::endl;
              //  logger.debug("buffer size {} bytes: {}", params.buffer_size, r);
                _raw_results.at(params.buffer_size).emplace(std::move(r));
                return make_ready_future<>();
            });
        });
    }

    const run_stats& find_closest(uint64_t buffer_size, uint64_t IOPS_goal) const {
        auto best_delta = std::numeric_limits<uint64_t>::max();
        const run_stats* best_result = &(*_raw_results.at(buffer_size).begin());

        for (auto& m : _raw_results.at(buffer_size)) {
            uint64_t d = std::abs(int64_t(IOPS_goal - m.IOPS()));
            if (d < best_delta) {
                best_delta = d;
                best_result = &m;
            }
        }
        return *best_result;
    }

    const run_stats& find_max(uint64_t buffer_size) const {
        return find_closest(buffer_size, std::numeric_limits<int64_t>::max());
    }

    const run_stats& find_first(uint64_t buffer_size, uint64_t IOPS_goal) const {
        for (auto& m : _raw_results.at(buffer_size)) {
            if (m.IOPS() > IOPS_goal) {
                return m;
            }
        }
        return *(_raw_results.at(buffer_size).begin());
    }

    auto range_around(unsigned point) const {
        auto min = point > 4 ? point - 4 : 1;
        auto max = point + 4;
        return boost::irange<unsigned, unsigned>(min, max, 1);
    }

    auto range_around(const run_stats& rs) const {
        return range_around(rs.concurrency());
    }

    unsigned max_concurrency() const {
        return std::min({ 512ul, smp::count * reactor::max_aio, _test_directory.max_iodepth });
    }
public:
    future<> stop() {
        return _iotune_shard_context.stop();
    }

    template <typename... Args>
    future<> start(Args... args) {
        // Instead of waiting for ENOSPC to happen, we'll see how much the filesystem handle.
        // Relying on ENOSPC could cause files in different shards to be wildly different in size
        struct ::statfs buf;
        auto r = ::statfs(_test_directory.name.c_str(), &buf);
        auto desired_size = uint64_t(100) << 30;
        if (r == 0) {
            auto max_size = buf.f_bavail * buf.f_bsize;
            desired_size = std::min(desired_size, uint64_t(0.50 * max_size));
        }
        return _iotune_shard_context.start(_test_directory, desired_size / smp::count, std::forward<Args>(args)...);
    }
    // This should take around a minute
    future<> measure_reads(uint64_t buffer_size) {
        run_results empty;
        empty.emplace(0);
        _raw_results.emplace(buffer_size, std::move(empty));

        run_params params{250ms, buffer_size};
        return explore_range(boost::irange<unsigned, unsigned>(1, max_concurrency(), 1), std::move(params));
#if 0 
        // Should take 512 / 4 * 250ms = ~ 32s
        return explore_range(boost::irange<unsigned, unsigned>(1, max_concurrency(), 1), std::move(params)).then([this, buffer_size] {
            run_params params{1000ms, buffer_size};
            auto& best_result = find_max(buffer_size);
            return explore_range(range_around(best_result), std::move(params));
        }).then([this, buffer_size] {
            auto& refined_best = find_max(buffer_size);
            logger.debug("{} buffers: Maximum READ IOPS {} at concurrency of {}", buffer_size, refined_best.IOPS(), refined_best.concurrency());
        
            std::set<unsigned> explorer;
            for (auto&& p : _percentiles_of_interest) {
                for (auto&& r: range_around(find_first(buffer_size, uint64_t(p * refined_best.IOPS())))) {
                    explorer.insert(r);
                }
            }
        
            // Should take 5 * 8 * 1s = 40s
            run_params params{1000ms, buffer_size};
            return explore_range(explorer, params);
        });
#endif
    }

    // FIXME: Generate proper YAML
    void show_config() {
        for (auto&& s: _raw_results) {
            std::cout <<  s.first << "kB:" << std::endl;    
        for (auto& r: s.second) {
                std::cout << r.concurrency() << ", " << r.IOPS() << std::endl;    
        }
    }

        std::cout << "reads:" << std::endl;
        for (auto&& s: _raw_results) {
            std::cout << "\t" << s.first << "kB:" << std::endl;    
            auto& max = find_max(s.first);
            for (auto&& p: _percentiles_of_interest) {
                auto& r = find_first(s.first, uint64_t(p * max.IOPS()));
                std::cout << "\t\t" << "IOPS(" << r.concurrency() << ")=" << r.IOPS() << std::endl;    
            }
            std::cout << "\t\t" << "IOPS(" << max.concurrency() << ")=" << max.IOPS() << std::endl;    
        }
    }

    future<> write_data() {
        return _iotune_shard_context.invoke_on_all([] (auto& isc) {
            return isc.write_data();
        });
    }

    iotune_manager(sstring dir) : _test_directory(dir) {}
};

test_file::test_file(const directory& dir, uint64_t desired_size)
    : _name(dir.name + "/ioqueue-discovery-" + to_sstring(engine().cpu_id()))
    , _file_size(desired_size)
    , _pos_distribution(0, std::numeric_limits<uint64_t>::max()) {}


int do_fsqual(sstring directory) {
    struct do_not_disturb_fsqual {
        sigset_t blockall, old;
        do_not_disturb_fsqual() {
            sigfillset(&blockall);
            auto r = ::pthread_sigmask(SIG_SETMASK, &blockall, &old);
            throw_kernel_error(r);
        }
        ~do_not_disturb_fsqual() {
            auto r = ::pthread_sigmask(SIG_SETMASK, &old, nullptr);
            throw_kernel_error(r);
        }
    };

    do_not_disturb_fsqual guard;
    if (!filesystem_has_good_aio_support(directory, false)) {
        logger.error("File system on {} is not qualified for seastar AIO;"
                " see http://docs.scylladb.com/kb/kb-fs-not-qualified-aio/ for details", directory);
        return 1;
    }
    return 0;
}

struct option_file_format_type {
    sstring value;
    explicit option_file_format_type(sstring f) : value(f) {}
};

std::ostream& operator<<(std::ostream& out, option_file_format_type offt) {
    return out << offt.value;
}

void validate(boost::any& v, const std::vector<std::string>& values,
              option_file_format_type* target_type, int) {
    using namespace boost::program_options;

    validators::check_first_occurrence(v);
    auto&& format = validators::get_single_string(values);
    if (format != "seastar" && format != "envfile") {
        throw validation_error(validation_error::invalid_option_value);
    }
    v = option_file_format_type(format);
}

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    bool fs_check = false;

    app_template app(app_template::config{"IOTune"});
    auto opt_add = app.add_options();
    opt_add
        ("evaluation-directory", bpo::value<sstring>()->required(), "directory where to execute the evaluation")
        ("options-file", bpo::value<sstring>()->default_value("~/.config/seastar/io.conf"), "Output configuration file")
        ("format", bpo::value<option_file_format_type>()->default_value(option_file_format_type("seastar")), "Configuration file format (seastar | envfile)")
        ("timeout", bpo::value<uint64_t>()->default_value(60 * 6), "Maximum time to wait for iotune to finish (seconds)")
        ("fs-check", bpo::bool_switch(&fs_check), "perform FS check only")
    ;

    return app.run(ac, av, [&] {
        auto& configuration = app.configuration();
        auto format = configuration["format"].as<option_file_format_type>().value;
        auto conf_file = configuration["options-file"].as<sstring>();
        auto directory = configuration["evaluation-directory"].as<sstring>();
        auto timeout = std::chrono::seconds(configuration["timeout"].as<uint64_t>());

        try {
            auto fsqual = do_fsqual(directory);
            if (fs_check || fsqual) {
                return make_ready_future<int>(fsqual);
            }
        } catch (std::exception& e) {
            logger.error("Exception when qualifying filesystem at {}", directory);
            return make_ready_future<int>(1);
        }

        auto iotune_manager = make_lw_shared<::iotune_manager>(directory);
        return iotune_manager->start(timeout).then([iotune_manager] {
            engine().at_exit([iotune_manager] {
                return iotune_manager->stop();
            });
        }).then([iotune_manager] {
            logger.info("Starting writes");
            return iotune_manager->write_data();
        }).then([iotune_manager] {
            logger.info("Starting reads");
            return do_with(std::vector<uint64_t>(), [iotune_manager] (auto& sizes) {
                sizes.push_back(1 << 10);
                while (sizes.back() <= (1 << 20)) {
                    auto next = sizes.back() << 1;
                    sizes.push_back(next);
                }
                return do_for_each(sizes, [iotune_manager] (auto buf_size) {
                    logger.info("Evaluating {} reads", buf_size);
                    return iotune_manager->measure_reads(buf_size);
                }).then([iotune_manager] {
//                    iotune_manager->show_config();
                    return make_ready_future<>();
                });
            });
        }).then([] {
            return make_ready_future<int>(0);
        });
    });
}

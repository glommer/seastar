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

struct directory {
    sstring name;
    file_desc file;
    directory(sstring name) : name(name)
                            , file(file_desc::open(name.c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY))
    {}
};

class test_file {
    sstring _name;
    uint64_t _file_size;
    file _file;

    float to_gb(auto b) {
        return float(b) / (1ull << 30);
    }
public:
    test_file(const directory& dir, uint64_t desired_size);
    future<> start() {
        return open_file_dma(_name, open_flags::rw | open_flags::create | open_flags::exclusive).then([this] (auto file) {
            _file = file;
            return remove_file(_name);
        });
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

class iotune_shard_context {
    test_file _test_file;
    std::chrono::seconds _test_duration;
public:
    iotune_shard_context(sstring dirname, uint64_t desired_file_size, std::chrono::seconds timeout)
        : _test_file(directory(dirname), desired_file_size)
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
};

class iotune_manager {
    sstring _dirname;
    seastar::sharded<iotune_shard_context> _iotune_shard_context;
public:
    future<> stop() {
        return _iotune_shard_context.stop();
    }

    template <typename... Args>
    future<> start(Args... args) {
        // Instead of waiting for ENOSPC to happen, we'll see how much the filesystem handle.
        // Relying on ENOSPC could cause files in different shards to be wildly different in size
        struct ::statfs buf;
        auto r = ::statfs(_dirname.c_str(), &buf);
        auto desired_size = uint64_t(100) << 30;
        if (r == 0) {
            auto max_size = buf.f_bavail * buf.f_bsize;
            desired_size = std::min(desired_size, uint64_t(0.60 * max_size));
        }
        return _iotune_shard_context.start(_dirname, desired_size / smp::count, std::forward<Args>(args)...);
    }
    future<> write_data() {
        return _iotune_shard_context.invoke_on_all([] (auto& isc) {
            return isc.write_data();
        });
    }

    iotune_manager(sstring dir) : _dirname(dir) {}
};

test_file::test_file(const directory& dir, uint64_t desired_size)
    : _name(dir.name + "/ioqueue-discovery-" + to_sstring(engine().cpu_id()))
    , _file_size(desired_size) {}


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

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    bool fs_check = false;

    app_template app(app_template::config{"IOTune"});
    auto opt_add = app.add_options();
    opt_add
        ("evaluation-directory", bpo::value<sstring>()->required(), "directory where to execute the evaluation")
        ("timeout", bpo::value<uint64_t>()->default_value(60 * 6), "Maximum time to wait for iotune to finish (seconds)")
        ("fs-check", bpo::bool_switch(&fs_check), "perform FS check only")
    ;

    return app.run(ac, av, [&] {
        auto& configuration = app.configuration();
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
        }).then([] {
            return make_ready_future<int>(0);
        });
    });
}

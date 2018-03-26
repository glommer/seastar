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
#include "core/thread.hh"
#include <chrono>
#include <vector>
#include <boost/range/irange.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/range/adaptor/filtered.hpp>
#include <boost/range/adaptor/map.hpp>
#include <boost/array.hpp>
#include <iomanip>
#include <random>

using namespace seastar;
using namespace std::chrono_literals;

static auto random_seed = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
static std::default_random_engine random_generator(random_seed);

/// Each shard has one context, and the context is responsible for creating the classes that should
/// run in this shard.
class context {
    sstring _dir;
    std::vector<file> _file;
    size_t _written = 0;
    size_t _pos = 0;
    static constexpr unsigned parallelism = 16; // per shard
public:
    context(sstring dir)
        : _dir(dir)
    {}

    future<> stop() { return make_ready_future<>(); }

    future<> create_file() {
        return seastar::async([this] {
            for (auto i = 0u; i < parallelism; ++i) {
                auto fname = sprint("%s/test-tpf-%d-%d", _dir, engine().cpu_id(), i);
                auto f = open_file_dma(fname, open_flags::rw | open_flags::create | open_flags::truncate).get0();
                remove_file(fname).get();
                f.truncate(512ull << 20);
                _file.push_back(std::move(f)); 
            }
        });
    }

    future<size_t> issue_writes(std::chrono::steady_clock::time_point issue_start, unsigned bufsize) {
        _written = 0;
        auto start = issue_start + 1s;
        auto stop = issue_start + 61s;
        auto req = boost::irange(0u, parallelism);
        return parallel_for_each(req, [this, start, stop, bufsize] (auto idx) {
            auto pos = std::make_unique<size_t>(0);
            auto bufptr = allocate_aligned_buffer<char>(bufsize, 4096);
            auto buf = bufptr.get();
            std::uniform_int_distribution<char> fill('@', '~');
            memset(buf, fill(random_generator), bufsize);
            return do_until([start, stop] { return std::chrono::steady_clock::now() >= stop; }, [this, buf, start, stop, bufsize, idx, pos = pos.get()] () mutable {
                return _file[idx].dma_write(*pos, buf, bufsize).then([this, start, stop, pos] (size_t bytes) mutable {
                    *pos += bytes;
                    if ((std::chrono::steady_clock::now() >= start) && (std::chrono::steady_clock::now() <= stop)) {
                        _written += bytes;
                    }
                });
            }).finally([pos = std::move(pos), bufptr = std::move(bufptr)] {});
        }).then([this] {
            return make_ready_future<size_t>(_written);
        }).finally([this] {
            _file = {};
        });
    }
};

int main(int ac, char** av) {
    namespace bpo = boost::program_options;

    app_template app;
    auto opt_add = app.add_options();
    opt_add
        ("directory", bpo::value<sstring>()->default_value("."), "directory where to execute the test")
    ;

    distributed<context> ctx;
    return app.run(ac, av, [&] {
        auto& opts = app.configuration();
        auto& directory = opts["directory"].as<sstring>();

        return file_system_at(directory).then([directory] (auto fs) {
            if (fs != fs_type::xfs) {
                throw std::runtime_error(sprint("This is a performance test. %s is not on XFS", directory));
            }
        }).then([&] {
            return ctx.start(directory).then([&ctx] {
                engine().at_exit([&ctx] {
                    return ctx.stop();
                });
            }).then([&ctx] {
                return do_with(std::vector<unsigned>({4 << 10, 8 << 10, 16 << 10, 32 << 10, 64 << 10, 128 << 10, 256 << 10, 512 << 10, 1 << 20}), [&ctx] (auto& sizes) { 
                    return do_for_each(sizes, [&ctx] (auto size) {
                        auto start = std::chrono::steady_clock::now();
                        return ctx.map_reduce0([size, start] (auto& instance) {
                            return instance.create_file().then([start, size, &instance] {
                                return instance.issue_writes(start, size);
                            });
                        }, size_t(0), std::plus<size_t>()).then([size] (auto tput) {
                            tput = (tput >> 20) / 60;
                            std::cout << (size >> 10) << " kB : " << tput << " MB/s" << std::endl;
                        });
                    });
                });
            });
        }).or_terminate();
    });
}

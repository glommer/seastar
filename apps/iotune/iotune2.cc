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
#include "util/defer.hh"
#include "util/log.hh"

seastar::logger logger("iotune");
using namespace std::chrono_literals;

bool filesystem_has_good_aio_support(sstring directory, bool verbose);

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

        auto fsqual = do_fsqual(directory);
        if (fs_check || fsqual) {
            return make_ready_future<int>(fsqual);
        }
        return make_ready_future<int>(0);
    });
}

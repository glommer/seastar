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
 * Copyright (C) 2018 ScyllaDB
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
#include <sys/sysmacros.h>
#include <boost/filesystem.hpp>
#include <boost/range/irange.hpp>
#include <boost/program_options.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <fstream>
#include <experimental/filesystem>
#include <wordexp.h>
#include <yaml-cpp/yaml.h>
#include "core/thread.hh"
#include "core/sstring.hh"
#include "core/posix.hh"
#include "core/resource.hh"
#include "core/aligned_buffer.hh"
#include "core/sharded.hh"
#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include "core/fsqual.hh"
#include "util/defer.hh"
#include "util/log.hh"

using namespace seastar;
using namespace std::chrono_literals;
namespace fs = std::experimental::filesystem;

logger iotune_logger("iotune");

using iotune_clock = std::chrono::steady_clock;
static thread_local std::default_random_engine random_generator(std::chrono::duration_cast<std::chrono::nanoseconds>(iotune_clock::now().time_since_epoch()).count());

inline
sstring read_sys_file(fs::path sys_file, size_t expected_size = 64) {
    auto file = file_desc::open(sys_file.string(), O_RDONLY | O_CLOEXEC);
    sstring buf;
    size_t n = 0;
    do {
        // try to avoid allocations
        sstring tmp(sstring::initialized_later{}, 8);
        auto ret = file.read(tmp.data(), 8ul);
        if (!ret) { // EAGAIN
            continue;
        }
        n = *ret;
        if (n > 0) {
            buf += tmp;
        }
    } while (n != 0);
    auto end = buf.find('\n');
    auto value = buf.substr(0, end);
    file.close();
    return value;
}

inline
void check_device_properties(fs::path dev_sys_file) {
    auto sched_file = dev_sys_file / fs::path("queue/scheduler");
    auto sched_string = read_sys_file(sched_file);
    auto beg = sched_string.find('[');
    auto end = sched_string.find(']');
    auto scheduler = sched_string.substr(beg+1, end-1);
    if ((scheduler != "noop") && (scheduler != "none")) {
        iotune_logger.warn("Scheduler for {} set to {}. It is recommend to set it to noop before evaluation so as not to skew the results.",
                sched_file.string(), scheduler);
    }

    auto nomerges_file = dev_sys_file / fs::path("queue/nomerges");
    auto nomerges = boost::lexical_cast<unsigned>(read_sys_file(nomerges_file));
    if (nomerges != 2) {
        iotune_logger.warn("nomerges for {} set to {}. It is recommend to set it to 2 before evaluation so that merges are disabled. Results can be skewed otherwise.",
                nomerges_file.string(), nomerges);
    }
}

struct directory {
    sstring _name;
    // We know that if we issue more than this, they will be blocked on linux anyway.
    unsigned _max_iodepth = 0;
    uint64_t _available_space;
    uint64_t _max_data_transfer_size = 100 << 20;
    uint64_t _min_data_transfer_size = 512;
    unsigned _disks_per_array = 0;

    void scan_device(unsigned dev_maj) {
        try {
            auto sys_file = fs::path("/sys/dev/block") / fs::path(fmt::format("{}:0", dev_maj));
            bool is_leaf = true;
            for (auto& dev : fs::directory_iterator(sys_file / fs::path("slaves"))) {
                is_leaf = false;
                auto slave_dev = read_sys_file(dev / fs::path("dev"));
                auto end = slave_dev.find(':');
                auto dev_maj = boost::lexical_cast<unsigned>(slave_dev.substr(0, end));
                scan_device(dev_maj);
            }
            if (is_leaf) {
                check_device_properties(sys_file);
                auto disk_max_transfer_size = boost::lexical_cast<uint64_t>(read_sys_file(sys_file / fs::path("queue/max_hw_sectors_kb"))) * 1024;
                auto disk_min_io_size = boost::lexical_cast<uint64_t>(read_sys_file(sys_file / fs::path("queue/minimum_io_size")));

                _max_data_transfer_size = std::min(_max_data_transfer_size, disk_max_transfer_size);
                _min_data_transfer_size = std::max(_min_data_transfer_size, disk_min_io_size);
                _max_iodepth += boost::lexical_cast<unsigned>(read_sys_file(sys_file / fs::path("queue/nr_requests")));
                _disks_per_array++;

            }
        } catch (fs::filesystem_error& fse) {
            iotune_logger.error("Error while parsing sysfs. Will continue with guessed values: {}", fse.what());
            _max_iodepth = 128;
        }
        _disks_per_array = std::max(_disks_per_array, 1u);
        // Cap this to 2MB to avoid super large allocations down the OS stack.
        _max_data_transfer_size = std::min(_max_data_transfer_size * _disks_per_array, uint64_t(2) << 20);
    }
public:
    directory(sstring name)
        : _name(name)
        , _available_space(fs::space(fs::path(_name)).available)
    {}

    unsigned max_iodepth() const {
        return _max_iodepth;
    }

    fs::path path() const {
        return fs::path(_name);
    }

    const sstring& name() const {
        return _name;
    }

    uint64_t maximum_io_size() const {
        return _max_data_transfer_size;
    }

    unsigned disks_per_array() const {
        return _disks_per_array;
    }

    uint64_t minimum_io_size() const {
        return _min_data_transfer_size;
    }

    future<> discover_directory() {
        return seastar::async([this] {
            auto f = open_directory(_name).get0();
            auto st = f.stat().get0();
            f.close().get();

            auto dev_maj = major(st.st_dev);
            scan_device(dev_maj);
        });
    }

    uint64_t available_space() const {
        return _available_space;
    }
};

struct io_rates {
    float bytes_per_sec;
    float iops;
    io_rates operator+(const io_rates& a) const {
        return io_rates{bytes_per_sec + a.bytes_per_sec, iops + a.iops};
    }

    io_rates& operator+=(const io_rates& a) {
        bytes_per_sec += a.bytes_per_sec;
        iops += a.iops;
        return *this;
    }
};

class invalid_position : public std::exception {
public:
    virtual const char* what() const noexcept {
        return "file access position invalid";
    }
};

struct position_generator {
    virtual uint64_t get_pos() = 0;
    virtual bool is_sequential() const = 0;
};

class sequential_issuer : public position_generator {
    size_t _buffer_size;
    uint64_t _position = 0;
    uint64_t _size_limit;
public:
    sequential_issuer(size_t buffer_size, uint64_t size_limit)
        : _buffer_size(buffer_size)
        , _size_limit(size_limit)
    {}

    virtual bool is_sequential() const {
        return true;
    }

    virtual uint64_t get_pos() {
        if (_position >= _size_limit) {
            throw invalid_position();
        }
        auto pos = _position;
        _position += _buffer_size;
        return pos;
    }
};

class random_issuer : public position_generator {
    size_t _buffer_size;
    uint64_t _last_position;
    std::uniform_int_distribution<uint64_t> _pos_distribution;
public:
    random_issuer(size_t buffer_size, uint64_t last_position)
        : _buffer_size(buffer_size)
        , _last_position(last_position)
        , _pos_distribution(0, (last_position / buffer_size) - 1)
    {}

    virtual bool is_sequential() const {
        return false;
    }

    virtual uint64_t get_pos() {
        uint64_t pos = _pos_distribution(random_generator) * _buffer_size;
        if (pos >= _last_position) {
            throw invalid_position();
        }
        return pos;
    }
};

class request_issuer {
public:
    virtual future<size_t> issue_request(uint64_t pos, char* buf, uint64_t size) = 0;
};


class write_request_issuer : public request_issuer {
    file _file;
public:
    write_request_issuer(file f) : _file(f) {}
    future<size_t> issue_request(uint64_t pos, char* buf, uint64_t size) override {
        return _file.dma_write(pos, buf, size);
    }
};

class read_request_issuer : public request_issuer {
    file _file;
public:
    read_request_issuer(file f) : _file(f) {}
    future<size_t> issue_request(uint64_t pos, char* buf, uint64_t size) override {
        return _file.dma_read(pos, buf, size);
    }
};

class io_worker {
    uint64_t _bytes = 0;
    unsigned _requests = 0;
    size_t _buffer_size;
    std::unique_ptr<position_generator> _pos_impl;
    std::unique_ptr<request_issuer> _req_impl;
public:
    bool is_sequential() const {
        return _pos_impl->is_sequential();
    }

    io_worker(size_t buffer_size, std::unique_ptr<request_issuer> reqs, std::unique_ptr<position_generator> pos)
        : _buffer_size(buffer_size)
        , _pos_impl(std::move(pos))
        , _req_impl(std::move(reqs))
    {}

    std::unique_ptr<char[], free_deleter> get_buffer() {
        return allocate_aligned_buffer<char>(_buffer_size, _buffer_size);
    }

    future<> issue_request(char* buf) {
        return _req_impl->issue_request(_pos_impl->get_pos(), buf, _buffer_size).then([this] (size_t size) {
            _bytes += size;
            _requests++;
        });
    }

    uint64_t bytes() const {
        return _bytes;
    }

    uint64_t requests() const {
        return _requests;
    }
};

class test_file {
public:
    enum class pattern { sequential, random };
private:
    fs::path _dirpath;
    uint64_t _file_size;
    file _file;

    std::unique_ptr<position_generator> get_position_generator(size_t buffer_size, pattern access_pattern) {
        if (access_pattern == pattern::sequential) {
            return std::make_unique<sequential_issuer>(buffer_size, _file_size);
        } else {
            return std::make_unique<random_issuer>(buffer_size, _file_size);
        }
    }
public:
    test_file(const directory& dir, uint64_t maximum_size)
        : _dirpath(dir.path() / fs::path(fmt::format("ioqueue-discovery-{}", engine().cpu_id())))
        , _file_size(maximum_size)
    {}

    future<> create_data_file() {
        // XFS likes access in many directories better.
        return make_directory(_dirpath.string()).then([this] {
            auto testfile = _dirpath / fs::path("testfile");
            return open_file_dma(testfile.string(), open_flags::rw | open_flags::create).then([this, testfile] (auto file) {
                _file = file;
                return remove_file(testfile.string()).then([this] {
                    return remove_file(_dirpath.string());
                });
            }).then([this] {
                return _file.truncate(_file_size);
            });
        });
    }

    future<io_rates> do_workload(std::unique_ptr<io_worker> worker_ptr, unsigned max_os_concurrency, std::chrono::duration<double> duration, bool update_file_size = false) {
        if (update_file_size) {
            _file_size = 0;
        }

        auto worker = worker_ptr.get();
        auto start = iotune_clock::now();
        auto stop = [end = start + duration] { return iotune_clock::now() >= end; };
        auto concurrency = boost::irange<unsigned, unsigned>(0, max_os_concurrency, 1);
        return parallel_for_each(std::move(concurrency), [this, stop, worker] (unsigned idx) {
            auto bufptr = worker->get_buffer();
            auto buf = bufptr.get();
            return do_until(std::move(stop), [this, buf, worker, idx] {
                return worker->issue_request(buf);
            }).finally([this, alive = std::move(bufptr)] {});
        }).then_wrapped([this, worker = std::move(worker_ptr), update_file_size, start] (future<> f) {
            auto t = std::chrono::duration_cast<std::chrono::duration<double>>(iotune_clock::now() - start);
            try {
                f.get();
            } catch (invalid_position& ip) {
                // expected if sequential. Example: reading and the file ended.
                if (!worker->is_sequential()) {
                    throw;
                }
            }

            if (update_file_size) {
                _file_size = worker->bytes();
            }
            io_rates rates;
            rates.bytes_per_sec = worker->bytes() / t.count();
            rates.iops = worker->requests() / t.count();
            return make_ready_future<io_rates>(std::move(rates));
        });
    }

    future<io_rates> read_workload(size_t buffer_size, pattern access_pattern, unsigned max_os_concurrency, std::chrono::duration<double> duration) {
        buffer_size = std::max(buffer_size, _file.disk_read_dma_alignment());
        std::cout.flush();
        auto worker = std::make_unique<io_worker>(buffer_size, std::make_unique<read_request_issuer>(_file), get_position_generator(buffer_size, access_pattern));
        return do_workload(std::move(worker), max_os_concurrency, duration);
    }

    future<io_rates> write_workload(size_t buffer_size, pattern access_pattern, unsigned max_os_concurrency, std::chrono::duration<double> duration) {
        buffer_size = std::max(buffer_size, _file.disk_write_dma_alignment());
        std::cout.flush();
        auto worker = std::make_unique<io_worker>(buffer_size, std::make_unique<write_request_issuer>(_file), get_position_generator(buffer_size, access_pattern));
        bool update_file_size = worker->is_sequential();
        return do_workload(std::move(worker), max_os_concurrency, duration, update_file_size).then([this] (io_rates r) {
            return _file.flush().then([r = std::move(r)] () mutable {
                return make_ready_future<io_rates>(std::move(r));
            });
        });
    }

    future<> stop() {
        return make_ready_future<>();
    }
};

class iotune_multi_shard_context {
    directory _test_directory;

    unsigned per_shard_io_depth() const {
        auto iodepth = _test_directory.max_iodepth() / smp::count;
        if (engine().cpu_id() < _test_directory.max_iodepth() % smp::count) {
            iodepth++;
        }
        return std::min(iodepth, 128u);
    }
    seastar::sharded<test_file> _iotune_test_file;
public:
    future<> stop() {
        return _iotune_test_file.stop();
    }

    future<> start() {
       return _iotune_test_file.start(_test_directory, _test_directory.available_space() / (2 * smp::count));
    }

    future<> create_data_file() {
        return _iotune_test_file.invoke_on_all([this] (test_file& tf) {
            return tf.create_data_file();
        });
    }

    future<io_rates> write_sequential_data(unsigned shard, size_t buffer_size) {
        return _iotune_test_file.invoke_on(shard, [this, buffer_size] (test_file& tf) {
            auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(30s) / smp::count;
            return tf.write_workload(buffer_size, test_file::pattern::sequential, 4 * _test_directory.disks_per_array(), duration);
        });
    }

    future<io_rates> read_sequential_data(unsigned shard, size_t buffer_size) {
        return _iotune_test_file.invoke_on(shard, [this, buffer_size] (test_file& tf) {
            auto duration = std::chrono::duration_cast<std::chrono::duration<double>>(5s) / smp::count;
            return tf.read_workload(buffer_size, test_file::pattern::sequential, 4 * _test_directory.disks_per_array(), duration);
        });
    }

    future<io_rates> write_random_data(size_t buffer_size) {
        return _iotune_test_file.map_reduce0([buffer_size, this] (test_file& tf) {
            return tf.write_workload(buffer_size, test_file::pattern::random, per_shard_io_depth(), 5s);
        }, io_rates(), std::plus<io_rates>());
    }

    future<io_rates> read_random_data(size_t buffer_size) {
        return _iotune_test_file.map_reduce0([buffer_size, this] (test_file& tf) {
            return tf.read_workload(buffer_size, test_file::pattern::random, per_shard_io_depth(), 5s);
        }, io_rates(), std::plus<io_rates>());
    }

    iotune_multi_shard_context(directory dir)
        : _test_directory(dir)
    {}
};

void write_configuration_file(sstring conf_file, std::string format, sstring properties_file, unsigned num_io_queues) {
    auto f = file_desc::open(conf_file, O_WRONLY | O_CLOEXEC | O_CREAT | O_TRUNC);

    sstring buf;
    if (format == "seastar") {
        buf = fmt::format("num-io-queues={}\nio-properties-file={}\n",
                num_io_queues, properties_file);
    } else {
        buf = fmt::format("SEASTAR_IO=\"--num-io-queues={} --io-properties-file={}\"\n",
                num_io_queues, properties_file);
    }

    auto ret = f.write(buf.data(), buf.size());
    if (!ret || (*ret != buf.size())) {
        throw std::runtime_error(fmt::format("Can't write {}: {}", conf_file, *ret));
    }
}

struct disk_descriptor {
    std::string mountpoint;
    uint64_t read_iops;
    uint64_t read_bw;
    uint64_t write_iops;
    uint64_t write_bw;
};

void write_property_file(sstring conf_file, struct disk_descriptor desc) {
    YAML::Emitter out;
    out << YAML::BeginMap;
    out << YAML::Key << "disks";
    out << YAML::BeginSeq;
    out << YAML::BeginMap;
    out << YAML::Key << "mountpoint" << YAML::Value << desc.mountpoint;
    out << YAML::Key << "read_iops" << YAML::Value << desc.read_iops;
    out << YAML::Key << "read_bandwidth" << YAML::Value << desc.read_bw;
    out << YAML::Key << "write_iops" << YAML::Value << desc.write_iops;
    out << YAML::Key << "write_bandwdith" << YAML::Value << desc.write_bw;
    out << YAML::EndMap;
    out << YAML::EndSeq;
    out << YAML::EndMap;
    out << YAML::Newline;

    auto f = file_desc::open(conf_file, O_WRONLY | O_CLOEXEC | O_CREAT | O_TRUNC);
    auto ret = f.write(out.c_str(), out.size());
    if (!ret || (*ret != out.size())) {
        throw std::runtime_error(fmt::format("Can't write {}: {}", conf_file, *ret));
    }
}

int main(int ac, char** av) {
    namespace bpo = boost::program_options;
    bool fs_check = false;

    app_template::config app_cfg;
    app_cfg.name = "IOTune";

    app_template app(std::move(app_cfg));
    auto opt_add = app.add_options();
    opt_add
        ("evaluation-directory", bpo::value<sstring>()->required(), "directory where to execute the evaluation")
        ("properties-file", bpo::value<sstring>(), "path in which to write the YAML file")
        ("options-file", bpo::value<sstring>(), "path in which to write the legacy conf file")
        ("format", bpo::value<sstring>()->default_value("seastar"), "Configuration file format (seastar | envfile)")
        ("fs-check", bpo::bool_switch(&fs_check), "perform FS check only")
    ;

    return app.run(ac, av, [&] {
        return seastar::async([&] {
            auto& configuration = app.configuration();
            auto eval_dir = configuration["evaluation-directory"].as<sstring>();
            auto format = configuration["format"].as<sstring>();

            if (filesystem_has_good_aio_support(eval_dir, false) == false) {
                iotune_logger.error("Exception when qualifying filesystem at {}", eval_dir);
                return 1;
            }

            // Directory is the same object for all tests.
            directory test_directory(eval_dir);
            test_directory.discover_directory().get();

            ::iotune_multi_shard_context iotune_tests(test_directory);
            iotune_tests.start().get();
            iotune_tests.create_data_file().get();

            auto stop = defer([&iotune_tests] {
                iotune_tests.stop().get();
            });

            fmt::print("Measuring sequential write bandwidth: ");
            std::cout.flush();
            io_rates write_bw;
            for (unsigned shard = 0; shard < smp::count; ++shard) {
                write_bw += iotune_tests.write_sequential_data(shard, test_directory.maximum_io_size()).get0();
            }
            write_bw.bytes_per_sec /= smp::count;
            fmt::print("{} MB/s\n", uint64_t(write_bw.bytes_per_sec / (1024 * 1024)));

            fmt::print("Measuring sequential read bandwidth: ");
            std::cout.flush();
            auto read_bw = iotune_tests.read_sequential_data(0, test_directory.maximum_io_size()).get0();
            fmt::print("{} MB/s\n", uint64_t(read_bw.bytes_per_sec / (1024 * 1024)));

            fmt::print("Measuring random write IOPS: ");
            std::cout.flush();
            auto write_iops = iotune_tests.write_random_data(test_directory.minimum_io_size()).get0();
            fmt::print("{} IOPS\n", uint64_t(write_iops.iops));

            fmt::print("Measuring random read IOPS: ");
            std::cout.flush();
            auto read_iops = iotune_tests.read_random_data(test_directory.minimum_io_size()).get0();
            fmt::print("{} IOPS\n", uint64_t(read_iops.iops));

            struct disk_descriptor desc;
            desc.mountpoint = test_directory.name();
            desc.read_iops = read_iops.iops;
            desc.read_bw = read_bw.bytes_per_sec;
            desc.write_iops = write_iops.iops;
            desc.write_bw = write_bw.bytes_per_sec;

            // Allow each I/O Queue to have at least 10k IOPS and 100MB. Values decided based
            // on the write performance, which tends to be lower.
            unsigned num_io_queues = smp::count;
            num_io_queues = std::min(smp::count, unsigned(desc.write_iops / 10000));
            num_io_queues = std::min(smp::count, unsigned(desc.write_bw / (100 * 1024 * 1024)));
            num_io_queues = std::max(num_io_queues, 1u);
            fmt::print("Recommended --num-io-queues: {}\n", num_io_queues);

            auto file = "properties file";
            try {
                if (configuration.count("properties-file")) {
                    write_property_file(configuration["properties-file"].as<sstring>(), desc);
                }

                file = "configuration file";
                if (configuration.count("options-file")) {
                    write_configuration_file(configuration["options-file"].as<sstring>(), format, configuration["properties-file"].as<sstring>(), num_io_queues);
                }
            } catch (...) {
                iotune_logger.error("Exception when writing {}: {}.\nPlease add the above values manually to your seastar command line.", file, std::current_exception());
                return 1;
            }
            return 0;
        });
    });
}

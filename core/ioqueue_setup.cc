#include <chrono>
#include <random>
#include <memory>
#include <vector>
#include <cmath>
#include <libaio.h>
#include <boost/thread/barrier.hpp>
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics.hpp>
#include <mutex>
#include <deque>
#include "sstring.hh"
#include "posix.hh"
#include "reactor.hh"
#include "util/defer.hh"

using namespace std::chrono_literals;

struct directory {
    sstring name;
    file_desc file;

    directory(sstring name) : name(name)
                            , file(file_desc::open(name.c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY))
    {}
};

struct test_file {
    sstring name;
    file_desc file;

    test_file(const directory& dir) : name(dir.name + "/ioqueue-discovery")
                                    , file(file_desc::open(name.c_str(),  O_DIRECT | O_CLOEXEC | O_RDWR | O_CREAT, S_IRWXU))
    {}
    void generate();
};

struct run_stats {
    uint64_t latency = 0;
    uint64_t IOPS = 0;
    uint64_t concurrency = 0;
    double latstdev = 0;
    run_stats& operator+=(const struct run_stats& stats) {
        if (stats.concurrency != 0) {
            IOPS += stats.IOPS;
            latstdev = latstdev * latstdev * concurrency + (stats.latstdev * stats.latstdev * stats.concurrency);
            auto new_latency = (concurrency * latency) + (stats.concurrency * stats.latency);
            concurrency += stats.concurrency;
            latstdev = std::sqrt(latstdev / concurrency);

            new_latency /= concurrency;
            latency = new_latency;
        }
        return *this;
    }
};

class test_manager {
public:
    enum class test_done { no, yes };
    using clock = std::chrono::steady_clock;
    static constexpr size_t file_size = 1 << 30;
    static constexpr size_t wbuffer_size = 128 << 10;
    static constexpr size_t rbuffer_size = 4 << 10;
private:
    boost::barrier _time_run_barrier;
    boost::barrier _start_run_barrier;
    boost::barrier _finish_run_barrier;
    boost::barrier _results_barrier;

    std::atomic<test_done> _test_done = { test_done::no };

    test_file _test_file;

    run_stats _test_result;
    std::mutex _result_mutex;
    std::vector<std::thread> _threads;
    test_manager::clock::time_point _run_start_time;

    std::unordered_map<unsigned, uint64_t> _run_results;

    run_stats issue_reads(unsigned this_concurrency, test_manager::clock::time_point start_time);

    run_stats current_result(size_t cpu_id) {
        assert(cpu_id == 0);
        std::lock_guard<std::mutex> guard(_result_mutex);
        return _test_result;
    }
    run_stats _best_result;
    unsigned _tries = 0;
    unsigned _step = 2;
    static constexpr unsigned max_tries = 3;

    std::atomic<int> next_concurrency = { 4 };

    // We want to find the point the sweet spot for the maximum number of I/O operations to issue.
    // We will search for the point where the increase in throughput (IOPS) is smaller than the increase
    // in latency.
    //
    // To achieve that we simply divide the IOPS we have obtained by the maximum latency expected for this run.
    //
    // The maximum latency is simply the mean latency plus twice the standard deviation.
    double run_score(run_stats &s) {
        if (s.IOPS == 0) {
            assert(s.latency == 0);
            return 0;
        }
        return float(s.IOPS) / (s.latency + 2 * s.latstdev);
    }
public:
    test_manager(size_t n, sstring dirname) : _time_run_barrier(n)
                                            , _start_run_barrier(n)
                                            , _finish_run_barrier(n)
                                            , _results_barrier(n)
                                            , _test_file(directory(dirname))
                                            , _run_start_time(test_manager::clock::now()) {
        _test_file.generate();
    }
    template <typename Func>
    void spawn_new(Func&& func) {
        _threads.emplace_back(std::thread(std::forward<Func>(func)));
    }

    unsigned get_thread_concurrency(size_t cpu_id) {
        _time_run_barrier.wait();
        if (cpu_id == 0) {
            _test_result = run_stats();
            _run_start_time = test_manager::clock::now() + 100ms;
        }
        _start_run_barrier.wait();

        auto overall_concurrency = next_concurrency.load(std::memory_order_relaxed);
        auto my_concurrency = overall_concurrency / _threads.size();
        if (cpu_id < (overall_concurrency % _threads.size())) {
            my_concurrency++;
        }
        return my_concurrency;
    }

    void run_test(unsigned concurrency) {
        if (concurrency != 0) {
            auto r = issue_reads(concurrency, _run_start_time);
            std::lock_guard<std::mutex> guard(_result_mutex);
            _test_result += r;
        }
        _finish_run_barrier.wait();
    }

    test_done analyze_results(size_t cpu_id) {
        if (cpu_id == 0) {
            struct run_stats result = current_result(cpu_id);
            if (run_score(_best_result) < run_score(result)) {
                next_concurrency.fetch_add(_step, std::memory_order_relaxed);
                _step = 2;
                _best_result = result;
            } else if (_step == 2) {
                _step = 1;
                next_concurrency.fetch_sub(_step, std::memory_order_relaxed);
            } else if (++_tries < max_tries)  {
                next_concurrency.fetch_add(_step, std::memory_order_relaxed);
            } else {
                _test_done.store(test_done::yes, std::memory_order_relaxed);
            }
        }
        _results_barrier.wait();
        return _test_done.load(std::memory_order_relaxed);
    }

    uint32_t finish_estimate() {
        for (auto&& t: _threads) {
            t.join();
        }
        // We now have a point where the curve starts to bend, which means,
        // latency is increasing while throughput is not. We, however, don't
        // want to put Seastar's I/O queue at exactly this point. We have all
        // sorts of delays throughout the stack, including in the Linux kernel.
        //
        // Moreover, not all disks have a beautiful, well behaved, and monotonic graph.
        //
        // Empirically, we will just allow three times as much as the number we have found.
        return _best_result.concurrency * 3;
    }
};

static thread_local std::default_random_engine random_generator(std::chrono::duration_cast<std::chrono::nanoseconds>(test_manager::clock::now().time_since_epoch()).count());

class reader {
public:
    enum class read_done { no, yes };
private:
    std::deque<uint64_t> _latencies;
    io_context_t& _io_context;
    file_desc _file;
    std::uniform_int_distribution<uint32_t> _pos_distribution;
    struct iocb _iocb;
    uint32_t _counter = 0;
    test_manager::clock::time_point _start_time;
    test_manager::clock::time_point _tstamp;
    uint64_t _total_time = 0;

    char _buf[test_manager::rbuffer_size] __attribute__((aligned(4096)));
public:
    reader(io_context_t& io_context, file_desc f, test_manager::clock::time_point start_time)
                        : _io_context(io_context)
                        , _file(std::move(f))
                        , _pos_distribution(0, (test_manager::file_size/ test_manager::rbuffer_size) - 1)
                        , _start_time(start_time)
                        , _tstamp(test_manager::clock::now())
    {
        // We want to make sure that we fire requests in the concurrency level we are testing.
        // But the various threads will start their requests at different times, so we calculate
        // a start time that is sufficiently in the future. All readers must make sure that they start
        // the test before that time point was reached.
        //
        // We will start firing requests right away, but won't measure those. Only when the _start_time
        // is reached we will start measuring. This will guarantee that the requests we do measure will
        // be issued at the desired concurrency level.
        //
        // We will keep issuing past the end time as well, for similar reasons.
        assert(test_manager::clock::now() < _start_time);
    }

    void issue() {
        io_prep_pread(&_iocb, _file.get(), _buf, test_manager::rbuffer_size, _pos_distribution(random_generator) * test_manager::rbuffer_size);
        _iocb.data = this;
        struct iocb* iocbpp[1];
        iocbpp[0] = &_iocb;
        _tstamp = std::chrono::steady_clock::now();
        auto r = ::io_submit(_io_context, 1, iocbpp);
        throw_kernel_error(r);
    }

    read_done req_finished() {
        auto tstamp = _tstamp;
        issue();
        auto now = std::chrono::steady_clock::now();
        if ((now > _start_time) && (now < (_start_time + 200ms))) {
            auto delta = std::chrono::duration_cast<std::chrono::microseconds>(now - tstamp).count();
            // We could simplify this by just taking stamps in the beginning and end, and given enough requests
            // that would amount to a good enough estimate. But since latency is also a important measure that
            // requires a delta to be calculated anyway, we can juse reuse it.
            _total_time += delta;
            assert(delta != 0);
            _latencies.push_back(delta);
        }

        // push requests 50ms after the end time
        if (now < _start_time + 250ms) {
            return read_done::no;
        }

        return read_done::yes;
    }

    struct run_stats get_stats() {
        using namespace boost::accumulators;
        accumulator_set<uint64_t, features<tag::mean, tag::variance>> acc;
        std::sort(_latencies.begin(), _latencies.end());

        // A lot of disks are prone to heavy spikes in latency, so we will only take into account the
        // 90th percentile when dimensioning the I/O queue requests. That should give us a better idea about
        // the expected behavior for this disk under normal circumnstances.
        //
        // We should also disregard the first 10 % of the 90th percentile. This is so we don't count the latencies
        // of requests that happened to be issued alone (or below the desired concurrency level) by a breeze of luck.
        unsigned tenpct = _latencies.size() / 10;
        for (auto it = _latencies.begin() + tenpct; it < _latencies.end() - tenpct; ++it) {
            assert(*it != 0);
            acc(*it);
        }

        auto latency = uint64_t(mean(acc)); // usec
        auto IOPS = (_latencies.size() * std::chrono::duration_cast<std::chrono::microseconds>(1s).count()) / _total_time; // req / s
        auto latvar = variance(acc);
        return { latency, IOPS, 1, std::sqrt(latvar) };
    }
};


run_stats test_manager::issue_reads(unsigned concurrency, test_manager::clock::time_point start_time) {
    io_context_t io_context = {0};
    auto r = ::io_setup(concurrency, &io_context);
    assert(r >= 0);
    auto destroyer = defer([&io_context] { ::io_destroy(io_context); });

    unsigned finished = 0;
    std::vector<io_event> ev;
    ev.resize(concurrency);

    auto fds = std::vector<reader>();
    for (unsigned i = 0u; i < concurrency; ++i) {
        fds.emplace_back(io_context, _test_file.file.dup(), start_time);
    }
    for (auto& r: fds) {
        r.issue();
    }

    struct timespec timeout = {10, 0};
    while (finished != concurrency) {
        auto n = ::io_getevents(io_context, 1, ev.size(), ev.data(), &timeout);
        assert(n > 0);
        for (auto i = 0ul; i < size_t(n); ++i) {
            auto done = reinterpret_cast<reader*>(ev[i].data)->req_finished();
            if (done == reader::read_done::yes) {
                finished++;
            }
        }
    }
    struct run_stats result;
    for (auto&& r: fds) {
        result += r.get_stats();
    }
    return result;
}

void test_file::generate() {
    struct stat st;
    auto ret = stat(name.c_str(), &st);

    // If the file already exists and is filled with real blocks, there is no
    // need to create it again.
    if ((ret == 0) && (size_t(st.st_blocks * 512) >= test_manager::file_size)) {
        // We're good!
        return;
    }

    auto buf = allocate_aligned_buffer<char>(test_manager::wbuffer_size, 4096);
    memset(buf.get(), 0, test_manager::wbuffer_size);
    auto ft = ftruncate(file.get(), test_manager::file_size);
    throw_kernel_error(ft);

    for (auto i = 0ul; i < (test_manager::file_size / test_manager::wbuffer_size); ++i) {
        auto size = write(file.get(), buf.get(), test_manager::wbuffer_size);
        if (size != (test_manager::wbuffer_size)) {
            throw std::system_error(errno, std::system_category());
        }
    }
}

uint32_t io_queue_discovery(sstring dir, std::vector<unsigned> cpus) {
    test_manager test_manager(cpus.size(), dir);

    for (auto i = 0ul; i < cpus.size(); ++i) {
        test_manager.spawn_new([&cpus, &test_manager, id = i] {
            pin_this_thread(cpus[id]);
            do {
                auto my_concurrency = test_manager.get_thread_concurrency(id);
                test_manager.run_test(my_concurrency);
            } while (test_manager.analyze_results(id) == test_manager::test_done::no);
        });
    }

    return test_manager.finish_estimate();
}


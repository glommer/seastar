#include <chrono>
#include <random>
#include <memory>
#include <vector>
#include <cmath>
#include "sstring.hh"
#include "posix.hh"
#include "reactor.hh"
#include <libaio.h>
#include <boost/thread/barrier.hpp>

using namespace std::chrono_literals;

static thread_local std::default_random_engine random_generator(std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());

class test_file;

struct directory {
    sstring name;
    file_desc file;

    directory(sstring name) : name(name)
                            , file(file_desc::open(name.c_str(), O_DIRECTORY | O_CLOEXEC | O_RDONLY))
    {}
};

class reader {
public:
    enum class read_done { no, yes };
    struct stats {
        uint64_t latency = 0;
        uint64_t IOPS = 0;
        uint64_t concurrency = 0;
        stats& operator+=(const struct stats& stats) {
            IOPS += stats.IOPS;
            latency = (concurrency * latency) + (stats.concurrency * stats.latency);
            concurrency += stats.concurrency;
            if (concurrency) {
                latency /= concurrency;
            }
            return *this;
        }
    };
private:
    io_context_t& _io_context;
    file_desc _file;
    std::uniform_int_distribution<uint32_t> _pos_distribution;
    std::unique_ptr<char[], free_deleter> _buf;
    struct iocb _iocb;
    uint32_t _counter = 0;
    std::chrono::steady_clock::time_point _tstamp;
    uint64_t _avg_latency = 0;
    uint64_t _total_time = 0;

public:
    reader(io_context_t& io_context, file_desc f)
                        : _io_context(io_context)
                        , _file(std::move(f))
                        , _pos_distribution(0, (8 * 1024 * 128 / 4) - 1)
                        , _buf(allocate_aligned_buffer<char>(4096, 4096))
                        , _tstamp(std::chrono::steady_clock::now())
    {}

    void issue() {
        io_prep_pread(&_iocb, _file.get(), _buf.get(), 4096, _pos_distribution(random_generator) * 4096);
        _iocb.data = this;
        struct iocb* iocbpp[1];
        iocbpp[0] = &_iocb;
        auto r = ::io_submit(_io_context, 1, iocbpp);
        throw_kernel_error(r);
        _tstamp = std::chrono::steady_clock::now();
    }

    read_done req_finished() {
        auto now = std::chrono::steady_clock::now();
        auto delta = std::chrono::duration_cast<std::chrono::microseconds>(now - _tstamp).count();
        _avg_latency += _counter * _avg_latency + (delta);
        _avg_latency /= ++_counter;
        // We could simplify this by just taking stamps in the beginning and end, and given enough requests
        // that would amount to a good enough estimate. But since latency is also a important measure that
        // requires a delta to be calculated anyway, we can juse reuse it.
        _total_time += delta;
        _tstamp = now;

        if (_counter < 1000) {
            issue();
            return read_done::no;
        }
    //    printf("Finished 1000 iterations. Latency: %ld, IOPS %ld\n", _avg_latency, (1000 * (1000 * 1000)) / _total_time);
        return read_done::yes;
    }

    struct stats get_stats() {
        return { _avg_latency, (1000 * std::chrono::duration_cast<std::chrono::microseconds>(1s).count()) / _total_time, 1 };
    }
};

struct test_file {
    sstring name;
    file_desc file;

    test_file(const directory& dir) : name(dir.name + "/ioqueue-discovery")
                                    , file(file_desc::open(name.c_str(), O_DIRECT | O_CLOEXEC | O_RDWR | O_CREAT, S_IRWXU))
    {}
    void generate() {
        std::string buf(size_t(128 << 10), char(0));
        auto ft = ftruncate(file.get(), 8 * 1024 * (128 << 10));
        throw_kernel_error(ft);

        for (auto i = 0; i < 8 * 1024; ++i) {
            auto size = write(file.get(), buf.c_str(), 128 << 10);
            if (size != (128 << 10)) {
                throw std::runtime_error(sprint("problem writing to test file %s", name));
            }
        }
    }
    struct reader get_reader(io_context_t& io_context) {
        return reader(io_context, file.dup());
    }
};

reader::stats issue_reads(unsigned concurrency, test_file& file) {
    io_context_t io_context = {0};
    auto r = ::io_setup(concurrency, &io_context);
    assert(r >= 0);

    unsigned finished = 0;
    std::vector<io_event> ev;
    ev.resize(concurrency);

    // better way to write this?
    auto fds = std::vector<std::unique_ptr<reader>>();
    for (unsigned i = 0u; i < concurrency; ++i) {
        fds.emplace_back(std::make_unique<reader>(file.get_reader(io_context)));
    }
    for (auto& r: fds) {
        r->issue();
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
    struct reader::stats result;
    for (auto&& r: fds) {
        result += r->get_stats();
        //printf("intermediate latency: %ld, concurrency %ld\n", result.latency, result.concurrency);
    }
    return result;
}

uint32_t io_queue_discovery(sstring dir, std::vector<unsigned> cpus) {
    directory directory(dir);
    test_file test_file(directory);
    test_file.generate();

    std::vector<std::thread> _threads;

    class test_synchronizer {
        boost::barrier _start;
        boost::barrier _finish_run;
        boost::barrier _results;
    public:
        std::atomic<int> next_concurrency = { 4 };

        test_synchronizer(size_t n) : _start(n), _finish_run(n), _results(n) {}
        void start() {
            _start.wait();
        }
        void finish_run() {
            _finish_run.wait();
        }
        void results() {
            _results.wait();
        }
    } test_sync(cpus.size());

    std::vector<reader::stats> results __attribute__((aligned(64)));
    results.resize(cpus.size());

    printf("Really aligned: %p\n", results.data());
    for (auto i = 0ul; i < cpus.size(); ++i) {
        // For all but the least powerful disks, a thread alone won't be enough to saturate the disk.
        // We'll have to potentially use all of them.
        _threads.emplace_back(std::thread([cpus = std::move(cpus), &test_sync, id = i, &results, &test_file] {
            pin_this_thread(cpus[id]);

            int concurrency = test_sync.next_concurrency.load(std::memory_order_relaxed);
            while (concurrency != 0) { 
                test_sync.start();
                auto my_concurrency = concurrency / cpus.size();
                if (id < (concurrency % cpus.size())) {
                    my_concurrency++;
                }
                if (my_concurrency != 0) {
                    // FIXME: Have a writer wrapper around test_file
                    results[id] = issue_reads(my_concurrency, test_file);
                }

                test_sync.finish_run();

                struct reader::stats result;
                for (auto&& r: results) {
                    result += r;
                    //printf("final intermediate latency: %ld, concurrency %ld\n", result.latency, result.concurrency);
                }
                memset(results.data(), 0, cpus.size());
                if (id == 0) {
                    static reader::stats left;
                    static reader::stats right;
                    assert(concurrency == int(result.concurrency));

                    printf("RESULTs: for LEFT %ld: lat %ld usec, IOPS %ld ops/s\n", left.concurrency, left.latency, left.IOPS);
                    printf("RESULTs: for RIGHT %ld: lat %ld usec, IOPS %ld ops/s\n", right.concurrency, right.latency, right.IOPS);
                    printf("RESULTs: for %ld: lat %ld usec, IOPS %ld ops/s\n\n", result.concurrency, result.latency, result.IOPS);
                    unsigned new_concurrency = 0; // 0 means we have found a point we are satisfied with, and will stop.
                    // FIXME: 5 is arbitrary. Calculate stddev
                    auto delta = [](auto& result, auto& comparison) {
                        int d = int(result.IOPS - comparison.IOPS);
                        return std::abs((100 * d) / result.IOPS);
                    };
                    if ((result.IOPS > right.IOPS) && delta(result, right) > 5) {
                        printf("\tdelta right is %.f, branching\n", delta(result, right));
                        if (result.concurrency < right.concurrency) {
                            right = result;
                            new_concurrency = (left.concurrency + right.concurrency) / 2;
                        } else {
                            left = right;
                            right = result;
                            new_concurrency = concurrency * 2;
                        }
                    } else if ((result.IOPS > left.IOPS) && delta(result, left) > 5) {
                        printf("\tdelta left is %.f branching\n", delta(result, left));
                        left = result;
                        new_concurrency = (left.concurrency + result.concurrency) / 2;
                    } else if (left.concurrency != right.concurrency) {
                        if (result.IOPS > result.IOPS) {
                            right = result;
                        }
                        new_concurrency = (left.concurrency + right.concurrency) / 2;
                    }
                    // Probably not relaxed
                    test_sync.next_concurrency.store(new_concurrency, std::memory_order_relaxed);
                }
                test_sync.results();
                concurrency = test_sync.next_concurrency.load(std::memory_order_relaxed);
            }
        }));
    }

    for (auto&& t: _threads) {
        t.join();
    }

    return 0;
}


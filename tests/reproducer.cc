#include <core/reactor.hh>
#include <core/sstring.hh>
#include "tests/test-utils.hh"
#include <boost/range/irange.hpp>
#include <stdlib.h>
#include <boost/filesystem.hpp>

// Creates a new empty directory with arbitrary name, which will be removed
// automatically when tmpdir object goes out of scope.
struct tmpdir {
    tmpdir() {
        char tmp[] = "tmpdir_XXXXXX";
        auto * dir = ::mkdtemp(tmp);
        if (dir == NULL) {
            throw std::runtime_error("Could not create temp dir");
        }
        path = dir;
        //std::cout << path << std::endl;
    }
    tmpdir(tmpdir&& v)
        : path(std::move(v.path)) {
        assert(v.path.empty());
    }
    tmpdir(const tmpdir&) = delete;
    ~tmpdir() {
        if (!path.empty()) {
            boost::filesystem::remove_all(path.c_str());
        }
    }
    tmpdir & operator=(tmpdir&& v) {
        if (&v != this) {
            this->~tmpdir();
            new (this) tmpdir(std::move(v));
        }
        return *this;
    }
    tmpdir & operator=(const tmpdir&) = delete;
    sstring path;
};

SEASTAR_TEST_CASE(reproducer) {
    printf("Starting test\n");
    auto tmp = make_lw_shared<tmpdir>();
    return do_with(int(0), [tmp] (int& finished) {
        size_t count = 10000;
        auto idx = boost::irange(0, int(count));
        printf("Shard %d starting %ld flushes\n", engine().cpu_id(), count);
        return parallel_for_each(idx.begin(), idx.end(), [&finished, tmp] (uint64_t idx) {
            auto oflags = open_flags::wo | open_flags::create | open_flags::exclusive;
            auto name = sprint("%s/test-file-%ld",tmp->path, idx);
            return open_file_dma(name, oflags).then([] (auto f) {
                return f.truncate(4096).then([f] () mutable {
                    auto bufptr = allocate_aligned_buffer<char>(4096, 4096);
                    auto buf = bufptr.get();
                    return f.dma_write(0, buf, 4096).then([bufptr = std::move(bufptr)] (auto s) {});
                }).then([f] () mutable {
                    return f.flush();
                }).then([f] {});
            });
        }).then([count] {
             printf("Shard %d finished %ld flushes\n", engine().cpu_id(), count);
             return make_ready_future<>();
        });
    }).then([tmp] {});
}



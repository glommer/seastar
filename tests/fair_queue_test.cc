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
 * Copyright (C) 2016 ScyllaDB
 */

#include "core/thread.hh"
#include "core/do_with.hh"
#include "test-utils.hh"
#include "core/sstring.hh"
#include "core/reactor.hh"
#include "core/fair_queue.hh"
#include "core/do_with.hh"
#include "core/future-util.hh"
#include "core/sleep.hh"
#include <boost/range/irange.hpp>
#include <chrono>

using namespace std::chrono_literals;

struct test_env {
    fair_queue fq;
    std::vector<int> results;
    std::vector<priority_class*> classes;
    std::vector<future<>> inflight;
    test_env(unsigned capacity) : fq(capacity)
    {}

    size_t register_priority_class(uint32_t shares) {
        results.push_back(0);
        classes.push_back(&fq.register_priority_class(shares));
        return classes.size() - 1;
    }
    void do_op(unsigned index, unsigned weight)  {
        auto cl = classes[index];
        auto f = fq.queue(*cl, weight, [this, index] {
            results[index]++;
            return sleep(100us);
        });
        inflight.push_back(std::move(f));
    }
    void update_shares(unsigned index, uint32_t shares) {
        auto cl = classes[index];
        fq.update_shares(*cl, shares);
    }
    // Verify if the ratios are what we expect. Because we can't be sure about
    // precise timing issues, we can always be off by one.
    //
    // Argument is the ratios towards the first class
    future<> verify(std::vector<unsigned> ratios) {
        return wait_on_pending().then([r = results, ratios = std::move(ratios)] {
            assert(ratios.size() == r.size());
            for (auto i = 0ul; i < ratios.size(); ++i) {
                int min_expected = ratios [i] * (r[0] - 1);
                int max_expected = ratios[i] * (r[0] + 1);
                BOOST_REQUIRE(r[i] >= min_expected);
                BOOST_REQUIRE(r[i] <= max_expected);
            }
        });
    }
    future<> wait_on_pending() {
        auto curr = make_lw_shared<std::vector<future<>>>();
        curr->swap(inflight);
        return when_all(curr->begin(), curr->end()).discard_result();
    }
};

// Equal ratios. Expected equal results.
SEASTAR_TEST_CASE(test_fair_queue_equal_2classes) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(5ms).then([env] {
        return env->verify({1, 1});
    }).then([env] {});
}

// Equal results, spread among 4 classes.
SEASTAR_TEST_CASE(test_fair_queue_equal_4classes) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);
    auto c = env->register_priority_class(10);
    auto d = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
        env->do_op(c, 1);
        env->do_op(d, 1);
    }
    return sleep(5ms).then([env] {
        return env->verify({1, 1, 1, 1});
    }).then([env] {});
}

// Class2 twice as powerful. Expected class2 to have 2 x more requests.
SEASTAR_TEST_CASE(test_fair_queue_different_shares) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(20);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(5ms).then([env] {
        return env->verify({1, 2});
    }).then([env] {});
}

// Equal ratios, high capacity queue. Should still divide equally.
//
// Note that we sleep less because now more requests will be going through the
// queue.
SEASTAR_TEST_CASE(test_fair_queue_equal_hi_capacity_2classes) {
    auto env = make_lw_shared<test_env>(10);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(1ms).then([env] {
        return env->verify({1, 1});
    }).then([env] {});

}

// Class2 twice as powerful, queue is high capacity. Still expected class2 to
// have 2 x more requests.
//
// Note that we sleep less because now more requests will be going through the
// queue.
SEASTAR_TEST_CASE(test_fair_queue_different_shares_hi_capacity) {
    auto env = make_lw_shared<test_env>(10);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(20);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(1ms).then([env] {
        return env->verify({1, 2});
    }).then([env] {});
}

// Classes equally powerful. But Class1 issues twice as expensive requests. Expected Class2 to have 2 x more requests.
SEASTAR_TEST_CASE(test_fair_queue_different_weights) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 2);
        env->do_op(b, 1);
    }
    return sleep(5ms).then([env] {
        return env->verify({1, 2});
    }).then([env] {});
}

// Class2 pushes many requests over 10ms. In the next msec at least, don't expect Class2 to be able to push anything else.
SEASTAR_TEST_CASE(test_fair_queue_dominant_queue) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(b, 1);
    }
    return env->wait_on_pending().then([env, a, b] {
        env->results[b] = 0;
        for (int i = 0; i < 20; ++i) {
            env->do_op(a, 1);
            env->do_op(b, 1);
        }
        return sleep(1ms).then([env] {
            return env->verify({1, 0});
        });
    }).then([env] {});
}

// Class2 pushes many requests over 10ms. After enough time, this shouldn't matter anymore.
SEASTAR_TEST_CASE(test_fair_queue_forgiving_queue) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(b, 1);
    }
    return env->wait_on_pending().then([] {
        return sleep(100ms);
    }).then([env, a, b] {
        env->results[b] = 0;
        for (int i = 0; i < 20; ++i) {
            env->do_op(a, 1);
            env->do_op(b, 1);
        }
        return sleep(1ms).then([env] {
            return env->verify({1, 1});
        });
    }).then([env] {});
}

// Classes push requests and then update swap their shares. In the end, should have executed
// the same number of requests.
SEASTAR_TEST_CASE(test_fair_queue_update_shares) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(20);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 100; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(1ms).then([env, a, b] {
       env->update_shares(a, 10); 
       env->update_shares(b, 20); 
       return sleep(1ms);
    }).then([env] {
       return env->verify({1, 1});
    }).then([env] {});
}

// Classes run for a longer period of time. Balance must be kept over many timer
// periods.
SEASTAR_TEST_CASE(test_fair_queue_longer_run) {
    auto env = make_lw_shared<test_env>(1);

    auto a = env->register_priority_class(10);
    auto b = env->register_priority_class(10);

    for (int i = 0; i < 10000; ++i) {
        env->do_op(a, 1);
        env->do_op(b, 1);
    }
    return sleep(500ms).then([env, a, b] {
       return env->verify({1, 1});
    }).then([env] {});
}

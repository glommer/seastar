#pragma once

#include "future.hh"
#include "semaphore.hh"
#include "shared_ptr.hh"
#include "timer.hh"
#include "print.hh"
#include <queue>
#include <type_traits>
#include <experimental/optional>

/// \addtogroup io-module
/// @{

/// \cond internal
/// \brief Priority class, to be used with a given \ref fair_queue
///
/// Before this class is used, it has to be associated with a \ref fair_queue instance
/// through the \ref associate_queue method. Using a priority_class without proper
/// initialization will throw a \ref unconfigured_queue exception.
///
/// \related fair_queue
class priority_class {
    friend class fair_queue;
    uint32_t _shares = 0;
    float _ops = 0;
    float _weight = 0;
    std::queue<promise<>> _queue;

    friend struct shared_ptr_no_esft<priority_class>;
    /// Constructs a priority class with a given \c shares
    explicit priority_class(uint32_t shares) : _shares(shares) {}
};
/// \endcond

/// \brief Fair queuing class
///
/// This is a fair queue, allowing multiple consumers to queue requests
/// that will then be served proportionally to their \ref shares.
///
/// The user of this interface is expected to register multiple \ref priority_class
/// objects, which will each have a \ref shares attribute. The queue will then
/// serve requests proportionally to \ref shares divided by the sum of all shares for
/// all classes registered against this queue.
///
/// Each priority class keeps a separate queue of requests. Requests pertaining to
/// a class can go through even if they are over its \ref shares limit, provided that
/// the other classes have empty queues.
///
/// When the queues that lag behind start seeing requests, the fair queue will serve
/// them first, until balance is restored. This balancing is expected to happen within
/// a certain period of time.
///
/// Once the classes are registered, work is submitted through the class itself.
class fair_queue {
    friend priority_class;
    using priority_class_ptr = lw_shared_ptr<priority_class>;

    struct class_compare {
        bool operator() (const priority_class_ptr& lhs, const priority_class_ptr &rhs) const {
            return lhs->_ops < rhs->_ops;
        }
    };

    semaphore _sem;
    uint64_t _total_shares = 0;
    timer<> _bandwidth_timer;
    using prioq = std::priority_queue<priority_class_ptr, std::vector<priority_class_ptr>, class_compare>;
    prioq _handles;

    void execute_one(unsigned weight) {
        _sem.wait().then([this, weight] {
            prioq::container_type scanned;
            while (!_handles.empty()) {
                auto h = _handles.top();
                scanned.push_back(h);
                _handles.pop();

                if (!h->_queue.empty()) {
                    auto& pr = h->_queue.front();
                    pr.set_value();
                    h->_queue.pop();
                    h->_ops += weight;
                    refill_heap(std::move(scanned));
                    return make_ready_future<>();;
                }
            }
            throw std::runtime_error("Trying to execute command in empty queue!");
        });
    }

    void refill_heap(prioq::container_type scanned) {
        for (auto& s: scanned) {
            _handles.push(s);
        }
    }

    void reset_stats() {
        prioq::container_type scanned;
        while (!_handles.empty()) {
            auto h = _handles.top();
            scanned.push_back(h);
            _handles.pop();
            h->_ops = 0;
            if (_total_shares) {
                h->_weight = h->_shares / _total_shares;
            } else {
                h->_weight = 0; // Can happen if the timer triggers before any class is registered.
            }
        }
        refill_heap(std::move(scanned));
    }
public:
    /// Constructs a fair queue with a given \c capacity.
    ///
    /// \param capacity how many concurrent requests are allowed in this queue.
    explicit fair_queue(int capacity) : _sem(capacity)
                                      , _bandwidth_timer([this] { reset_stats(); }) {
        _bandwidth_timer.arm_periodic(std::chrono::milliseconds(20));
    }

    /// Registers a priority class against this fair queue.
    ///
    /// \param shares, how many shares to create this class with
    priority_class& register_priority_class(uint32_t shares) {
        _total_shares += shares;
        priority_class_ptr pclass = make_lw_shared<priority_class>(shares);
        _handles.push(pclass);
        reset_stats();
        return *pclass;
    }
    /// \return how many waiters are currently queued for all classes.
    size_t waiters() const {
        return _sem.waiters();
    }

    /// Executes the function \c func through this class' \ref fair_queue, consuming \c weight
    ///
    /// \throw misconfigured_queue exception if called for an unbound queue
    ///
    /// \return whatever \c func returns
    template <typename Func>
    std::result_of_t<Func()> queue(priority_class& pc, unsigned weight, Func func) {
        promise<> pr;
        auto fut = pr.get_future();
        pc._queue.push(std::move(pr));
        execute_one(weight);
        return fut.then([func = std::move(func)] {
            return func();
        }).finally([this] {
            _sem.signal();
        });
    }

    /// Updates the current shares of this priority class
    ///
    /// \param new_shares the new number of shares for this priority class
    void update_shares(priority_class& pc, uint32_t new_shares) {
        _total_shares -= pc._shares;
        pc._shares = new_shares;
        reset_stats();
    }

    float weight_of_class(priority_class &pc) {
        return float(pc._shares) / _total_shares;
    }
};

/// @}

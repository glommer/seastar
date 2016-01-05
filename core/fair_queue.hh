#pragma once

#include "future.hh"
#include "semaphore.hh"
#include "shared_ptr.hh"
#include "timer.hh"
#include "print.hh"
#include <queue>
#include <type_traits>
#include <experimental/optional>

/// \addtogroup fiber-module
/// @{
class priority_class;
using priority_class_ptr = lw_shared_ptr<priority_class>;

/// Exception thrown when a fair queue is not properly set up
class misconfigured_queue : public std::exception {
    sstring _msg;
public:
    misconfigured_queue(sstring msg) : std::exception(), _msg("misconfigured queue: " + msg) {}
    /// Reports the exception reason.
    const char *what() const noexcept {
        return _msg.c_str();
    }
};

/// \brief Fair queuing class
///
/// This is a fair queue, allowing multiple consumers to queue requests
/// that will then be served proportionally to their \ref power.
///
/// The user of this interface is expected to register multiple \ref priority_class
/// objects, which will each have a \ref power attribute. The queue will then
/// serve requests proportionally to \ref power divided by the sum of all powers for
/// all classes registered against this queue.
///
/// Each priority class keeps a separate queue of requests. Requests pertaining to
/// a class can go through even if they are over its \ref power limit, provided that
/// the other classes have empty queues.
///
/// When the queues that lag behind start seeing requests, the fair queue will serve
/// them first, until balance is restored. This balancing is expected to happen within
/// a certain period of time, the resolution of the queue. Queues with smaller resolution
/// numbers adapt faster to changes in workload.
///
/// Once the classes are registered, work is submitted through the class itself.
struct fair_queue : public enable_lw_shared_from_this<fair_queue> {
    friend priority_class;
private:
    struct qh_compare {
        bool operator() (const lw_shared_ptr<priority_class>& lhs, const lw_shared_ptr<priority_class>&rhs) const;
    };

    semaphore _sem;
    uint64_t _total_ops = 0;
    uint64_t _total_power = 0;
    timer<> _bandwidth_timer;
    using prioq = std::priority_queue<priority_class_ptr, std::vector<priority_class_ptr>, qh_compare>;
    prioq _handles;

private:
    void refill_heap(prioq::container_type scanned);

    // Those operations only happen through the priority_class
    void execute_one();
    void finish_one() {
        _sem.signal();
    }

    void reset_stats();
public:
    /// Constructs a fair queue with a given \c capacity, that balances requests over
    /// \c period milliseconds (the queue resolution).
    ///
    /// \param capacity how many concurrent requests are allowed in this queue.
    /// \param period over how much time (in milliseconds) the queue is expected to balance.
    fair_queue(int capacity, std::experimental::optional<std::chrono::milliseconds> period) : _sem(capacity)
                             , _bandwidth_timer([this] { reset_stats(); }) {
        if (period) {
            _bandwidth_timer.arm_periodic(*period);
        }
    }
    /// Constructs a fair queue with a given \c capacity, with a resolution
    /// of 100 miliseconds
    ///
    /// \param capacity how many concurrent requests are allowed in this queue.
    fair_queue(int capacity) : fair_queue(capacity, std::chrono::milliseconds(100)) {}

    /// Registers a priority class against this queue.
    ///
    /// \param class, a priority pclass, which is an object of the class \c priority_class
    void register_priority_class(priority_class_ptr pclass);

    /// \return how many waiters are currently queued for all classes.
    size_t waiters() const {
        return _sem.waiters();
    }
};

/// \brief Priority class, to be used with a given \ref fair_queue
///
/// Before this class is used, it has to be associated with a \ref fair_queue instance
/// through the \ref associate_queue method. Using a priority_class without proper
/// initialization will throw a \ref unconfigured_queue exception.
///
/// \related fair_queue
class priority_class {
    uint32_t _power = 0;
    uint32_t _ops = 0;
    lw_shared_ptr<fair_queue> _fq;
    friend fair_queue;
    std::queue<promise<>> _queue;

public:
    /// Associates this priority class to a \ref fair_queue. Each priority class can
    /// only be associated with a single priority queue.
    ///
    /// Calling this method for a queue that is already configured will throw \ref
    /// misconfigured_queue exception.
    void associate_queue(lw_shared_ptr<fair_queue> qh) {
        if (_fq) {
            throw misconfigured_queue("fair queue already bound.");
        }
        _fq = qh;
    }

    /// Constructs a priority class with a given \c power
    priority_class(uint32_t power) : _power(power), _fq(nullptr) {}

    /// Executes the function \c func through this class' \ref fair_queue.
    ///
    /// \throw misconfigured_queue exception if called for an unbound queue
    ///
    /// \return whatever \c func returns
    template <typename Func>
    std::result_of_t<Func()> queue(Func func) {
        if (!_fq) {
            throw misconfigured_queue("Not bound to any fair queue.");
        }

        promise<> pr;
        auto fut = pr.get_future();
        _queue.push(std::move(pr));
        _fq->execute_one();
        return fut.then([func = std::move(func)] {
            return func();
        }).finally([this] {
            _fq->finish_one();
        });
    }
};
/// @}

#include "fair_queue.hh"

void fair_queue::refill_heap(prioq::container_type scanned) {
    for (auto& s: scanned) {
        _handles.push(s);
    }
}

void fair_queue::execute_one() {
    // Explain here why it works. If I am wrong and if it doesn't, will have to re-sort all the time. Sucks
    _sem.wait().then([this] {
        prioq::container_type scanned;
        while (!_handles.empty()) {
            auto h = _handles.top();
            scanned.push_back(h);
            _handles.pop();

            if (!h->_queue.empty()) {
                auto& pr = h->_queue.front();
                pr.set_value();
                h->_queue.pop();
                h->_ops += 1;
                _total_ops++;
                refill_heap(std::move(scanned));
                return make_ready_future<>();;
            }
        }
        throw std::runtime_error("Trying to execute command in empty queue!");
    });
}

void fair_queue::reset_stats() {
    prioq::container_type scanned;
    while (!_handles.empty()) {
        auto h = _handles.top();
        scanned.push_back(h);
        _handles.pop();
        h->_ops = 0;
    }
    _total_ops = 0;
    refill_heap(std::move(scanned));
}

void fair_queue::register_priority_class(priority_class_ptr qh) {
    qh->associate_queue(shared_from_this());
    _handles.push(qh);
    _total_power += qh->_power;
}

bool
fair_queue::qh_compare::operator() (const priority_class_ptr& lhs, const priority_class_ptr &rhs) const {
    assert(lhs->_fq == rhs->_fq);

    auto total_ops = lhs->_fq->_total_ops;
    auto total_power = lhs->_fq->_total_power;

    int32_t fl = lhs->_ops - (lhs->_power * total_ops) / total_power;
    int32_t rl = rhs->_ops - (rhs->_power * total_ops) / total_power;
    return (fl > rl);
}

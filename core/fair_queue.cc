#include "fair_queue.hh"

void fair_queue::refill_heap(prioq::container_type scanned) {
    for (auto& s: scanned) {
        _handles.push(s);
    }
}

void fair_queue::execute_one(unsigned weight) {
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

void fair_queue::reset_stats() {
    prioq::container_type scanned;
    while (!_handles.empty()) {
        auto h = _handles.top();
        scanned.push_back(h);
        _handles.pop();
        h->_ops = 0;
        h->_weight = h->_shares / _total_shares;
    }
    refill_heap(std::move(scanned));
}

void fair_queue::register_priority_class(priority_class_ptr qh) {
    qh->associate_queue(shared_from_this());
    _handles.push(qh);
    _total_shares += qh->_shares;
    reset_stats();
}

bool
fair_queue::class_compare::operator() (const lw_shared_ptr<priority_class>& lhs, const lw_shared_ptr<priority_class>&rhs) const {
    assert(lhs->_fq == rhs->_fq);
    return lhs->_ops < rhs->_ops;
}

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
#pragma once
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/max.hpp>
#include <boost/accumulators/statistics/p_square_quantile.hpp>
#include <boost/array.hpp>
#include <core/scollectd.hh>
#include <core/sstring.hh>

namespace seastar {

/// \addtogroup util-module
/// @{
/// \cond internal
using namespace boost::accumulators;
template <unsigned Percentile>
struct accumulator_core {
    using accumulator_type = accumulator_set<double, stats<tag::p_square_quantile>>;

    accumulator_type new_instance() {
        return accumulator_type(quantile_probability = double(Percentile) / 100);
    }

    double read(const accumulator_type& acc) const {
        return p_square_quantile(acc);
    }

    sstring percentile_sstring() {
        return to_sstring(Percentile);
    }
};

template <>
struct accumulator_core<100> {
    using accumulator_type = accumulator_set<double, stats<tag::max>>;

    accumulator_type new_instance() {
        auto ret = accumulator_type();
        ret(0);
        return ret;
    }

    double read(const accumulator_type& acc) const {
        return max(acc);
    }

    sstring percentile_sstring() {
        return "max";
    }
};

template <unsigned... Percentiles>
struct percentile_accumulator;
/// \endcond

/// \brief accumulates data corresponding to a percentile specified by \c Percentile
///
/// This class implements the p-square estimator for calculating percentile histograms.
/// Internally it uses the boost::accumulator framework, and this wrapper mainly adds two
/// things:
///
/// 1) a period during which the measurements are collected. After calling \ref probe_for_period,
///    the data is reset and a new period starts. This goes well with subsystems like collectd
///    that have a well-defined poll period.
///
/// 2) a specialization for the 100 % percentile. Since the results for boost's p-square estimator
///    are approximate by nature. However, 100 % percentiles (maximum) can be important to analyze
///    one-off events.
template <unsigned Percentile>
struct basic_percentile_accumulator : private accumulator_core<Percentile> {
    /// Reads the current value corresponding to this percentile
    double read() const {
        return accumulator_core<Percentile>::read(_acc);
    }

    /// Returns the value corresponding to this percentile and resets the accumulator.
    double probe_for_period() {
        auto tmp = accumulator_core<Percentile>::new_instance();
        std::swap(_acc, tmp);
        return accumulator_core<Percentile>::read(tmp);
    }

    /// Returns a human-readable \ref sstring describing this percentile
    sstring percentile_sstring() {
        return accumulator_core<Percentile>::percentile_sstring();
    }
private:
    using accumulator_type = typename accumulator_core<Percentile>::accumulator_type;

    void add_measurement(double measurement) {
        _acc(measurement);
    }
    template <unsigned... Percentiles>
    friend struct percentile_accumulator;
    accumulator_type _acc;
};

/// \brief accumulates data corresponding to the percentiles specified in \c Percentiles.
///
/// Example usage:
///
/// seastar::percentile_accumulator<50, 90, 95, 100> acc;
///
/// acc.add_measurement(0.01);
///
/// auto measure = acc.read<95>();
///
///
/// The measurements are added to all percentiles being tracked, meaning the tracking happens in
/// O(#percentiles). In this aspect this class is similar to boost's extended_p_square tag, but
/// our specialization has two important differences:
///
/// 1) As with the \ref basic_percentile_accumulator, it has a well defined period during which
///    the measurements are collected. Keeping the accumulators separate ourselves allow them to
///    have lifetimes detached from one another, which makes using this in infrastructure like
///    collectd easier.
///
/// 2) We can specify all individual accumulators, including "max", with a common interface.
template <unsigned... Percentiles>
struct percentile_accumulator {
    /// Adds the measurement specified at \param measurement to the accumulators being tracked by this accumulator
    void add_measurement(double measurement) {
        for_each_accumulator([&measurement] (auto& acc) { return acc.add_measurement(measurement); });
    }

    /// Returns the value corresponding to the percentile specified by \c Percentile.
    template <unsigned Percentile>
    double read() const {
        return std::get<basic_percentile_accumulator<Percentile>>(_percentiles).read();
    }

    /// Returns the value corresponding to the percentile specified by \c Percentile and resets the accumulator.
    template <unsigned Percentile>
    double probe_for_period() {
        return std::get<basic_percentile_accumulator<Percentile>>(_percentiles).probe_for_period();
    }

    /// Calls the function specified at \param func in each and every individual percentile accumulator being tracked by this accumulator
    template <typename Func>
    void for_each_accumulator(Func&& func) {
        for_each_accumulator_helper(std::forward<Func>(func), std::make_index_sequence<sizeof...(Percentiles)>{});
    }
private:
    std::tuple<basic_percentile_accumulator<Percentiles>...> _percentiles;

    template <typename Func, size_t... Is>
    void for_each_accumulator_helper(Func&& func, std::index_sequence<Is...>) {
        (int[]){0, (func(std::get<Is>(_percentiles)), 0)...};
    }
};
}
/// @}

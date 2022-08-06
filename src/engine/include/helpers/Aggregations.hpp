// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef HELPERS_AGGREGATIONS_HPP_
#define HELPERS_AGGREGATIONS_HPP_

// ------------------------------------------------------------------------

#include <algorithm>
#include <cmath>
#include <map>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

// ------------------------------------------------------------------------

#include "fct/fct.hpp"

// ------------------------------------------------------------------------

#include "helpers/Float.hpp"
#include "helpers/NullChecker.hpp"

namespace helpers {

class Aggregations {
 public:
  /// Throws an exception if not all values are the same.
  template <class IteratorType>
  static Float assert_equal(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    const auto assert_equal = [](const Float init, const Float val) {
      if (init != val) {
        throw std::runtime_error("Values not equal: " + std::to_string(init) +
                                 " vs. " + std::to_string(val) + ".");
      }

      return init;
    };

    return num_agg(_begin, _end, assert_equal, *_begin);
  }

  /// Takes the average of all non-null entries.
  template <class IteratorType>
  static Float avg(IteratorType _begin, IteratorType _end) {
    const auto divisor = count(_begin, _end);

    if (divisor == 0.0) [[unlikely]] {
      return NAN;
    }

    const auto numerator = sum(_begin, _end);

    return numerator / divisor;
  }

  /// Counts all non-null entries.
  template <class IteratorType>
  static Float count(IteratorType _begin, IteratorType _end) {
    const auto count = [](const Float init, const Float val) {
      if (NullChecker::is_null(val)) {
        return init;
      }
      return init + 1.0;
    };

    return num_agg(_begin, _end, count, 0.0);
  }

  /// Counts all values that are strictly greater than the mean.
  template <class IteratorType>
  static Float count_above_mean(IteratorType _begin, IteratorType _end) {
    // Needed to catch numerical instability issues.
    if (all_same(_begin, _end)) [[unlikely]] {
      return 0.0;
    }

    const auto mean = avg(_begin, _end);

    const auto count_op = [mean](const Float init, const Float val) {
      if (val > mean) {
        return init + 1.0;
      }
      return init;
    };

    return num_agg(_begin, _end, count_op, 0.0);
  }

  /// Counts all values that are strictly smaller than the mean.
  template <class IteratorType>
  static Float count_below_mean(IteratorType _begin, IteratorType _end) {
    // Needed to catch numerical instability issues.
    if (all_same(_begin, _end)) [[unlikely]] {
      return 0.0;
    }

    const auto mean = avg(_begin, _end);

    const auto count_op = [mean](const Float init, const Float val) {
      if (val < mean) {
        return init + 1.0;
      }
      return init;
    };

    return num_agg(_begin, _end, count_op, 0.0);
  }

  /// Counts the non-null number of entries.
  template <class IteratorType1, class IteratorType2>
  static Float count_categorical(IteratorType1 _begin, IteratorType2 _end) {
    Float result = 0.0;

    for (auto it = _begin; it != _end; ++it) {
      if (NullChecker::is_null(*it)) {
        continue;
      }

      ++result;
    }

    return result;
  }

  /// Counts the non-null distinct number of entries.
  static Float count_distinct(const std::vector<std::string>& _vec) {
    return count_distinct(_vec.begin(), _vec.end());
  }

  /// Counts the non-null distinct number of entries.
  static Float count_distinct(const std::vector<Int>& _vec) {
    return count_distinct(_vec.begin(), _vec.end());
  }

  /// Counts the distinct number of entries.
  template <class IteratorType1, class IteratorType2>
  static Float count_distinct(IteratorType1 _begin, IteratorType2 _end) {
    using ConstValueType =
        typename std::remove_reference<decltype(*_begin)>::type;

    using ValueType = typename std::remove_const<ConstValueType>::type;

    auto set = std::unordered_set<ValueType>();

    for (auto it = _begin; it != _end; ++it) {
      if (NullChecker::is_null(*it)) {
        continue;
      }

      set.insert(*it);
    }

    return static_cast<Float>(set.size());
  }

  /// Number of distinct elements divided by number of total elements.
  template <class IteratorType>
  static Float count_distinct_over_count(IteratorType _begin,
                                         IteratorType _end) {
    const auto n = count(_begin, _end);

    if (n == 0.0) [[unlikely]] {
      return NAN;
    }

    return count_distinct(_begin, _end) / n;
  }

  /// EWMA is short for exponentially weighted moving average.
  template <class IteratorType>
  static Float ewma(const Float _half_life, IteratorType _begin,
                    IteratorType _end) {
#if (defined(_WIN32) || defined(_WIN64) || defined(__APPLE__))
    const Float log05 = std::log(0.5);
#else
    constexpr Float log05 = std::log(0.5);
#endif

    const auto get_first = [_half_life, log05](const Float _init,
                                               const auto& _pair) -> Float {
      if (NullChecker::is_null(_pair.second)) [[unlikely]] {
        return _init;
      }
      return _init + std::exp(log05 * _pair.first / _half_life);
    };

    const auto sum_first = std::accumulate(_begin, _end, 0.0, get_first);

    if (sum_first == 0.0) {
      return NAN;
    }

    const auto get_both = [_half_life, log05](const Float _init,
                                              const auto& _pair) -> Float {
      if (NullChecker::is_null(_pair.second)) [[unlikely]] {
        return _init;
      }
      return _init + std::exp(log05 * _pair.first / _half_life) * _pair.second;
    };

    const auto sum_both = std::accumulate(_begin, _end, 0.0, get_both);

    return sum_both / sum_first;
  }

  /// Implements the FIRST aggregation. Assumes that the iterator points to a
  /// set of pairs, the first signifying the element over which we want to
  /// sort and the second signifying the value.
  template <class IteratorType>
  static Float first(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto ts_is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      return p1.first < p2.first;
    };

    const auto p = *RANGES::min_element(_begin, _end, ts_is_smaller);

    return p.second;
  }

  /// Takes the kurtosis of all non-null entries.
  template <class IteratorType>
  static Float kurtosis(IteratorType _begin, IteratorType _end) {
    const auto n = count(_begin, _end);

    if (n == 0.0) [[unlikely]] {
      return NAN;
    }

    // Needed to catch numerical instability issues.
    if (all_same(_begin, _end)) [[unlikely]] {
      return 0.0;
    }

    const auto mean = avg(_begin, _end);

    const auto std = stddev(_begin, _end);

    const auto kurt = [mean, std, n](const Float init, const Float val) {
      if (NullChecker::is_null(val)) {
        return init;
      }
      const auto diff = (val - mean) / std;
      return init + diff * diff * diff * diff / n;
    };

    return num_agg(_begin, _end, kurt, 0.0);
  }

  /// Implements the LAST aggregation. Assumes that the iterator points to a
  /// set of pairs, the first signifying the element over which we want to
  /// sort and the second signifying the value.
  template <class IteratorType>
  static Float last(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto ts_is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      return p1.first < p2.first;
    };

    const auto p = *RANGES::max_element(_begin, _end, ts_is_smaller);

    return p.second;
  }

  /// Finds the maximum of all non-null entries.
  template <class IteratorType>
  static Float maximum(IteratorType _begin, IteratorType _end) {
    const auto max_op = [](const Float init, const Float val) {
      return ((val > init || std::isnan(init)) ? val : init);
    };

    return num_agg(_begin, _end, max_op, NAN);
  }

  template <class IteratorType>
  static Float median(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    auto values = std::vector<Float>(_begin, _end);

    std::sort(values.begin(), values.end());

    if (values.size() % 2 == 0) {
      return (values[(values.size() / 2) - 1] + values[values.size() / 2]) /
             2.0;
    }
    return values[values.size() / 2];
  }

  /// Finds the minimum of all non-null entries.
  template <class IteratorType>
  static Float minimum(IteratorType _begin, IteratorType _end) {
    const auto min_op = [](const Float init, const Float val) {
      return ((val < init || std::isnan(init)) ? val : init);
    };

    return num_agg(_begin, _end, min_op, NAN);
  }

  /// Returns the most frequent value.
  template <class T, class IteratorType>
  static T mode(IteratorType _begin, IteratorType _end) {
    const auto freq = count_frequencies<T>(_begin, _end);

    if (freq.size() == 0) [[unlikely]] {
      return NullChecker::make_null<T>();
    }

    using Pair = std::pair<T, size_t>;

    const auto less_frequent = [](const Pair& p1, const Pair& p2) -> bool {
      return p1.second < p2.second;
    };

    return std::max_element(freq.begin(), freq.end(), less_frequent)->first;
  }

  /// Calculates the number of times the maximum value is seen.
  template <class IteratorType>
  static Float num_max(IteratorType _begin, IteratorType _end) {
    const auto max = maximum(_begin, _end);

    if (std::isnan(max)) [[unlikely]] {
      return 0.0;
    }

    const auto count_op = [max](const Float init, const Float val) {
      if (val == max) {
        return init + 1.0;
      }
      return init;
    };

    return num_agg(_begin, _end, count_op, 0.0);
  }

  /// Calculates the number of times the minimum value is seen.
  template <class IteratorType>
  static Float num_min(IteratorType _begin, IteratorType _end) {
    const auto min = minimum(_begin, _end);

    if (std::isnan(min)) [[unlikely]] {
      return 0.0;
    }

    const auto count_op = [min](const Float init, const Float val) {
      if (val == min) {
        return init + 1.0;
      }
      return init;
    };

    return num_agg(_begin, _end, count_op, 0.0);
  }

  /// Returns the quantile designated by _q.
  template <class IteratorType>
  static Float quantile(const Float _q, IteratorType _begin,
                        IteratorType _end) {
    assert_true(_q >= 0.0);

    assert_true(_q <= 1.0);

    auto values = std::vector<Float>(_begin, _end);

    if (values.size() == 0) [[unlikely]] {
      return NAN;
    }

    RANGES::sort(values);

    const auto ix_float = static_cast<Float>(values.size() - 1) * _q;

    const auto ix = static_cast<size_t>(ix_float);

    if (ix == values.size() - 1) [[unlikely]] {
      return values[ix];
    }

    const auto share = ix_float - static_cast<Float>(ix);

    return values[ix + 1] * share + values[ix] * (1.0 - share);
  }

  /// Takes the skewness of all non-null entries.
  template <class IteratorType>
  static Float skew(IteratorType _begin, IteratorType _end) {
    const auto n = count(_begin, _end);

    if (n == 0.0) [[unlikely]] {
      return NAN;
    }

    // Needed to catch numerical instability issues.
    if (all_same(_begin, _end)) [[unlikely]] {
      return 0.0;
    }

    const auto mean = avg(_begin, _end);

    const auto std = stddev(_begin, _end);

    const auto skewness = [mean, std, n](const Float init, const Float val) {
      if (NullChecker::is_null(val)) [[unlikely]] {
        return init;
      }
      const auto diff = (val - mean) / std;
      return init + diff * diff * diff / n;
    };

    return num_agg(_begin, _end, skewness, 0.0);
  }

  /// Takes the standard deviation of all non-null entries.
  template <class IteratorType>
  static Float stddev(IteratorType _begin, IteratorType _end) {
    return std::sqrt(var(_begin, _end));
  }

  /// Takes the sum of all non-null entries.
  template <class IteratorType>
  static Float sum(IteratorType _begin, IteratorType _end) {
    const auto sum = [](const Float init, const Float val) {
      if (NullChecker::is_null(val)) [[unlikely]] {
        return init;
      }
      return init + val;
    };

    return num_agg(_begin, _end, sum, 0.0);
  }

  /// Implements the time_since_first_maximum aggregation.
  /// Assumes that the iterator points to a
  /// set of pairs, the first signifying the time difference and
  /// the second signifying the value.
  template <class IteratorType>
  static Float time_since_first_maximum(IteratorType _begin,
                                        IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      if (p1.second == p2.second) [[unlikely]] {
        return p1.first < p2.first;
      }
      return p1.second < p2.second;
    };

    const auto p = *RANGES::max_element(_begin, _end, is_smaller);

    return p.first;
  }

  /// Implements the time_since_first_minimum aggregation.
  /// Assumes that the iterator points to a
  /// set of pairs, the first signifying the time difference and
  /// the second signifying the value.
  template <class IteratorType>
  static Float time_since_first_minimum(IteratorType _begin,
                                        IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      if (p1.second == p2.second) [[unlikely]] {
        return p1.first > p2.first;
      }
      return p1.second < p2.second;
    };

    const auto p = *RANGES::min_element(_begin, _end, is_smaller);

    return p.first;
  }

  /// Implements the time_since_last_maximum aggregation.
  /// Assumes that the iterator points to a
  /// set of pairs, the first signifying the time difference and
  /// the second signifying the value.
  template <class IteratorType>
  static Float time_since_last_maximum(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      if (p1.second == p2.second) [[unlikely]] {
        return p1.first > p2.first;
      }
      return p1.second < p2.second;
    };

    const auto p = *RANGES::max_element(_begin, _end, is_smaller);

    return p.first;
  }

  /// Implements the time_since_first_minimum aggregation.
  /// Assumes that the iterator points to a
  /// set of pairs, the first signifying the time difference and
  /// the second signifying the value.
  template <class IteratorType>
  static Float time_since_last_minimum(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 0) [[unlikely]] {
      return NAN;
    }

    using Pair = std::pair<Float, Float>;

    const auto is_smaller = [](const Pair& p1, const Pair& p2) -> Float {
      if (p1.second == p2.second) [[unlikely]] {
        return p1.first < p2.first;
      }
      return p1.second < p2.second;
    };

    const auto p = *RANGES::min_element(_begin, _end, is_smaller);

    return p.first;
  }

  /// trend extracts a linear trend from the variable.
  template <class IteratorType>
  static Float trend(IteratorType _begin, IteratorType _end) {
    const auto second_is_not_null = [](const auto& _pair) -> bool {
      return !NullChecker::is_null(_pair.second);
    };

    const auto get_first = [](const auto& _pair) -> Float {
      return _pair.first;
    };

    const auto get_second = [](const auto& _pair) -> Float {
      return _pair.second;
    };

    auto range = fct::Range(_begin, _end);

    auto range_x =
        range | VIEWS::filter(second_is_not_null) | VIEWS::transform(get_first);

    auto range_y = range | VIEWS::filter(second_is_not_null) |
                   VIEWS::transform(get_second);

    const auto mean_x = avg(range_x.begin(), range_x.end());

    const auto mean_y = avg(range_y.begin(), range_y.end());

    const auto calc_xx = [mean_x](const Float _init, const Float _x) -> Float {
      const auto x_centered = _x - mean_x;
      return _init + x_centered * x_centered;
    };

    const auto xx =
        std::accumulate(range_x.begin(), range_x.end(), 0.0, calc_xx);

    if (xx == 0.0) {
      return mean_y;
    }

    const auto calc_xy = [mean_x, mean_y](const Float _init,
                                          const auto& _pair) -> Float {
      const auto x_centered = _pair.first - mean_x;
      const auto y_centered = _pair.second - mean_y;
      return _init + x_centered * y_centered;
    };

    auto range_xy = range | VIEWS::filter(second_is_not_null);

    const auto xy =
        std::accumulate(range_xy.begin(), range_xy.end(), 0.0, calc_xy);

    const auto beta = xy / xx;

    return mean_y - mean_x * beta;
  }

  /// Takes the variance of all non-null entries.
  template <class IteratorType>
  static Float var(IteratorType _begin, IteratorType _end) {
    const auto n = count(_begin, _end);

    if (n == 0.0) {
      return NAN;
    }

    const auto mean = avg(_begin, _end);

    const auto variance = [mean, n](const Float init, const Float val) {
      if (NullChecker::is_null(val)) [[unlikely]] {
        return init;
      }
      const auto diff = val - mean;
      return init + diff * diff / n;
    };

    return num_agg(_begin, _end, variance, 0.0);
  }

  /// Variance over mean.
  template <class IteratorType>
  static Float variation_coefficient(IteratorType _begin, IteratorType _end) {
    const auto mean = avg(_begin, _end);

    if (NullChecker::is_null(mean) || mean == 0.0) [[unlikely]] {
      return NAN;
    }

    const auto variance = var(_begin, _end);

    return variance / mean;
  }

  // ------------------------------------------------------------------------

 private:
  /// Maps each unique element onto its frequencies.
  template <class T, class IteratorType>
  static std::map<T, size_t> count_frequencies(IteratorType _begin,
                                               IteratorType _end) {
    std::map<T, size_t> freq_map;

    for (auto key = _begin; key != _end; ++key) {
      if (NullChecker::is_null(*key)) {
        continue;
      }

      const auto it = freq_map.find(*key);

      if (it == freq_map.end()) {
        freq_map[*key] = 1;
      } else {
        it->second++;
      }
    }

    return freq_map;
  }

  /// Whether all values are the same.
  template <class IteratorType>
  static bool all_same(IteratorType _begin, IteratorType _end) {
    if (std::distance(_begin, _end) <= 1) {
      return true;
    }

    auto it = _begin;

    const auto val = *it;

    ++it;

    for (; it != _end; ++it) {
      if (*it != val) {
        return false;
      }
    }

    return true;
  }

  /// Undertakes a numerical aggregation based on the template class
  /// Aggregation.
  template <class Aggregation, class IteratorType>
  static Float num_agg(IteratorType _begin, IteratorType _end,
                       const Aggregation& _agg, const Float _init) {
    return std::accumulate(_begin, _end, _init, _agg);
  }

  // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_AGGREGATIONS_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_MATCHMAKER_HPP_
#define HELPERS_MATCHMAKER_HPP_

#include <cmath>
#include <memory>
#include <vector>

#include "helpers/DataFrame.hpp"
#include "helpers/Float.hpp"

namespace helpers {

template <class PopulationType, class MatchType, class MakeMatchType>
class Matchmaker {
 public:
  /// Identifies matches between population table and peripheral tables.
  static std::vector<MatchType> make_matches(
      const PopulationType& _population, const DataFrame& _peripheral,
      const std::shared_ptr<const std::vector<Float>>& _sample_weights,
      const MakeMatchType _make_match);

  /// Identifies matches between a specific sample in the population table
  /// (signified by _ix_output and peripheral tables.
  static void make_matches(const PopulationType& _population,
                           const DataFrame& _peripheral,
                           const size_t _ix_output,
                           const MakeMatchType _make_match,
                           std::vector<MatchType>* _matches);

 private:
  /// If we don't have a ts_index, we need to go through the matches one by one
  /// and see whether they are in range.
  static void make_matches_using_linear_method(
      const DataFrame& _peripheral, const MakeMatchType _make_match,
      const size_t _ix_output, const Int _join_key, const Float _time_stamp_out,
      std::vector<MatchType>* _matches);

 private:
  /// When we have a ts_index, we can rely on that (usually that is more
  /// efficient).
  static void make_matches_using_ts_index(const DataFrame& _peripheral,
                                          const MakeMatchType _make_match,
                                          const size_t _ix_output,
                                          const Int _join_key,
                                          const Float _time_stamp_out,
                                          std::vector<MatchType>* _matches) {
    assert_true(_peripheral.ts_index_);
    const auto range =
        _peripheral.ts_index_->find_range(_join_key, _time_stamp_out);
    for (const size_t ix_input : range) {
      _matches->push_back(_make_match(ix_input, _ix_output));
    }
  }
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
std::vector<MatchType>
Matchmaker<PopulationType, MatchType, MakeMatchType>::make_matches(
    const PopulationType& _population, const DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<Float>>& _sample_weights,
    const MakeMatchType _make_match) {
  std::vector<MatchType> matches;

  for (size_t ix_output = 0; ix_output < _population.nrows(); ++ix_output) {
    if (_sample_weights) {
      assert_true(_sample_weights->size() == _population.nrows());
      if ((*_sample_weights)[ix_output] <= 0.0) {
        continue;
      }
    }

    Matchmaker::make_matches(_population, _peripheral, ix_output, _make_match,
                             &matches);
  }

  return matches;
}

// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
void Matchmaker<PopulationType, MatchType, MakeMatchType>::make_matches(
    const PopulationType& _population, const DataFrame& _peripheral,
    const size_t _ix_output, const MakeMatchType _make_match,
    std::vector<MatchType>* _matches) {
  const auto join_key = _population.join_key(_ix_output);

  const auto time_stamp_out = _population.time_stamp(_ix_output);

  if (_peripheral.has(join_key)) {
    if (_peripheral.ts_index_) {
      make_matches_using_ts_index(_peripheral, _make_match, _ix_output,
                                  join_key, time_stamp_out, _matches);
    } else {
      make_matches_using_linear_method(_peripheral, _make_match, _ix_output,
                                       join_key, time_stamp_out, _matches);
    }
  }
}

// ------------------------------------------------------------------------

template <class PopulationType, class MatchType, class MakeMatchType>
void Matchmaker<PopulationType, MatchType, MakeMatchType>::
    make_matches_using_linear_method(const DataFrame& _peripheral,
                                     const MakeMatchType _make_match,
                                     const size_t _ix_output,
                                     const Int _join_key,
                                     const Float _time_stamp_out,
                                     std::vector<MatchType>* _matches) {
  const auto [begin, end] = _peripheral.find(_join_key);

  for (auto it = begin; it != end; ++it) {
    const auto ix_input = *it;

    const auto lower = _peripheral.time_stamp(ix_input);

    const auto upper = _peripheral.upper_time_stamp(ix_input);

    const bool match_in_range = lower <= _time_stamp_out &&
                                (std::isnan(upper) || upper > _time_stamp_out);

    if (match_in_range) {
      _matches->push_back(_make_match(ix_input, _ix_output));
    }
  }
}

// ----------------------------------------------------------------------------

}  // namespace helpers

#endif  // HELPERS_MATCHMAKER_HPP_

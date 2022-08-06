// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FASTPROP_ALGORITHM_CONDITIONPARSER_HPP_
#define FASTPROP_ALGORITHM_CONDITIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <vector>

// ----------------------------------------------------------------------------

#include "fastprop/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "fastprop/algorithm/TableHolder.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace algorithm {
// ------------------------------------------------------------------------

class ConditionParser {
 public:
  /// Generates a vector of lambda function each of which determines whether a
  /// match is to be included in the aggregation based on the
  /// abstract_feature's conditions.
  static std::vector<std::function<bool(const containers::Match &)>>
  make_condition_functions(
      const TableHolder &_table_holder, const std::vector<size_t> &_index,
      const std::vector<containers::AbstractFeature> &_abstract_features);

 private:
  /// Generates a lambda function that determines whether a match is to be
  /// included in the aggregation based on the abstract_feature's conditions.
  static std::function<bool(const containers::Match &)> make_apply_conditions(
      const TableHolder &_table_holder,
      const containers::AbstractFeature &_abstract_feature);

  /// Generates a filtering function based on a categorical column.
  static std::function<bool(const containers::Match &)> make_categorical(
      const containers::DataFrame &_peripheral,
      const containers::Condition &_condition);

  /// Generates a filtering function based on lags.
  static std::function<bool(const containers::Match &)> make_lag(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const containers::Condition &_condition);

  /// Generates a filtering function based on same_units_categorical.
  static std::function<bool(const containers::Match &)>
  make_same_units_categorical(const containers::DataFrame &_population,
                              const containers::DataFrame &_peripheral,
                              const containers::Condition &_condition);

  /// Parses all conditions in the abstract features.
  static std::vector<std::function<bool(const containers::Match &)>>
  parse_conditions(const TableHolder &_table_holder,
                   const containers::AbstractFeature &_abstract_feature);

  /// Parses one condition in the abstract features.
  static std::function<bool(const containers::Match &)> parse_single_condition(
      const containers::DataFrame &_population,
      const containers::DataFrame &_peripheral,
      const containers::Condition &_condition);
};

// ------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

#endif  // FASTPROP_ALGORITHM_CONDITIONPARSER_HPP_

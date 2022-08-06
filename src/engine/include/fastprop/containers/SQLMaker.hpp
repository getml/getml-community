// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef FASTPROP_CONTAINERS_SQLMAKER_HPP_
#define FASTPROP_CONTAINERS_SQLMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <utility>

// ----------------------------------------------------------------------------

#include "fastprop/enums/enums.hpp"
#include "helpers/helpers.hpp"
#include "transpilation/transpilation.hpp"

// ----------------------------------------------------------------------------

#include "fastprop/containers/AbstractFeature.hpp"

// ----------------------------------------------------------------------------

namespace fastprop {
namespace containers {

class SQLMaker {
 public:
  typedef typename AbstractFeature::VocabForDf VocabForDf;

  typedef std::vector<VocabForDf> Vocabulary;

 public:
  SQLMaker(const helpers::StringIterator& _categories,
           const std::string& _feature_prefix, const helpers::Schema& _input,
           const helpers::Schema& _output,
           const std::shared_ptr<const transpilation::SQLDialectGenerator>&
               _sql_dialect_generator)
      : categories_(_categories),
        feature_prefix_(_feature_prefix),
        input_(_input),
        output_(_output),
        sql_dialect_generator_(_sql_dialect_generator) {
    assert_true(sql_dialect_generator_);
  }

  ~SQLMaker() = default;

 public:
  /// Creates a condition.
  std::string condition(const Condition& _condition) const;

  /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
  /// AGGREGATED)).
  std::string select_statement(const AbstractFeature& _abstract_feature) const;

  /// Creates the value to be aggregated (for instance a column name or the
  /// difference between two columns)
  std::string value_to_be_aggregated(
      const AbstractFeature& _abstract_feature) const;

 public:
  /// Whether the aggregation is an aggregation that relies on the
  /// first-last-logic.
  static bool is_first_last(const enums::Aggregation _agg) {
    return (_agg == enums::Aggregation::first ||
            _agg == enums::Aggregation::last ||
            _agg == enums::Aggregation::ewma1s ||
            _agg == enums::Aggregation::ewma1m ||
            _agg == enums::Aggregation::ewma1h ||
            _agg == enums::Aggregation::ewma1d ||
            _agg == enums::Aggregation::ewma7d ||
            _agg == enums::Aggregation::ewma30d ||
            _agg == enums::Aggregation::ewma90d ||
            _agg == enums::Aggregation::ewma365d ||
            _agg == enums::Aggregation::time_since_first_maximum ||
            _agg == enums::Aggregation::time_since_first_minimum ||
            _agg == enums::Aggregation::time_since_last_maximum ||
            _agg == enums::Aggregation::time_since_last_minimum ||
            _agg == enums::Aggregation::trend);
  }

 private:
  /// Returns the column name signified by _column_used and _data_used.
  std::string get_name(const enums::DataUsed _data_used,
                       const size_t _peripheral, const size_t _input_col,
                       const size_t _output_col) const;

  /// Returns the column names signified by _column_used and _data_used, for
  /// same_units_...
  std::pair<std::string, std::string> get_same_units(
      const enums::DataUsed _data_used, const size_t _input_col,
      const size_t _output_col) const;

  /// Generates an additional argument passed to the aggregation function.
  std::string make_additional_argument(
      const enums::Aggregation& _aggregation) const;

  /// Returns the select statement for AVG_TIME_BETWEEN.
  std::string select_avg_time_between() const;

 private:
  /// Trivial (const private) accessor.
  const helpers::StringIterator& categories() const { return categories_; }

 private:
  /// The categorical encoding.
  const helpers::StringIterator categories_;

  /// The prefix to be used for each feature.
  const std::string feature_prefix_;

  /// The Schema of the input table.
  const helpers::Schema input_;

  /// The Schema of the output table.
  const helpers::Schema output_;

  /// Generates the right code for the required SQL dialect.
  const std::shared_ptr<const transpilation::SQLDialectGenerator>
      sql_dialect_generator_;
};

// ------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

// ----------------------------------------------------------------------------

#endif  // FASTPROP_CONTAINERS_SQLMAKER_HPP_

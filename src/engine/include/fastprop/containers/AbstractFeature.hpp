// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_
#define FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_

#include "fastprop/Int.hpp"
#include "fastprop/containers/Condition.hpp"
#include "fastprop/enums/Aggregation.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

#include <memory>
#include <vector>

namespace fastprop {
namespace containers {

struct AbstractFeature {
  using ReflectionType =
      rfl::NamedTuple<rfl::Field<"aggregation_", enums::Aggregation>,
                      rfl::Field<"categorical_value_", Int>,
                      rfl::Field<"conditions_", std::vector<Condition>>,
                      rfl::Field<"data_used_", enums::DataUsed>,
                      rfl::Field<"input_col_", size_t>,
                      rfl::Field<"output_col_", size_t>,
                      rfl::Field<"peripheral_", size_t>>;

  static constexpr Int NO_CATEGORICAL_VALUE = -1;

  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

  typedef std::vector<VocabForDf> Vocabulary;

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const enums::DataUsed _data_used, const size_t _input_col,
                  const size_t _output_col, const size_t _peripheral);

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const enums::DataUsed _data_used, const size_t _input_col,
                  const size_t _peripheral);

  AbstractFeature(const enums::Aggregation _aggregation,
                  const std::vector<Condition> &_conditions,
                  const size_t _input_col, const size_t _peripheral,
                  const enums::DataUsed _data_used,
                  const Int _categorical_value);

  AbstractFeature(const ReflectionType &_obj);

  ~AbstractFeature();

  /// Necessary for the deserialization to work.
  ReflectionType reflection() const;

  /// Expresses the abstract feature as SQL code.
  std::string to_sql(
      const helpers::StringIterator &_categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>
          &_sql_dialect_generator,
      const std::string &_feature_prefix, const std::string &_feature_num,
      const helpers::Schema &_input, const helpers::Schema &_output) const;

  /// The aggregation to apply
  const enums::Aggregation aggregation_;

  /// The value to which we compare the categorical column.
  const Int categorical_value_;

  /// The conditions applied to the aggregation.
  const std::vector<Condition> conditions_;

  /// The kind of data used
  const enums::DataUsed data_used_;

  /// The colnum of the column in the input table
  const size_t input_col_;

  /// The colnum of the column in the output table (only relevant for
  /// same_unit features).
  const size_t output_col_;

  /// The number of the peripheral table used
  const size_t peripheral_;
};

}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_ABSTRACTFEATURE_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_CONTAINERS_CONDITION_HPP_
#define FASTPROP_CONTAINERS_CONDITION_HPP_

#include <cstddef>
#include <memory>
#include <string>

#include "fastprop/Float.hpp"
#include "fastprop/Int.hpp"
#include "fastprop/enums/enums.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "helpers/helpers.hpp"
#include "transpilation/transpilation.hpp"

namespace fastprop {
namespace containers {

struct Condition {
  using NamedTupleType =
      fct::NamedTuple<fct::Field<"bound_lower_", std::optional<Float>>,
                      fct::Field<"bound_upper_", std::optional<Float>>,
                      fct::Field<"category_used_", Int>,
                      fct::Field<"data_used_", enums::DataUsed>,
                      fct::Field<"input_col_", size_t>,
                      fct::Field<"output_col_", size_t>,
                      fct::Field<"peripheral_", size_t>>;

  Condition(const enums::DataUsed _data_used, const size_t _input_col,
            const size_t _output_col, const size_t _peripheral);

  Condition(const Int _category_used, const enums::DataUsed _data_used,
            const size_t _input_col, const size_t _peripheral);

  Condition(const Float _bound_lower, const Float _bound_upper,
            const enums::DataUsed _data_used, const size_t _peripheral);

  explicit Condition(const NamedTupleType &_obj);

  ~Condition();

  /// Necessary for the automated serialization.
  NamedTupleType named_tuple() const;

  /// Expresses the abstract feature as SQL code.
  std::string to_sql(
      const helpers::StringIterator &_categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>
          &_sql_dialect_generator,
      const std::string &_feature_prefix, const helpers::Schema &_input,
      const helpers::Schema &_output) const;

  /// Equality operator
  bool operator==(const Condition &_other) const {
    return (
        bound_lower_ == _other.bound_lower_ &&
        bound_upper_ == _other.bound_upper_ &&
        category_used_ == _other.category_used_ &&
        data_used_ == _other.data_used_ && input_col_ == _other.input_col_ &&
        output_col_ == _other.output_col_ && peripheral_ == _other.peripheral_);
  }

  /// Non-equality operator
  bool operator!=(const Condition &_other) const { return !(*this == _other); }

  /// The lower bound (when data_used_ == lag).
  const Float bound_lower_;

  /// The upper bound (when data_used_ == lag).
  const Float bound_upper_;

  /// The category used (when data_used_ == categorical).
  const Int category_used_;

  /// The kind of data used
  const enums::DataUsed data_used_;

  /// The colnum of the column in the input table
  const size_t input_col_;

  /// The colnum of the column in the output table
  const size_t output_col_;

  /// The number of the peripheral table used
  const size_t peripheral_;
};

}  // namespace containers
}  // namespace fastprop

#endif  // FASTPROP_CONTAINERS_CONDITION_HPP_

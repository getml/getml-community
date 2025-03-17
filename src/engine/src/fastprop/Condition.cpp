// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fastprop/containers/Condition.hpp"

#include "fastprop/containers/SQLMaker.hpp"

namespace fastprop {
namespace containers {

Condition::Condition(const enums::DataUsed _data_used, const size_t _input_col,
                     const size_t _output_col, const size_t _peripheral)
    : bound_lower_(0.0),
      bound_upper_(0.0),
      category_used_(-1),
      data_used_(_data_used),
      input_col_(_input_col),
      output_col_(_output_col),
      peripheral_(_peripheral) {
  assert_true(_data_used.value() ==
              enums::DataUsed::value_of<"same_units_categorical">());
}

// ----------------------------------------------------------------------------

Condition::Condition(const Float _bound_lower, const Float _bound_upper,
                     const enums::DataUsed _data_used, const size_t _peripheral)
    : bound_lower_(_bound_lower),
      bound_upper_(_bound_upper),
      category_used_(-1),
      data_used_(_data_used),
      input_col_(0),
      output_col_(0),
      peripheral_(_peripheral) {
  assert_true(_data_used.value() == enums::DataUsed::value_of<"lag">());
}
// ----------------------------------------------------------------------------

Condition::Condition(const Int _category_used, const enums::DataUsed _data_used,
                     const size_t _input_col, const size_t _peripheral)
    : bound_lower_(0.0),
      bound_upper_(0.0),
      category_used_(_category_used),
      data_used_(_data_used),
      input_col_(_input_col),
      output_col_(0),
      peripheral_(_peripheral) {
  assert_true(_data_used.value() == enums::DataUsed::value_of<"categorical">());
}

// ----------------------------------------------------------------------------

Condition::Condition(const ReflectionType &_obj)
    : bound_lower_(_obj.get<"bound_lower_">().value_or(0.0)),
      bound_upper_(_obj.get<"bound_upper_">().value_or(0.0)),
      category_used_(_obj.get<"category_used_">()),
      data_used_(_obj.get<"data_used_">()),
      input_col_(_obj.get<"input_col_">()),
      output_col_(_obj.get<"output_col_">()),
      peripheral_(_obj.get<"peripheral_">()) {}

// ----------------------------------------------------------------------------

typename Condition::ReflectionType Condition::reflection() const {
  return rfl::make_field<"bound_lower_">(bound_lower_) *
         rfl::make_field<"bound_upper_">(bound_upper_) *
         rfl::make_field<"category_used_">(category_used_) *
         rfl::make_field<"data_used_">(data_used_) *
         rfl::make_field<"input_col_">(input_col_) *
         rfl::make_field<"output_col_">(output_col_) *
         rfl::make_field<"peripheral_">(peripheral_);
}

// ----------------------------------------------------------------------------

std::string Condition::to_sql(
    const helpers::StringIterator &_categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix, const helpers::Schema &_input,
    const helpers::Schema &_output) const {
  return SQLMaker(_categories, _feature_prefix, _input, _output,
                  _sql_dialect_generator)
      .condition(*this);
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

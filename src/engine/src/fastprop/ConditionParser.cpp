// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "fastprop/algorithm/ConditionParser.hpp"

namespace fastprop {
namespace algorithm {
// ----------------------------------------------------------------------------

std::vector<std::function<bool(const containers::Match &)>>
ConditionParser::make_condition_functions(
    const TableHolder &_table_holder, const std::vector<size_t> &_index,
    const std::vector<containers::AbstractFeature> &_abstract_features) {
  const auto make_function = [&_table_holder,
                              &_abstract_features](const size_t ix) {
    assert_true(ix < _abstract_features.size());
    return ConditionParser::make_apply_conditions(_table_holder,
                                                  _abstract_features.at(ix));
  };

  auto range = _index | VIEWS::transform(make_function);

  return std::vector<std::function<bool(const containers::Match &)>>(
      range.begin(), range.end());
}

// ----------------------------------------------------------------------------

std::function<bool(const containers::Match &)>
ConditionParser::make_apply_conditions(
    const TableHolder &_table_holder,
    const containers::AbstractFeature &_abstract_feature) {
  const auto conditions = parse_conditions(_table_holder, _abstract_feature);

  return [conditions](const containers::Match &match) -> bool {
    for (const auto &cond : conditions) {
      if (!cond(match)) {
        return false;
      }
    }

    return true;
  };
}

// ----------------------------------------------------------------------------

std::function<bool(const containers::Match &)>
ConditionParser::make_categorical(const containers::DataFrame &_peripheral,
                                  const containers::Condition &_condition) {
  assert_true(_condition.input_col_ < _peripheral.num_categoricals());

  const auto col = _peripheral.categorical_col(_condition.input_col_);

  const auto category_used = _condition.category_used_;

  return [col, category_used](const containers::Match &match) -> bool {
    return (col[match.ix_input] == category_used);
  };
}

// ----------------------------------------------------------------------------

std::function<bool(const containers::Match &)> ConditionParser::make_lag(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Condition &_condition) {
  assert_true(_population.num_time_stamps() > 0);

  assert_true(_peripheral.num_time_stamps() > 0);

  const auto col1 = _population.time_stamp_col();

  const auto col2 = _peripheral.time_stamp_col();

  const auto lower = _condition.bound_lower_;

  const auto upper = _condition.bound_upper_;

  return [col1, col2, lower, upper](const containers::Match &match) -> bool {
    return (col2[match.ix_input] + upper > col1[match.ix_output]) &&
           (col2[match.ix_input] + lower <= col1[match.ix_output]);
  };
}

// ----------------------------------------------------------------------------

std::function<bool(const containers::Match &)>
ConditionParser::make_same_units_categorical(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Condition &_condition) {
  assert_true(_condition.input_col_ < _peripheral.num_categoricals());

  assert_true(_condition.output_col_ < _population.num_categoricals());

  const auto col1 = _population.categorical_col(_condition.output_col_);

  const auto col2 = _peripheral.categorical_col(_condition.input_col_);

  return [col1, col2](const containers::Match &match) -> bool {
    return (col1[match.ix_output] == col2[match.ix_input]);
  };
}

// ----------------------------------------------------------------------------

std::vector<std::function<bool(const containers::Match &)>>
ConditionParser::parse_conditions(
    const TableHolder &_table_holder,
    const containers::AbstractFeature &_abstract_feature) {
  assert_true(_table_holder.main_tables().size() ==
              _table_holder.peripheral_tables().size());

  assert_true(_abstract_feature.peripheral_ <
              _table_holder.main_tables().size());

  const auto &population =
      _table_holder.main_tables().at(_abstract_feature.peripheral_).df();

  const auto &peripheral =
      _table_holder.peripheral_tables().at(_abstract_feature.peripheral_);

  const auto parse = [_abstract_feature, &population,
                      &peripheral](const containers::Condition &cond)
      -> std::function<bool(const containers::Match &)> {
    assert_true(cond.peripheral_ == _abstract_feature.peripheral_);
    return ConditionParser::parse_single_condition(population, peripheral,
                                                   cond);
  };

  auto range = _abstract_feature.conditions_ | VIEWS::transform(parse);

  return std::vector<std::function<bool(const containers::Match &)>>(
      range.begin(), range.end());
}

// ----------------------------------------------------------------------------

std::function<bool(const containers::Match &)>
ConditionParser::parse_single_condition(
    const containers::DataFrame &_population,
    const containers::DataFrame &_peripheral,
    const containers::Condition &_condition) {
  switch (_condition.data_used_) {
    case enums::DataUsed::categorical:
      return make_categorical(_peripheral, _condition);

    case enums::DataUsed::lag:
      return make_lag(_population, _peripheral, _condition);

    case enums::DataUsed::same_units_categorical:
      return make_same_units_categorical(_population, _peripheral, _condition);

    default:
      throw_unless(false, "Unknown condition");
      return [](const containers::Match &match) { return true; };
  }
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

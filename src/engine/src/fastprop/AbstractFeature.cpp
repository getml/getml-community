// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fastprop/containers/AbstractFeature.hpp"

#include "fastprop/containers/SQLMaker.hpp"

#include <sstream>

namespace fastprop {
namespace containers {

AbstractFeature::AbstractFeature(const enums::Aggregation _aggregation,
                                 const std::vector<Condition> &_conditions,
                                 const enums::DataUsed _data_used,
                                 const size_t _input_col,
                                 const size_t _output_col,
                                 const size_t _peripheral)
    : aggregation_(_aggregation),
      categorical_value_(NO_CATEGORICAL_VALUE),
      conditions_(_conditions),
      data_used_(_data_used),
      input_col_(_input_col),
      output_col_(_output_col),
      peripheral_(_peripheral) {}

// ----------------------------------------------------------------------------

AbstractFeature::AbstractFeature(const enums::Aggregation _aggregation,
                                 const std::vector<Condition> &_conditions,
                                 const enums::DataUsed _data_used,
                                 const size_t _input_col,
                                 const size_t _peripheral)
    : AbstractFeature(_aggregation, _conditions, _data_used, _input_col, 0,
                      _peripheral) {}

// ----------------------------------------------------------------------------

AbstractFeature::AbstractFeature(const enums::Aggregation _aggregation,
                                 const std::vector<Condition> &_conditions,
                                 const size_t _input_col,
                                 const size_t _peripheral,
                                 const enums::DataUsed _data_used,
                                 const Int _categorical_value)
    : aggregation_(_aggregation),
      categorical_value_(_categorical_value),
      conditions_(_conditions),
      data_used_(_data_used),
      input_col_(_input_col),
      output_col_(0),
      peripheral_(_peripheral) {
  assert_true(_categorical_value >= 0);
}

// ----------------------------------------------------------------------------

AbstractFeature::AbstractFeature(const ReflectionType &_obj)
    : aggregation_(_obj.get<"aggregation_">()),
      categorical_value_(_obj.get<"categorical_value_">()),
      conditions_(_obj.get<"conditions_">()),
      data_used_(_obj.get<"data_used_">()),
      input_col_(_obj.get<"input_col_">()),
      output_col_(_obj.get<"output_col_">()),
      peripheral_(_obj.get<"peripheral_">()) {}

// ----------------------------------------------------------------------------

typename AbstractFeature::ReflectionType AbstractFeature::reflection() const {
  return rfl::make_field<"aggregation_">(aggregation_) *
         rfl::make_field<"categorical_value_">(categorical_value_) *
         rfl::make_field<"conditions_">(conditions_) *
         rfl::make_field<"data_used_">(data_used_) *
         rfl::make_field<"input_col_">(input_col_) *
         rfl::make_field<"output_col_">(output_col_) *
         rfl::make_field<"peripheral_">(peripheral_);
}

// ----------------------------------------------------------------------------

std::string AbstractFeature::to_sql(
    const helpers::StringIterator &_categories,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix, const std::string &_feature_num,
    const helpers::Schema &_input, const helpers::Schema &_output) const {
  assert_true(_sql_dialect_generator);

  const auto quote1 = _sql_dialect_generator->quotechar1();
  const auto quote2 = _sql_dialect_generator->quotechar2();

  const auto sql_maker = SQLMaker(_categories, _feature_prefix, _input, _output,
                                  _sql_dialect_generator);

  std::stringstream sql;

  sql << _sql_dialect_generator->drop_table_if_exists(
      "FEATURE_" + _feature_prefix + _feature_num);

  sql << _sql_dialect_generator->create_table(aggregation_, _feature_prefix,
                                              _feature_num);

  sql << "SELECT ";

  sql << sql_maker.select_statement(*this);

  sql << " AS " << quote1 << "feature_" << _feature_prefix << _feature_num
      << quote2 << "," << std::endl;

  sql << "       t1." << _sql_dialect_generator->rowid() << " AS "
      << _sql_dialect_generator->rownum() << std::endl;

  assert_true(_output.join_keys().size() == 1);

  assert_true(_input.join_keys().size() == 1);

  sql << _sql_dialect_generator->make_joins(_output.name(), _input.name(),
                                            _output.join_keys_name(),
                                            _input.join_keys_name());

  if (data_used_.value() == enums::DataUsed::value_of<"subfeatures">()) {
    const auto number = _feature_prefix + std::to_string(peripheral_ + 1) +
                        "_" + std::to_string(input_col_ + 1);

    sql << "LEFT JOIN " << _sql_dialect_generator->schema() << quote1
        << "FEATURE_" << number << quote2 << " f_" << number << std::endl;

    sql << "ON t2." << _sql_dialect_generator->rowid() << " = f_" << number
        << "." << _sql_dialect_generator->rownum() << std::endl;
  }

  const bool use_time_stamps =
      (_input.num_time_stamps() > 0 && _output.num_time_stamps() > 0);

  if (use_time_stamps) {
    sql << "WHERE ";

    const auto upper_ts = _input.num_time_stamps() > 1
                              ? _input.upper_time_stamps_name()
                              : std::string("");

    sql << _sql_dialect_generator->make_time_stamps(_output.time_stamps_name(),
                                                    _input.time_stamps_name(),
                                                    upper_ts, "t1", "t2", "t1");
  }

  for (size_t i = 0; i < conditions_.size(); ++i) {
    if (i == 0 && !use_time_stamps) {
      sql << "WHERE ";
    } else {
      sql << "AND ";
    }

    sql << conditions_.at(i).to_sql(_categories, _sql_dialect_generator,
                                    _feature_prefix, _input, _output)
        << std::endl;
  }

  sql << _sql_dialect_generator->group_by(
             aggregation_, sql_maker.value_to_be_aggregated(*this))
      << ";" << std::endl
      << std::endl
      << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

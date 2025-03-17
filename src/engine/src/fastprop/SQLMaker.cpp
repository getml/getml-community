// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fastprop/containers/SQLMaker.hpp"

#include "debug/assert_msg.hpp"

namespace fastprop {
namespace containers {

std::string SQLMaker::condition(const Condition& _condition) const {
  assert_true(sql_dialect_generator_);

  const auto quote1 = sql_dialect_generator_->quotechar1();

  const auto quote2 = sql_dialect_generator_->quotechar2();

  const auto make_staging_table_colname =
      [this, quote1, quote2](const std::string& _colname,
                             const std::string& _alias) -> std::string {
    return _alias + "." + quote1 +
           sql_dialect_generator_->make_staging_table_colname(_colname) +
           quote2;
  };

  switch (_condition.data_used_.value()) {
    case enums::DataUsed::value_of<"categorical">(): {
      const auto name = get_name(_condition.data_used_, _condition.peripheral_,
                                 _condition.input_col_, _condition.output_col_);

      assert_true(_condition.category_used_ < categories().size());

      const auto category = categories().at(_condition.category_used_).str();

      return name + " = '" + category + "'";
    }

    case enums::DataUsed::value_of<"lag">(): {
      const auto col1 =
          make_staging_table_colname(output_.time_stamps_name(), "t1");

      const auto col2 =
          make_staging_table_colname(input_.time_stamps_name(), "t2");

      return "( " + col2 + " + " + std::to_string(_condition.bound_upper_) +
             " > " + col1 + " AND " + col2 + " + " +
             std::to_string(_condition.bound_lower_) + " <= " + col1 + " )";
    }

    case enums::DataUsed::value_of<"same_units_categorical">(): {
      const auto [name1, name2] = get_same_units(
          _condition.data_used_, _condition.input_col_, _condition.output_col_);

      return name1 + " = " + name2;
    }

    default:
      assert_msg(false,
                 "Unknown DataUsed: '" + _condition.data_used_.name() + "'");
      return "";
  }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::get_name(const enums::DataUsed _data_used,
                               const size_t _peripheral,
                               const size_t _input_col,
                               const size_t _output_col) const {
  assert_true(sql_dialect_generator_);

  const auto quote1 = sql_dialect_generator_->quotechar1();

  const auto quote2 = sql_dialect_generator_->quotechar2();

  const auto make_staging_table_colname =
      [this, quote1, quote2](const std::string& _colname) -> std::string {
    return "t2." + quote1 +
           sql_dialect_generator_->make_staging_table_colname(_colname) +
           quote2 + "";
  };

  switch (_data_used.value()) {
    case enums::DataUsed::value_of<"categorical">():
      assert_true(_input_col < input_.num_categoricals());
      return make_staging_table_colname(input_.categorical_name(_input_col));

    case enums::DataUsed::value_of<"discrete">():
      assert_true(_input_col < input_.num_discretes());
      return make_staging_table_colname(input_.discrete_name(_input_col));

    case enums::DataUsed::value_of<"numerical">():
      assert_true(_input_col < input_.num_numericals());
      return make_staging_table_colname(input_.numerical_name(_input_col));

    case enums::DataUsed::value_of<"subfeatures">(): {
      const auto number = feature_prefix_ + std::to_string(_peripheral + 1) +
                          "_" + std::to_string(_input_col + 1);

      return "COALESCE( f_" + number + "." + quote1 + "feature_" + number +
             quote2 + ", 0.0 )";
    }

    case enums::DataUsed::value_of<"text">():
      assert_true(_input_col < input_.num_text());
      return make_staging_table_colname(input_.text_name(_input_col));

    default:
      assert_msg(false, "Unknown DataUsed: '" + _data_used.name() + "'");
  }

  return "";
}

// ----------------------------------------------------------------------------

std::pair<std::string, std::string> SQLMaker::get_same_units(
    const enums::DataUsed _data_used, const size_t _input_col,
    const size_t _output_col) const {
  assert_true(sql_dialect_generator_);

  const auto quote1 = sql_dialect_generator_->quotechar1();

  const auto quote2 = sql_dialect_generator_->quotechar2();

  const auto make_staging_table_colname =
      [this, quote1, quote2](const std::string& _colname,
                             const std::string& _alias) -> std::string {
    return _alias + "." + quote1 +
           sql_dialect_generator_->make_staging_table_colname(_colname) +
           quote2;
  };

  switch (_data_used.value()) {
    case enums::DataUsed::value_of<"same_units_categorical">(): {
      assert_true(_output_col < output_.num_categoricals());

      assert_true(_input_col < input_.num_categoricals());

      const auto name1 = make_staging_table_colname(
          output_.categorical_name(_output_col), "t1");

      const auto name2 =
          make_staging_table_colname(input_.categorical_name(_input_col), "t2");

      return std::make_pair(name1, name2);
    }

    case enums::DataUsed::value_of<"same_units_discrete">(): {
      assert_true(_output_col < output_.num_discretes());

      assert_true(_input_col < input_.num_discretes());

      const auto name1 =
          make_staging_table_colname(output_.discrete_name(_output_col), "t1");

      const auto name2 =
          make_staging_table_colname(input_.discrete_name(_input_col), "t2");

      return std::make_pair(name1, name2);
    }

    case enums::DataUsed::value_of<"same_units_discrete_ts">(): {
      assert_true(_output_col < output_.num_discretes());

      assert_true(_input_col < input_.num_discretes());

      const auto name1 =
          make_staging_table_colname(output_.discrete_name(_output_col), "t1");

      const auto name2 =
          make_staging_table_colname(input_.discrete_name(_input_col), "t2");

      return std::make_pair(name1, name2);
    }

    case enums::DataUsed::value_of<"same_units_numerical">(): {
      assert_true(_output_col < output_.num_numericals());

      assert_true(_input_col < input_.num_numericals());

      const auto name1 =
          make_staging_table_colname(output_.numerical_name(_output_col), "t1");

      const auto name2 =
          make_staging_table_colname(input_.numerical_name(_input_col), "t2");

      return std::make_pair(name1, name2);
    }

    case enums::DataUsed::value_of<"same_units_numerical_ts">(): {
      assert_true(_output_col < output_.num_numericals());

      assert_true(_input_col < input_.num_numericals());

      const auto name1 =
          make_staging_table_colname(output_.numerical_name(_output_col), "t1");

      const auto name2 =
          make_staging_table_colname(input_.numerical_name(_input_col), "t2");

      return std::make_pair(name1, name2);
    }

    default:
      assert_msg(false, "Unknown data_used_: '" + _data_used.name() + "'");
      return std::make_pair("", "");
  }
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_avg_time_between() const {
  assert_true(input_.num_time_stamps() > 0);

  assert_true(sql_dialect_generator_);

  const auto quote1 = sql_dialect_generator_->quotechar1();

  const auto quote2 = sql_dialect_generator_->quotechar2();

  const auto ts_name = "t2." + quote1 +
                       sql_dialect_generator_->make_staging_table_colname(
                           input_.time_stamps_name()) +
                       quote2;

  return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + ts_name + " ) - MIN ( " +
         ts_name + " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";
}

// ----------------------------------------------------------------------------

std::string SQLMaker::make_additional_argument(
    const enums::Aggregation& _aggregation) const {
  assert_true(sql_dialect_generator_);

  const auto quote1 = sql_dialect_generator_->quotechar1();

  const auto quote2 = sql_dialect_generator_->quotechar2();

  const auto make_staging_table_colname =
      [this, quote1, quote2](const std::string& _colname,
                             const std::string& _alias) -> std::string {
    return _alias + "." + quote1 +
           sql_dialect_generator_->make_staging_table_colname(_colname) +
           quote2;
  };

  if (_aggregation.value() == enums::Aggregation::value_of<"FIRST">() ||
      _aggregation.value() == enums::Aggregation::value_of<"LAST">()) {
    return make_staging_table_colname(input_.time_stamps_name(), "t2");
  }

  return make_staging_table_colname(output_.time_stamps_name(), "t1") + " - " +
         make_staging_table_colname(input_.time_stamps_name(), "t2");
}

// ----------------------------------------------------------------------------

std::string SQLMaker::select_statement(
    const AbstractFeature& _abstract_feature) const {
  if (_abstract_feature.aggregation_.value() ==
      enums::Aggregation::value_of<"AVG TIME BETWEEN">()) {
    const auto ts_name = "t2." + sql_dialect_generator_->quotechar1() +
                         sql_dialect_generator_->make_staging_table_colname(
                             input_.time_stamps_name()) +
                         sql_dialect_generator_->quotechar2();

    return sql_dialect_generator_->aggregation(
        _abstract_feature.aggregation_,
        value_to_be_aggregated(_abstract_feature), ts_name);
  }

  if (is_first_last(_abstract_feature.aggregation_)) {
    return sql_dialect_generator_->aggregation(
        _abstract_feature.aggregation_,
        value_to_be_aggregated(_abstract_feature),
        make_additional_argument(_abstract_feature.aggregation_));
  }

  return sql_dialect_generator_->aggregation(
      _abstract_feature.aggregation_, value_to_be_aggregated(_abstract_feature),
      std::nullopt);
}

// ----------------------------------------------------------------------------

std::string SQLMaker::value_to_be_aggregated(
    const AbstractFeature& _abstract_feature) const {
  switch (_abstract_feature.data_used_.value()) {
    case enums::DataUsed::value_of<"categorical">():
      if (_abstract_feature.categorical_value_ ==
          AbstractFeature::NO_CATEGORICAL_VALUE) {
        return get_name(
            _abstract_feature.data_used_, _abstract_feature.peripheral_,
            _abstract_feature.input_col_, _abstract_feature.output_col_);
      } else {
        const auto name = get_name(
            _abstract_feature.data_used_, _abstract_feature.peripheral_,
            _abstract_feature.input_col_, _abstract_feature.output_col_);

        assert_true(_abstract_feature.categorical_value_ < categories().size());

        const auto category =
            categories().at(_abstract_feature.categorical_value_).str();

        return "CASE WHEN " + name + " = '" + category + "' THEN 1 ELSE 0 END";
      }

    case enums::DataUsed::value_of<"discrete">():
    case enums::DataUsed::value_of<"numerical">():
    case enums::DataUsed::value_of<"subfeatures">():
      return get_name(
          _abstract_feature.data_used_, _abstract_feature.peripheral_,
          _abstract_feature.input_col_, _abstract_feature.output_col_);

    case enums::DataUsed::value_of<"na">():
      return "*";

    case enums::DataUsed::value_of<"same_units_categorical">(): {
      const auto [name1, name2] = get_same_units(_abstract_feature.data_used_,
                                                 _abstract_feature.input_col_,
                                                 _abstract_feature.output_col_);

      return "CASE WHEN " + name1 + " = " + name2 + " THEN 1 ELSE 0 END";
    }

    case enums::DataUsed::value_of<"same_units_discrete">():
    case enums::DataUsed::value_of<"same_units_discrete_ts">():
    case enums::DataUsed::value_of<"same_units_numerical">():
    case enums::DataUsed::value_of<"same_units_numerical_ts">(): {
      const auto [name1, name2] = get_same_units(_abstract_feature.data_used_,
                                                 _abstract_feature.input_col_,
                                                 _abstract_feature.output_col_);

      return name1 + " - " + name2;
    }

    default:
      assert_msg(false, "Unknown data_used_: '" +
                            _abstract_feature.data_used_.name() + "'");
      return "";
  }
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace fastprop

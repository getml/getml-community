// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/preprocessors/data_model_checking.hpp"

#include <algorithm>

#include "engine/utils/Aggregations.hpp"
#include "fct/fct.hpp"
#include "helpers/Column.hpp"
#include "helpers/Macros.hpp"
#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/SQLGenerator.hpp"

namespace engine {
namespace preprocessors {
namespace data_model_checking {

/// Calculates the number of joins in the placeholder.
size_t calc_num_joins(const helpers::Placeholder& _placeholder);

/// Checks whether all data frames are propositionalization frames.
void check_all_propositionalization(
    const std::shared_ptr<const helpers::Placeholder> _placeholder,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners);

/// Checks the validity of the data frames.
void check_data_frames(
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    logging::ProgressLogger* _logger, communication::Warner* _warner);

/// Checks all of the columns in the data frame make sense.
void check_df(const containers::DataFrame& _df, const bool _check_num_columns,
              communication::Warner* _warner);

/// Recursively checks the plausibility of the joins.
void check_join(
    const helpers::Placeholder& _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<Float>& _prob_pick,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    logging::ProgressLogger* _logger, communication::Warner* _warner);

/// Raises a warning if there is something wrong with the matches.
std::tuple<bool, size_t, Float, size_t, std::vector<Float>> check_matches(
    const std::string& _join_key_used, const std::string& _other_join_key_used,
    const std::string& _time_stamp_used,
    const std::string& _other_time_stamp_used,
    const std::string& _upper_time_stamp_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    const std::vector<Float>& _prob_pick);

/// Checks whether there are too many columns for relmt.
void check_num_columns_relmt(const containers::DataFrame& _population,
                             const containers::DataFrame& _peripheral,
                             communication::Warner* _warner);

/// Makes sure that the peripheral tables are in the right size.
void check_peripheral_size(
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const std::vector<containers::DataFrame>& _peripheral);

/// Makes sure that the data model is actually relational.
void check_relational(
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners);

communication::Warning data_model_can_be_improved(const std::string& _message);

/// Checks whether there is a particular type of feature learner.
bool find_feature_learner(
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const std::string& _model);

/// Finds the time stamps, if necessary.
std::tuple<std::optional<helpers::Column<Float>>,
           std::optional<helpers::Column<Float>>,
           std::optional<helpers::Column<Float>>>
find_time_stamps(const std::string& _time_stamp_used,
                 const std::string& _other_time_stamp_used,
                 const std::string& _upper_time_stamp_used,
                 const containers::DataFrame& _population_df,
                 const containers::DataFrame& _peripheral_df);

/// Checks whether all non-NULL elements in _col are equal to each other
bool is_all_equal(const containers::Column<Float>& _col);

/// Accounts for the fact that data frames might be joined.
std::string modify_df_name(const std::string& _df_name);

/// Accounts for the fact that we might have joined over several join keys.
std::string modify_join_key_name(const std::string& _jk_name);

/// Adds warning messages related to the joins.
void raise_join_warnings(const bool _propositionalization,
                         const bool _is_propositionalization_with_relmt,
                         const bool _is_many_to_one, const size_t _num_matches,
                         const Float _num_expected,
                         const size_t _num_jk_not_found,
                         const std::string& _join_key_used,
                         const std::string& _other_join_key_used,
                         const containers::DataFrame& _population_df,
                         const containers::DataFrame& _peripheral_df,
                         communication::Warner* _warner);

/// Adds a warning that all values are equal.
void warn_all_equal(const bool _is_float, const std::string& _colname,
                    const std::string& _df_name,
                    communication::Warner* _warner);

/// Adds a warning that a data frame is empty.
void warn_is_empty(const std::string& _df_name, communication::Warner* _warner);

/// Adds a warning message related to many-to-one or one-to-one
/// relationships.
void warn_many_to_one(const std::string& _join_key_used,
                      const std::string& _other_join_key_used,
                      const containers::DataFrame& _population_df,
                      const containers::DataFrame& _peripheral_df,
                      communication::Warner* _warner);

/// Generates a no-matches warning.
void warn_no_matches(const std::string& _join_key_used,
                     const std::string& _other_join_key_used,
                     const containers::DataFrame& _population_df,
                     const containers::DataFrame& _peripheral_df,
                     communication::Warner* _warner);

/// Generates a not-found warning
void warn_not_found(const Float _not_found_ratio,
                    const std::string& _join_key_used,
                    const std::string& _other_join_key_used,
                    const containers::DataFrame& _population_df,
                    const containers::DataFrame& _peripheral_df,
                    communication::Warner* _warner);

/// Generates a warning when somebody tries to combine the
/// propositionalization tag with relmt.
void warn_propositionalization_with_relmt(
    const std::string& _join_key_used, const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner);

/// Generates a warning that there are to many columns for Multirel.
void warn_too_many_columns_multirel(const size_t _num_columns,
                                    const std::string& _df_name,
                                    communication::Warner* _warner);

/// Generates a warning that there are too many columns for RelMT.
void warn_too_many_columns_relmt(const size_t _num_columns,
                                 const std::string& _population_name,
                                 const std::string& _peripheral_name,
                                 communication::Warner* _warner);

/// Generates a too-many-matches warning.
void warn_too_many_matches(const size_t _num_matches,
                           const std::string& _join_key_used,
                           const std::string& _other_join_key_used,
                           const containers::DataFrame& _population_df,
                           const containers::DataFrame& _peripheral_df,
                           communication::Warner* _warner);

/// Generates a warning for when there are too many nulls.
void warn_too_many_nulls(const bool _is_float, const Float _share_null,
                         const std::string& _colname,
                         const std::string& _df_name,
                         communication::Warner* _warner);

/// Generates a warning that there too many unique values.
void warn_too_many_unique(const Float _num_distinct,
                          const std::string& _colname,
                          const std::string& _df_name,
                          communication::Warner* _warner);

/// Generates a warning that the share of unique columns is too high.
void warn_unique_share_too_high(const Float _unique_share,
                                const std::string& _colname,
                                const std::string& _df_name,
                                communication::Warner* _warner);

// ----------------------------------------------------------------------------

/// Standard header for an info message.
std::string info() { return "INFO"; }

/// Standard header for a warning message.
std::string warning() { return "WARNING"; }

/// Standard header for a column that should be unused.
communication::Warning column_should_be_unused(const std::string& _message) {
  return communication::Warning(
      fct::make_field<"message_">(_message),
      fct::make_field<"label_", std::string>("COLUMN SHOULD BE UNUSED"),
      fct::make_field<"warning_type_">(warning()));
}

/// Standard header for any improvements to the data model.
communication::Warning data_model_can_be_improved(const std::string& _message) {
  return communication::Warning(
      fct::make_field<"message_">(_message),
      fct::make_field<"label_", std::string>("DATA MODEL CAN BE IMPROVED"),
      fct::make_field<"warning_type_">(warning()));
}

/// Checks whether ts1 lies between ts2 and upper.
bool is_in_range(const Float ts1, const Float ts2, const Float upper) {
  return ts2 <= ts1 && (std::isnan(upper) || ts1 < upper);
}

/// Standard header for when some join keys where not found.
communication::Warning join_keys_not_found(const std::string& _message) {
  return communication::Warning(
      fct::make_field<"message_">(_message),
      fct::make_field<"label_", std::string>("FOREIGN KEYS NOT FOUND"),
      fct::make_field<"warning_type_">(info()));
}

/// Standard header for something that might take long.
communication::Warning might_take_long(const std::string& _message) {
  return communication::Warning(
      fct::make_field<"message_">(_message),
      fct::make_field<"label_", std::string>("MIGHT TAKE LONG"),
      fct::make_field<"warning_type_">(info()));
}

/// Removes any macros from a colname.
std::string modify_colname(const std::string& _colname) {
  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::HumanReadableSQLGenerator()
        .make_staging_table_colname(_colname);
  };
  const auto colnames =
      helpers::Macros::modify_colnames({_colname}, make_staging_table_colname);
  assert_true(colnames.size() == 1);
  return colnames.at(0);
}

/// Transforms the column into a helpers::Column.
template <class T>
std::optional<helpers::Column<T>> to_helper_col(
    const std::optional<containers::Column<T>>& _col) {
  if (!_col) {
    return std::nullopt;
  }
  return helpers::Column<T>(_col->const_data_ptr(), _col->name(), {}, "");
}

// ----------------------------------------------------------------------------

size_t calc_num_joins(const helpers::Placeholder& _placeholder) {
  auto range = _placeholder.joined_tables() | VIEWS::transform(calc_num_joins);

  return _placeholder.joined_tables().size() +
         std::accumulate(range.begin(), range.end(), 0);
}

// ----------------------------------------------------------------------------

communication::Warner check(
    const std::shared_ptr<const helpers::Placeholder> _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    const std::shared_ptr<const communication::SocketLogger>& _logger) {
  check_relational(_peripheral, _feature_learners);

  const auto is_not_text_field = [](const containers::DataFrame& _df) -> bool {
    return _df.name().find(helpers::Macros::text_field()) == std::string::npos;
  };

  const auto peripheral =
      fct::collect::vector(_peripheral | VIEWS::filter(is_not_text_field));

  check_peripheral_size(_peripheral_names, peripheral);

  check_all_propositionalization(_placeholder, _feature_learners);

  communication::Warner warner;

  const size_t num_total = _feature_learners.size() > 0 ? 50 : 100;

  auto logger_df = logging::ProgressLogger("Checking...", _logger,
                                           peripheral.size() + 1, 0, num_total);

  check_data_frames(_population, peripheral, _feature_learners, &logger_df,
                    &warner);

  assert_true(_placeholder);

  if (_feature_learners.size() > 0) {
    const auto num_joins = calc_num_joins(*_placeholder);

    auto logger_joins =
        logging::ProgressLogger("", _logger, num_joins, 50, 100);

    if (_population.nrows() == 0) {
      throw std::runtime_error("There are no rows in the population table.");
    }

    // The probability of being picked is equal for all rows in the
    // population table.
    const auto prob_pick = std::vector<Float>(
        _population.nrows(), 1.0 / static_cast<Float>(_population.nrows()));

    check_join(*_placeholder, _peripheral_names, _population, peripheral,
               prob_pick, _feature_learners, &logger_joins, &warner);
  }

  return warner;
}

// ----------------------------------------------------------------------------

void check_all_propositionalization(
    const std::shared_ptr<const helpers::Placeholder> _placeholder,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners) {
  const auto is_not_prop =
      [](const std::shared_ptr<featurelearners::AbstractFeatureLearner>& _fl)
      -> bool {
    assert_true(_fl);
    return (_fl->type() != featurelearners::AbstractFeatureLearner::FASTPROP);
  };

  const bool any_non_fast_prop = std::any_of(
      _feature_learners.begin(), _feature_learners.end(), is_not_prop);

  assert_true(_placeholder);

  const auto is_true = [](const bool _val) { return _val; };

  const bool all_propositionalization =
      (_placeholder->propositionalization().size() > 0) &&
      std::all_of(_placeholder->propositionalization().begin(),
                  _placeholder->propositionalization().end(), is_true);

  if (all_propositionalization && any_non_fast_prop) {
    throw std::runtime_error(
        "All joins in the data model have been set to "
        "propositionalization. You should use FastProp instead.");
  }
}

// ----------------------------------------------------------------------------

void check_categorical_column(const containers::Column<Int>& _col,
                              const std::string& _df_name,
                              communication::Warner* _warner) {
  const auto length = static_cast<Float>(_col.size());

  assert_true(_col.size() > 0);

  const Float num_non_null =
      utils::Aggregations::count_categorical(_col.begin(), _col.end());

  const auto share_null = 1.0 - num_non_null / length;

  if (share_null > 0.9) {
    warn_too_many_nulls(false, share_null, _col.name(), _df_name, _warner);
  }

  if (num_non_null < 0.5) {
    return;
  }

  const bool is_comparison_only =
      (_col.unit().find("comparison only") != std::string::npos);

  const Float num_distinct =
      utils::Aggregations::count_distinct(_col.begin(), _col.end());

  if (num_distinct == 1.0 && !is_comparison_only) {
    warn_all_equal(false, _col.name(), _df_name, _warner);
  }

  if (num_distinct > 1000.0 && !is_comparison_only) {
    warn_too_many_unique(num_distinct, _col.name(), _df_name, _warner);
  }

  const auto unique_share = num_distinct / num_non_null;

  if (!is_comparison_only && unique_share > 0.25) {
    warn_unique_share_too_high(unique_share, _col.name(), _df_name, _warner);
  }
}

// ----------------------------------------------------------------------------

void check_data_frames(
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners,
    logging::ProgressLogger* _logger, communication::Warner* _warner) {
  const auto has_multirel = find_feature_learner(
      _feature_learners, featurelearners::AbstractFeatureLearner::MULTIREL);

  const auto has_relmt = find_feature_learner(
      _feature_learners, featurelearners::AbstractFeatureLearner::RELMT);

  check_df(_population, false, _warner);

  _logger->increment();

  for (const auto& df : _peripheral) {
    check_df(df, has_multirel, _warner);
    _logger->increment();
  }

  if (has_relmt) {
    for (const auto& df : _peripheral) {
      check_num_columns_relmt(_population, df, _warner);
    }
  }
}

// ----------------------------------------------------------------------------

void check_df(const containers::DataFrame& _df, const bool _check_num_columns,
              communication::Warner* _warner) {
  if (_df.nrows() == 0) {
    warn_is_empty(_df.name(), _warner);
    return;
  }

  if (_check_num_columns) {
    const auto num_columns = _df.num_numericals() + _df.num_categoricals();

    if (num_columns > 20) {
      warn_too_many_columns_multirel(num_columns, _df.name(), _warner);
    }
  }

  for (size_t i = 0; i < _df.num_categoricals(); ++i) {
    check_categorical_column(_df.categorical(i), _df.name(), _warner);
  }

  for (size_t i = 0; i < _df.num_numericals(); ++i) {
    check_float_column(_df.numerical(i), _df.name(), _warner);
  }

  for (size_t i = 0; i < _df.num_time_stamps(); ++i) {
    check_float_column(_df.time_stamp(i), _df.name(), _warner);
  }
}

// ----------------------------------------------------------------------------

void check_float_column(const containers::Column<Float>& _col,
                        const std::string& _df_name,
                        communication::Warner* _warner) {
  const auto length = static_cast<Float>(_col.size());

  assert_true(_col.size() > 0);

  const Float num_non_null =
      utils::Aggregations::count(_col.begin(), _col.end());

  const auto share_null = 1.0 - num_non_null / length;

  if (share_null > 0.9) {
    warn_too_many_nulls(true, share_null, _col.name(), _df_name, _warner);
  }

  const bool is_comparison_only =
      (_col.unit().find("comparison only") != std::string::npos);

  const auto all_equal = !is_comparison_only && is_all_equal(_col);

  if (all_equal) {
    warn_all_equal(true, _col.name(), _df_name, _warner);
  }
}

// ----------------------------------------------------------------------------

void check_join(
    const helpers::Placeholder& _placeholder,
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const containers::DataFrame& _population,
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<Float>& _prob_pick,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    logging::ProgressLogger* _logger, communication::Warner* _warner) {
  const auto is_prop =
      [](const std::shared_ptr<featurelearners::AbstractFeatureLearner>& _fl)
      -> bool {
    assert_true(_fl);
    return (_fl->type() == featurelearners::AbstractFeatureLearner::FASTPROP);
  };

  const bool all_propositionalization =
      std::all_of(_feature_learners.begin(), _feature_learners.end(), is_prop);

  const auto is_relmt =
      [](const std::shared_ptr<featurelearners::AbstractFeatureLearner>& _fl)
      -> bool {
    assert_true(_fl);
    return (_fl->type() == featurelearners::AbstractFeatureLearner::RELMT);
  };

  const bool any_relmt =
      std::any_of(_feature_learners.begin(), _feature_learners.end(), is_relmt);

  assert_true(_peripheral_names);

  assert_true(_peripheral_names->size() == _peripheral.size());

  const auto& joined_tables = _placeholder.joined_tables();

  const auto& join_keys_used = _placeholder.join_keys_used();

  const auto& other_join_keys_used = _placeholder.other_join_keys_used();

  const auto& propositionalization = _placeholder.propositionalization();

  const auto& time_stamps_used = _placeholder.time_stamps_used();

  const auto& other_time_stamps_used = _placeholder.other_time_stamps_used();

  const auto& upper_time_stamps_used = _placeholder.upper_time_stamps_used();

  const auto size = joined_tables.size();

  assert_true(join_keys_used.size() == size);

  assert_true(other_join_keys_used.size() == size);

  assert_true(propositionalization.size() == size);

  assert_true(time_stamps_used.size() == size);

  assert_true(other_time_stamps_used.size() == size);

  assert_true(upper_time_stamps_used.size() == size);

  for (size_t i = 0; i < size; ++i) {
    const auto& name = joined_tables.at(i).name();

    const auto it =
        std::find(_peripheral_names->begin(), _peripheral_names->end(), name);

    if (it == _peripheral_names->end()) {
      throw std::runtime_error("No placeholder called '" + name +
                               "' among the peripheral placeholders.");
    }

    const auto dist = std::distance(_peripheral_names->begin(), it);

    const auto [is_many_to_one, num_matches, num_expected, num_jk_not_found,
                prob_pick] =
        check_matches(join_keys_used.at(i), other_join_keys_used.at(i),
                      time_stamps_used.at(i), other_time_stamps_used.at(i),
                      upper_time_stamps_used.at(i), _population,
                      _peripheral.at(dist), _prob_pick);

    raise_join_warnings(all_propositionalization || propositionalization.at(i),
                        any_relmt && propositionalization.at(i), is_many_to_one,
                        num_matches, num_expected, num_jk_not_found,
                        join_keys_used.at(i), other_join_keys_used.at(i),
                        _population, _peripheral.at(dist), _warner);

    _logger->increment();

    check_join(joined_tables.at(i), _peripheral_names, _peripheral.at(dist),
               _peripheral, prob_pick, _feature_learners, _logger, _warner);
  }
}

// ----------------------------------------------------------------------------

std::tuple<bool, size_t, Float, size_t, std::vector<Float>> check_matches(
    const std::string& _join_key_used, const std::string& _other_join_key_used,
    const std::string& _time_stamp_used,
    const std::string& _other_time_stamp_used,
    const std::string& _upper_time_stamp_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    const std::vector<Float>& _prob_pick) {
  assert_true(_population_df.nrows() == _prob_pick.size());

  const auto jk1 = _population_df.join_key(_join_key_used);

  const auto [ts1, ts2, upper] =
      find_time_stamps(_time_stamp_used, _other_time_stamp_used,
                       _upper_time_stamp_used, _population_df, _peripheral_df);

  bool is_many_to_one = true;

  size_t num_matches = 0;

  size_t num_jk_not_found = 0;

  auto prob_pick = std::vector<Float>(_peripheral_df.nrows());

  auto local_num_matches = std::vector<Float>(_population_df.nrows());

  const auto idx2 = _peripheral_df.index(_other_join_key_used);

  for (size_t ix1 = 0; ix1 < jk1.size(); ++ix1) {
    const auto [begin, end] = idx2.find(jk1[ix1]);

    if (begin == nullptr) {
      num_jk_not_found++;
      continue;
    }

    for (auto it = begin; it != end; ++it) {
      const auto ix2 = *it;

      const bool in_range =
          is_in_range(ts1 ? (*ts1)[ix1] : 0.0, ts2 ? (*ts2)[ix2] : 0.0,
                      upper ? (*upper)[ix2] : NAN);

      if (!in_range) {
        continue;
      }

      ++num_matches;

      local_num_matches.at(ix1) += 1.0;

      prob_pick.at(ix2) += _prob_pick.at(ix1);

      if (local_num_matches.at(ix1) > 1.0) {
        is_many_to_one = false;
      }
    }
  }

  const auto num_expected =
      std::inner_product(local_num_matches.begin(), local_num_matches.end(),
                         _prob_pick.begin(), 0.0);

  return std::make_tuple(is_many_to_one, num_matches, num_expected,
                         num_jk_not_found, prob_pick);
}

// ----------------------------------------------------------------------------

void check_peripheral_size(
    const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
    const std::vector<containers::DataFrame>& _peripheral) {
  assert_true(_peripheral_names);

  if (_peripheral_names->size() != _peripheral.size()) {
    throw std::runtime_error(
        "The number of peripheral tables in the placeholder must "
        "be "
        "equal to the number of peripheral tables passed (" +
        std::to_string(_peripheral_names->size()) + " vs. " +
        std::to_string(_peripheral.size()) +
        "). This is the point of having placeholders.");
  }
}

// ----------------------------------------------------------------------------

void check_num_columns_relmt(const containers::DataFrame& _population,
                             const containers::DataFrame& _peripheral,
                             communication::Warner* _warner) {
  const auto num_columns =
      _population.num_numericals() + _population.num_time_stamps() +
      _peripheral.num_numericals() + _peripheral.num_time_stamps();

  if (num_columns > 40) {
    warn_too_many_columns_relmt(num_columns, _population.name(),
                                _peripheral.name(), _warner);
  }
}

// ----------------------------------------------------------------------------

void check_relational(
    const std::vector<containers::DataFrame>& _peripheral,
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        _feature_learners) {
  if (_peripheral.size() == 0 && _feature_learners.size() > 0) {
    throw std::runtime_error(
        "The data model you have passed is not relational (there are "
        "no joins in it that aren't many-to-one or one-to-one), yet "
        "you have passed relational feature learners.");
  }
}

// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------

bool find_feature_learner(
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners,
    const std::string& _model) {
  const auto is_model =
      [_model](
          const std::shared_ptr<const featurelearners::AbstractFeatureLearner>&
              fl) {
        assert_true(fl);
        return fl->type() == _model;
      };

  const bool has_model =
      std::any_of(_feature_learners.begin(), _feature_learners.end(), is_model);

  return has_model;
}

// ----------------------------------------------------------------------------

std::tuple<std::optional<helpers::Column<Float>>,
           std::optional<helpers::Column<Float>>,
           std::optional<helpers::Column<Float>>>
find_time_stamps(const std::string& _time_stamp_used,
                 const std::string& _other_time_stamp_used,
                 const std::string& _upper_time_stamp_used,
                 const containers::DataFrame& _population_df,
                 const containers::DataFrame& _peripheral_df) {
  if ((_time_stamp_used == "") != (_other_time_stamp_used == "")) {
    throw std::runtime_error(
        "You have to pass both time_stamp_used and "
        "other_time_stamps_used or neither of them.");
  }

  if (_time_stamp_used == "" && _upper_time_stamp_used != "") {
    throw std::runtime_error(
        "If you pass no time_stamp_used, then passing an "
        "upper_time_stamp_used makes no sense.");
  }

  auto ts1 = std::optional<containers::Column<Float>>();

  if (_time_stamp_used != "") {
    ts1 = _population_df.time_stamp(_time_stamp_used);
  }

  auto ts2 = std::optional<containers::Column<Float>>();

  if (_other_time_stamp_used != "") {
    ts2 = _peripheral_df.time_stamp(_other_time_stamp_used);
  }

  auto upper = std::optional<containers::Column<Float>>();

  if (_upper_time_stamp_used != "") {
    upper = _peripheral_df.time_stamp(_upper_time_stamp_used);
  }

  return std::make_tuple(to_helper_col(ts1), to_helper_col(ts2),
                         to_helper_col(upper));
}

// ----------------------------------------------------------------------------

bool is_all_equal(const containers::Column<Float>& _col) {
  auto it = std::find_if(_col.begin(), _col.end(),
                         [](Float val) { return !std::isnan(val); });

  if (it == _col.end()) {
    return true;
  }

  Float val = *it;

  for (; it != _col.end(); ++it) {
    if (std::isnan(*it)) {
      continue;
    }

    if (val != *it) {
      return false;
    }
  }

  return true;
}

// ------------------------------------------------------------------------

std::string modify_df_name(const std::string& _df_name) {
  return transpilation::SQLGenerator::make_staging_table_name(_df_name);
}

// ------------------------------------------------------------------------

std::string modify_join_key_name(const std::string& _jk_name) {
  const auto names = helpers::Macros::parse_join_key_name(_jk_name);

  std::string modified;

  for (size_t i = 0; i < names.size(); ++i) {
    const bool has_column =
        (names.at(i).find(helpers::Macros::column()) != std::string::npos);

    const auto name = has_column ? helpers::Macros::get_param(
                                       names.at(i), helpers::Macros::column())
                                 : names.at(i);

    modified += "'" + name + "'";

    if (i != names.size() - 1) {
      modified += ", ";
    }
  }

  return modified;
}

// ------------------------------------------------------------------------

void raise_join_warnings(const bool _is_propositionalization,
                         const bool _is_propositionalization_with_relmt,
                         const bool _is_many_to_one, const size_t _num_matches,
                         const Float _num_expected,
                         const size_t _num_jk_not_found,
                         const std::string& _join_key_used,
                         const std::string& _other_join_key_used,
                         const containers::DataFrame& _population_df,
                         const containers::DataFrame& _peripheral_df,
                         communication::Warner* _warner) {
  if (_num_matches == 0) {
    warn_no_matches(_join_key_used, _other_join_key_used, _population_df,
                    _peripheral_df, _warner);

    return;
  }

  if (_is_many_to_one) {
    warn_many_to_one(_join_key_used, _other_join_key_used, _population_df,
                     _peripheral_df, _warner);
  }

  if (!_is_propositionalization && _num_expected > 300.0) {
    warn_too_many_matches(_num_matches, _join_key_used, _other_join_key_used,
                          _population_df, _peripheral_df, _warner);
  }

  if (_is_propositionalization_with_relmt) {
    warn_propositionalization_with_relmt(_join_key_used, _other_join_key_used,
                                         _population_df, _peripheral_df,
                                         _warner);
  }

  const auto not_found_ratio = static_cast<Float>(_num_jk_not_found) /
                               static_cast<Float>(_population_df.nrows());

  if (not_found_ratio > 0.0) {
    warn_not_found(not_found_ratio, _join_key_used, _other_join_key_used,
                   _population_df, _peripheral_df, _warner);
  }
}

// ------------------------------------------------------------------------

void warn_all_equal(const bool _is_float, const std::string& _colname,
                    const std::string& _df_name,
                    communication::Warner* _warner) {
  const std::string role = _is_float
                               ? containers::DataFrame::ROLE_UNUSED_FLOAT
                               : containers::DataFrame::ROLE_UNUSED_STRING;

  const auto colname = modify_colname(_colname);

  const auto df_name = modify_df_name(_df_name);

  const auto message =
      "All non-NULL entries in column '" + colname + "' in " + df_name +
      " are equal to each other. You should "
      "consider setting its role to " +
      role +
      " or using it for "
      "comparison only (you can do the latter by setting a unit "
      "that contains 'comparison only').";

  _warner->add(column_should_be_unused(message));
}

// ------------------------------------------------------------------------

void warn_propositionalization_with_relmt(
    const std::string& _join_key_used, const std::string& _other_join_key_used,
    const containers::DataFrame& _population_df,
    const containers::DataFrame& _peripheral_df,
    communication::Warner* _warner) {
  const auto join_key_used = modify_join_key_name(_join_key_used);

  const auto other_join_key_used = modify_join_key_name(_other_join_key_used);

  const auto population_name = modify_df_name(_population_df.name());

  const auto peripheral_name = modify_df_name(_peripheral_df.name());

  const auto message =
      "You have set the 'propositionalization' marker when joining " +
      population_name + " and " + peripheral_name + " over " + join_key_used +
      " and " + other_join_key_used +
      ". At the same time, you are using RelMT. The RelMT "
      "algorithm does not scale very well to "
      "with many columns, as the propositionalization is likely to produce. "
      "This pipeline might take a very "
      "long time to fit. "
      "You could replace RelMT with Relboost or Fastboost. Both algorithms "
      "have been designed to scale well to situations like this.";

  _warner->add(might_take_long(message));
}

// ------------------------------------------------------------------------

void warn_is_empty(const std::string& _df_name,
                   communication::Warner* _warner) {
  const auto df_name = modify_df_name(_df_name);
  _warner->add(
      data_model_can_be_improved("It appears that " + df_name + " is empty."));
}

// ------------------------------------------------------------------------

void warn_many_to_one(const std::string& _join_key_used,
                      const std::string& _other_join_key_used,
                      const containers::DataFrame& _population_df,
                      const containers::DataFrame& _peripheral_df,
                      communication::Warner* _warner) {
  const auto join_key_used = modify_join_key_name(_join_key_used);

  const auto other_join_key_used = modify_join_key_name(_other_join_key_used);

  const auto population_name = modify_df_name(_population_df.name());

  const auto peripheral_name = modify_df_name(_peripheral_df.name());

  const auto message =
      "It appears that " + population_name + " and " + peripheral_name +
      " are in a many-to-one or one-to-one relationship when "
      "joined over " +
      join_key_used + " and " + other_join_key_used +
      ". If the relationship must always be many-to-one or one-to-one, you "
      "should "
      "mark the relationship as such in the .join(...) method of the "
      "placeholder. If this is just an idiosyncracy of the training set, you "
      "should consider using a different training set.";

  _warner->add(data_model_can_be_improved(message));
}

// ------------------------------------------------------------------------

void warn_no_matches(const std::string& _join_key_used,
                     const std::string& _other_join_key_used,
                     const containers::DataFrame& _population_df,
                     const containers::DataFrame& _peripheral_df,
                     communication::Warner* _warner) {
  const auto join_key_used = modify_join_key_name(_join_key_used);

  const auto other_join_key_used = modify_join_key_name(_other_join_key_used);

  const auto population_name = modify_df_name(_population_df.name());

  const auto peripheral_name = modify_df_name(_peripheral_df.name());

  const auto message =
      "There are no matches between " + join_key_used + " in " +
      population_name + " and " + other_join_key_used + " in " +
      peripheral_name +
      ". You should consider removing this join from your data "
      "model or re-examine your join keys.";

  _warner->add(data_model_can_be_improved(message));
}

// ------------------------------------------------------------------------

void warn_not_found(const Float _not_found_ratio,
                    const std::string& _join_key_used,
                    const std::string& _other_join_key_used,
                    const containers::DataFrame& _population_df,
                    const containers::DataFrame& _peripheral_df,
                    communication::Warner* _warner) {
  const auto join_key_used = modify_join_key_name(_join_key_used);

  const auto other_join_key_used = modify_join_key_name(_other_join_key_used);

  const auto population_name = modify_df_name(_population_df.name());

  const auto peripheral_name = modify_df_name(_peripheral_df.name());

  const auto message =
      "When joining " + population_name + " and " + peripheral_name + " over " +
      join_key_used + " and " + other_join_key_used +
      ", there are no corresponding entries for " +
      std::to_string(_not_found_ratio * 100.0) + "% of entries in " +
      join_key_used + " in '" + population_name +
      "'. You might want to double-check your join keys.";

  _warner->add(join_keys_not_found(message));
}

// ------------------------------------------------------------------------

void warn_too_many_columns_multirel(const size_t _num_columns,
                                    const std::string& _df_name,
                                    communication::Warner* _warner) {
  const auto df_name = modify_df_name(_df_name);

  const auto message =
      df_name + " contains " + std::to_string(_num_columns) +
      " categorical and numerical columns. "
      "Please note that columns created by the preprocessors "
      "are also part of this count. The multirel "
      "algorithm does not scale very well to data frames "
      "with many columns. This pipeline might take a very "
      "long time to fit. You should consider removing some "
      "columns or preprocessors. You could use a column selection "
      "to pick the right columns. "
      "You could also replace "
      "Multirel with Relboost or Fastboost. Both algorithms "
      "have been designed to scale well to data "
      "frames with many columns.";

  _warner->add(might_take_long(message));
}

// ------------------------------------------------------------------------

void warn_too_many_columns_relmt(const size_t _num_columns,
                                 const std::string& _population_name,
                                 const std::string& _peripheral_name,
                                 communication::Warner* _warner) {
  const auto population_name = modify_df_name(_population_name);

  const auto peripheral_name = modify_df_name(_peripheral_name);

  std::string message = population_name;

  if (population_name != peripheral_name) {
    message +=
        " and " + peripheral_name + " contain " + std::to_string(_num_columns);
  } else {
    message += " contains " + std::to_string(_num_columns / 2);
  }

  message +=
      " numerical columns and time stamps. "
      "Please note that columns created by the preprocessors "
      "are also part of this count. The relmt "
      "algorithm does not scale very well to data frames "
      "with many columns. This pipeline might take a very "
      "long time to fit. You should consider removing some "
      "columns or preprocessors. You can use a column selection "
      "to pick the right columns. "
      "You could also replace "
      "RelMT with Relboost or Fastboost. Both algorithms "
      "have been designed to scale well to data "
      "frames with many columns.";

  _warner->add(might_take_long(message));
}

// ------------------------------------------------------------------------

void warn_too_many_matches(const size_t _num_matches,
                           const std::string& _join_key_used,
                           const std::string& _other_join_key_used,
                           const containers::DataFrame& _population_df,
                           const containers::DataFrame& _peripheral_df,
                           communication::Warner* _warner) {
  const auto join_key_used = modify_join_key_name(_join_key_used);

  const auto other_join_key_used = modify_join_key_name(_other_join_key_used);

  const auto population_name = modify_df_name(_population_df.name());

  const auto peripheral_name = modify_df_name(_peripheral_df.name());

  const auto message =
      "There are " + std::to_string(_num_matches) + " matches between " +
      population_name + " and " + peripheral_name + " when joined over " +
      join_key_used + " and " + other_join_key_used +
      ". This pipeline might take a very long time to fit. There are "
      "multiple ways to fix this: \n"
      "1) You could impose a narrower limit on the scope of "
      "this join by reducing the memory (the period of time until "
      "the feature learner 'forgets' historical data). "
      "You can reduce the memory "
      "by setting the appropriate parameter in the .join(...)-method "
      "of the Placeholder. "
      "Please note that a memory of 0.0 means that "
      "the time series will not forget any past "
      "data.\n"
      "2) You could also set the relationship parameter to "
      "propositionalization, which would force the pipeline to use the "
      "FastProp algorithm for this particular join. You can also do that in "
      "the .join(...)-method of the Placeholder.\n"
      "3) You could also use FastProp or Fastboost for the "
      "entire pipeline.";

  _warner->add(might_take_long(message));
}

// ------------------------------------------------------------------------

void warn_too_many_nulls(const bool _is_float, const Float _share_null,
                         const std::string& _colname,
                         const std::string& _df_name,
                         communication::Warner* _warner) {
  const std::string role = _is_float
                               ? containers::DataFrame::ROLE_UNUSED_FLOAT
                               : containers::DataFrame::ROLE_UNUSED_STRING;

  const auto colname = modify_colname(_colname);

  const auto df_name = modify_df_name(_df_name);

  const auto message = std::to_string(_share_null * 100.0) +
                       "% of all entries in column '" + colname + "' in " +
                       df_name +
                       " are NULL values. You should "
                       "consider setting its role to " +
                       role + ".";

  _warner->add(column_should_be_unused(message));
}

// ------------------------------------------------------------------------

void warn_too_many_unique(const Float _num_distinct,
                          const std::string& _colname,
                          const std::string& _df_name,
                          communication::Warner* _warner) {
  const auto colname = modify_colname(_colname);

  const auto df_name = modify_df_name(_df_name);

  const auto message =
      "The number of unique entries in column "
      "'" +
      colname + "' in " + df_name + " is " +
      std::to_string(static_cast<int>(_num_distinct)) +
      ". This might take a long time to fit. You should "
      "consider setting its role to unused_string or using it "
      "for "
      "comparison only (you can do the latter by setting a unit "
      "that "
      "contains 'comparison only').";

  _warner->add(might_take_long(message));
}

// ------------------------------------------------------------------------

void warn_unique_share_too_high(const Float _unique_share,
                                const std::string& _colname,
                                const std::string& _df_name,
                                communication::Warner* _warner) {
  const auto colname = modify_colname(_colname);

  const auto df_name = modify_df_name(_df_name);

  const auto message =
      "The ratio of unique entries to non-NULL entries in column "
      "'" +
      colname + "' in data frame " + df_name + " is " +
      std::to_string(_unique_share * 100.0) +
      "%. You should "
      "consider setting its role to unused_string or using it "
      "for "
      "comparison only (you can do the latter by setting a unit "
      "that "
      "contains 'comparison only').";

  _warner->add(column_should_be_unused(message));
}

}  // namespace data_model_checking
}  // namespace preprocessors
}  // namespace engine

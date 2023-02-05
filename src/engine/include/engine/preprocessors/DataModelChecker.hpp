// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_DATAMODELCHECKER_HPP_
#define ENGINE_PREPROCESSORS_DATAMODELCHECKER_HPP_

#include <memory>
#include <string>
#include <vector>

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"
#include "engine/featurelearners/AbstractFeatureLearner.hpp"
#include "helpers/helpers.hpp"
#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/transpilation.hpp"

namespace engine {
namespace preprocessors {

class DataModelChecker {
 public:
  /// Generates warnings, if there are obvious issues in the data model.
  static communication::Warner check(
      const std::shared_ptr<const helpers::Placeholder> _placeholder,
      const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
      const containers::DataFrame& _population,
      const std::vector<containers::DataFrame>& _peripheral,
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>
          _feature_learners,
      const std::shared_ptr<const communication::SocketLogger>& _logger);

  /// Checks the plausibility of a categorical column.
  static void check_categorical_column(const containers::Column<Int>& _col,
                                       const std::string& _df_name,
                                       communication::Warner* _warner);

  /// Checks the plausibility of a float column.
  static void check_float_column(const containers::Column<Float>& _col,
                                 const std::string& _df_name,
                                 communication::Warner* _warner);

 private:
  /// Calculates the number of joins in the placeholder.
  static size_t calc_num_joins(const helpers::Placeholder& _placeholder);

  /// Checks whether all data frames are propositionalization frames.
  static void check_all_propositionalization(
      const std::shared_ptr<const helpers::Placeholder> _placeholder,
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>
          _feature_learners);

  /// Checks the validity of the data frames.
  static void check_data_frames(
      const containers::DataFrame& _population,
      const std::vector<containers::DataFrame>& _peripheral,
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>
          _feature_learners,
      logging::ProgressLogger* _logger, communication::Warner* _warner);

  /// Checks all of the columns in the data frame make sense.
  static void check_df(const containers::DataFrame& _df,
                       const bool _check_num_columns,
                       communication::Warner* _warner);

  /// Recursively checks the plausibility of the joins.
  static void check_join(
      const helpers::Placeholder& _placeholder,
      const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
      const containers::DataFrame& _population,
      const std::vector<containers::DataFrame>& _peripheral,
      const std::vector<Float>& _prob_pick,
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
          _feature_learners,
      logging::ProgressLogger* _logger, communication::Warner* _warner);

  /// Raises a warning if there is something wrong with the matches.
  static std::tuple<bool, size_t, Float, size_t, std::vector<Float>>
  check_matches(const std::string& _join_key_used,
                const std::string& _other_join_key_used,
                const std::string& _time_stamp_used,
                const std::string& _other_time_stamp_used,
                const std::string& _upper_time_stamp_used,
                const containers::DataFrame& _population_df,
                const containers::DataFrame& _peripheral_df,
                const std::vector<Float>& _prob_pick);

  /// Checks whether there are too many columns for relmt.
  static void check_num_columns_relmt(const containers::DataFrame& _population,
                                      const containers::DataFrame& _peripheral,
                                      communication::Warner* _warner);

  /// Makes sure that the peripheral tables are in the right size.
  static void check_peripheral_size(
      const std::shared_ptr<const std::vector<std::string>> _peripheral_names,
      const std::vector<containers::DataFrame>& _peripheral);

  /// Makes sure that the data model is actually relational.
  static void check_relational(
      const std::vector<containers::DataFrame>& _peripheral,
      const std::vector<
          std::shared_ptr<featurelearners::AbstractFeatureLearner>>
          _feature_learners);

  /// Checks whether there is a particular type of feature learner.
  static bool find_feature_learner(
      const std::vector<std::shared_ptr<
          featurelearners::AbstractFeatureLearner>>& _feature_learners,
      const std::string& _model);

  /// Finds the time stamps, if necessary.
  static std::tuple<std::optional<containers::Column<Float>>,
                    std::optional<containers::Column<Float>>,
                    std::optional<containers::Column<Float>>>
  find_time_stamps(const std::string& _time_stamp_used,
                   const std::string& _other_time_stamp_used,
                   const std::string& _upper_time_stamp_used,
                   const containers::DataFrame& _population_df,
                   const containers::DataFrame& _peripheral_df);

  /// Extracts the time stamps from the population placeholder.
  static std::tuple<std::vector<std::string>, std::vector<std::string>,
                    std::vector<std::string>>
  get_time_stamps_used(const Poco::JSON::Object& _population_placeholder,
                       const size_t _expected_size);

  /// Checks whether all non-NULL elements in _col are equal to each other
  static bool is_all_equal(const containers::Column<Float>& _col);

  /// Accounts for the fact that data frames might be joined.
  static std::string modify_df_name(const std::string& _df_name);

  /// Accounts for the fact that we might have joined over several join keys.
  static std::string modify_join_key_name(const std::string& _jk_name);

  /// Adds warning messages related to the joins.
  static void raise_join_warnings(
      const bool _propositionalization,
      const bool _is_propositionalization_with_relmt,
      const bool _is_many_to_one, const size_t _num_matches,
      const Float _num_expected, const size_t _num_jk_not_found,
      const std::string& _join_key_used,
      const std::string& _other_join_key_used,
      const containers::DataFrame& _population_df,
      const containers::DataFrame& _peripheral_df,
      communication::Warner* _warner);

  /// Adds a warning that all values are equal.
  static void warn_all_equal(const bool _is_float, const std::string& _colname,
                             const std::string& _df_name,
                             communication::Warner* _warner);

  /// Adds a warning that a data frame is empty.
  static void warn_is_empty(const std::string& _df_name,
                            communication::Warner* _warner);

  /// Adds a warning message related to many-to-one or one-to-one
  /// relationships.
  static void warn_many_to_one(const std::string& _join_key_used,
                               const std::string& _other_join_key_used,
                               const containers::DataFrame& _population_df,
                               const containers::DataFrame& _peripheral_df,
                               communication::Warner* _warner);

  /// Generates a no-matches warning.
  static void warn_no_matches(const std::string& _join_key_used,
                              const std::string& _other_join_key_used,
                              const containers::DataFrame& _population_df,
                              const containers::DataFrame& _peripheral_df,
                              communication::Warner* _warner);

  /// Generates a not-found warning
  static void warn_not_found(const Float _not_found_ratio,
                             const std::string& _join_key_used,
                             const std::string& _other_join_key_used,
                             const containers::DataFrame& _population_df,
                             const containers::DataFrame& _peripheral_df,
                             communication::Warner* _warner);

  /// Generates a warning when somebody tries to combine the
  /// propositionalization tag with relmt.
  static void warn_propositionalization_with_relmt(
      const std::string& _join_key_used,
      const std::string& _other_join_key_used,
      const containers::DataFrame& _population_df,
      const containers::DataFrame& _peripheral_df,
      communication::Warner* _warner);

  /// Generates a warning that there are to many columns for Multirel.
  static void warn_too_many_columns_multirel(const size_t _num_columns,
                                             const std::string& _df_name,
                                             communication::Warner* _warner);

  /// Generates a warning that there are too many columns for RelMT.
  static void warn_too_many_columns_relmt(const size_t _num_columns,
                                          const std::string& _population_name,
                                          const std::string& _peripheral_name,
                                          communication::Warner* _warner);

  /// Generates a too-many-matches warning.
  static void warn_too_many_matches(const size_t _num_matches,
                                    const std::string& _join_key_used,
                                    const std::string& _other_join_key_used,
                                    const containers::DataFrame& _population_df,
                                    const containers::DataFrame& _peripheral_df,
                                    communication::Warner* _warner);

  /// Generates a warning for when there are too many nulls.
  static void warn_too_many_nulls(const bool _is_float, const Float _share_null,
                                  const std::string& _colname,
                                  const std::string& _df_name,
                                  communication::Warner* _warner);

  /// Generates a warning that there too many unique values.
  static void warn_too_many_unique(const Float _num_distinct,
                                   const std::string& _colname,
                                   const std::string& _df_name,
                                   communication::Warner* _warner);

  /// Generates a warning that the share of unique columns is too high.
  static void warn_unique_share_too_high(const Float _unique_share,
                                         const std::string& _colname,
                                         const std::string& _df_name,
                                         communication::Warner* _warner);

  // -------------------------------------------------------------------------

 private:
  /// Standard header for a column that should be unused.
  static std::string column_should_be_unused() {
    return warning() + "[COLUMN SHOULD BE UNUSED]: ";
  }

  /// Standard header for an ill-defined data model.
  static std::string data_model_can_be_improved() {
    return warning() + "[DATA MODEL CAN BE IMPROVED]: ";
  }

  /// Standard header for an info message.
  static std::string info() { return "INFO "; }

  /// Checks whether ts1 lies between ts2 and upper.
  static bool is_in_range(const Float ts1, const Float ts2, const Float upper) {
    return ts2 <= ts1 && (std::isnan(upper) || ts1 < upper);
  }

  /// Standard header for when some join keys where not found.
  static std::string join_keys_not_found() {
    return info() + "[FOREIGN KEYS NOT FOUND]: ";
  }

  /// Standard header for something that might take long.
  static std::string might_take_long() {
    return info() + "[MIGHT TAKE LONG]: ";
  }

  /// Removes any macros from a colname.
  static std::string modify_colname(const std::string& _colname) {
    const auto make_staging_table_colname =
        [](const std::string& _colname) -> std::string {
      return transpilation::HumanReadableSQLGenerator()
          .make_staging_table_colname(_colname);
    };
    const auto colnames = helpers::Macros::modify_colnames(
        {_colname}, make_staging_table_colname);
    assert_true(colnames.size() == 1);
    return colnames.at(0);
  }

  /// Standard header for a warning message.
  static std::string warning() { return "WARNING "; }
};

}  // namespace preprocessors
}  // namespace engine

// ----------------------------------------------------------------------------

#endif  // ENGINE_PREPROCESSORS_DATAMODELCHECKER_HPP_

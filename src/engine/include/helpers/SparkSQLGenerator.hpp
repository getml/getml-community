#ifndef HELPERS_SPARKSQLGENERATOR_HPP_
#define HELPERS_SPARKSQLGENERATOR_HPP_

// -------------------------------------------------------------------------

#include <cstddef>

// -------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// -------------------------------------------------------------------------

#include "helpers/enums/enums.hpp"

// -------------------------------------------------------------------------

#include "helpers/ColumnDescription.hpp"
#include "helpers/SQLDialectGenerator.hpp"
#include "helpers/Schema.hpp"

// -------------------------------------------------------------------------

namespace helpers {
// -------------------------------------------------------------------------

class SparkSQLGenerator : public SQLDialectGenerator {
 private:
  /// The number of autofeatures inserted into a feature table at the same
  /// time.
  static constexpr size_t BATCH_SIZE = 50;

 public:
  SparkSQLGenerator() {}

  ~SparkSQLGenerator() = default;

 public:
  /// The first quotechar.
  std::string quotechar1() const final { return "`"; }

  /// The second quotechar.
  std::string quotechar2() const final { return "`"; }

 public:
  /// Expresses an aggregation in the SQL dialect.
  std::string aggregation(
      const enums::Aggregation& _agg, const std::string& _colname1,
      const std::optional<std::string>& _colname2) const final;

  /// Removes the Macros from the colname and replaces it with proper SQLite3
  /// code.
  std::string edit_colname(const std::string& _raw_name,
                           const std::string& _alias) const final;

  /// Generates the SQL code necessary for joining the mapping tables onto the
  /// staged table.
  std::string join_mapping(const std::string& _name,
                           const std::string& _colname,
                           const bool _is_text) const final;

  /// Makes a clean, but unique colname.
  std::string make_colname(const std::string& _colname) const final;

  /// Generates the table that contains all the features.
  std::string make_feature_table(const std::string& _main_table,
                                 const std::vector<std::string>& _autofeatures,
                                 const std::vector<std::string>& _targets,
                                 const std::vector<std::string>& _categorical,
                                 const std::vector<std::string>& _numerical,
                                 const std::string& _prefix) const final;

  /// Generates the joins to be included in every single .
  std::string make_joins(const std::string& _output_name,
                         const std::string& _input_name,
                         const std::string& _output_join_keys_name,
                         const std::string& _input_join_keys_name) const final;

  /// Generates the SQL code needed to impute the features and drop the
  /// feature tables.
  std::string make_postprocessing(
      const std::vector<std::string>& _sql) const final;

  /// Generates the table header for the SQL code of the mapping
  std::string make_mapping_table_header(const std::string& _name,
                                        const bool _key_is_num) const final;

  /// Generates the select statement for the feature table.
  std::string make_select(
      const std::string& _main_table,
      const std::vector<std::string>& _autofeatures,
      const std::vector<std::string>& _targets,
      const std::vector<std::string>& _categorical,
      const std::vector<std::string>& _numerical) const final;

  /// Transpiles the features in SQLite3 code. This
  /// is supposed to replicate the .transform(...) method
  /// of a pipeline.
  std::string make_sql(const std::string& _main_table,
                       const std::vector<std::string>& _autofeatures,
                       const std::vector<std::string>& _sql,
                       const std::vector<std::string>& _targets,
                       const std::vector<std::string>& _categorical,
                       const std::vector<std::string>& _numerical) const final;

  /// Generates the staging tables.
  std::vector<std::string> make_staging_tables(
      const bool _population_needs_targets,
      const std::vector<bool>& _peripheral_needs_targets,
      const Schema& _population_schema,
      const std::vector<Schema>& _peripheral_schema) const final;

  /// Generates the code for joining the subfeature tables.
  std::string make_subfeature_joins(
      const std::string& _feature_prefix, const size_t _peripheral_used,
      const std::string& _alias = "t2",
      const std::string& _feature_postfix = "") const final;

  /// Generates the code for the time stamp conditions.
  std::string make_time_stamps(const std::string& _time_stamp_name,
                               const std::string& _lower_time_stamp_name,
                               const std::string& _upper_time_stamp_name,
                               const std::string& _output_alias,
                               const std::string& _input_alias,
                               const std::string& _t1_or_t2) const final;

  /// Generates code for the text field splitter.
  std::string split_text_fields(
      const std::shared_ptr<ColumnDescription>& _desc) const final;

  /// Generates code to check whether a string contains another string.
  std::string string_contains(const std::string& _colname,
                              const std::string& _keyword,
                              const bool _contains) const final;

 private:
  /// Generates the SQL code for AVG_TIME_BETWEEN.
  std::string avg_time_between_aggregation(const std::string& _colname1,
                                           const std::string& _colname2) const;

  /// Parses the prefix,  the new name and the postfix out of the
  /// raw name.
  std::tuple<std::string, std::string, std::string> demangle_colname(
      const std::string& _raw_name) const;

  /// Generates the SQL code for COUNT_ABOVE_MEAN and COUNT_BELOW_MEAN.
  std::string count_above_below_mean_aggregation(const std::string& _colname1,
                                                 const bool _above) const;

  /// Generates the SQL code for dropping a batch of features (for when there
  /// are too many features to join in one query).
  std::string drop_batch_tables(const std::vector<std::string>& _autofeatures,
                                const std::string& _prefix) const;

  /// Generates the SQL code for FIRST and LAST.
  std::string first_last_aggregation(const std::string& _colname1,
                                     const std::string& _colname2,
                                     const bool _first) const;

  /// Generates the SQL code for FIRST_MAXIMUM, FIRST_MINIMUM, etc.
  std::string first_or_last_optimum_aggregation(const std::string& _colname1,
                                                const std::string& _colname2,
                                                const bool _is_first,
                                                const bool _is_minimum) const;

  /// Escape chars need to be displayed properly in the resulting SQL code.
  std::string handle_escape_char(const char c) const;

  /// Generates the SQL code for joining a batch of features (for when there
  /// are too many features to join in one query).
  std::string join_batch_tables(const std::vector<std::string>& _autofeatures,
                                const std::string& _prefix) const;

  /// Generates the SQL code for a batch of features (for when there are too
  /// many features to join in one query).
  std::string make_batch_tables(const std::string& _main_table,
                                const std::vector<std::string>& _autofeatures,
                                const std::string& _prefix) const;

  /// Generates the code for exponentially weighted moving averages.
  std::string make_ewma_aggregation(const enums::Aggregation& _agg,
                                    const std::string& _value,
                                    const std::string& _timestamp) const;

  /// Generates the LEFT JOINS required to join all of the features to the
  /// feature table.
  std::string make_feature_joins(
      const std::vector<std::string>& _autofeatures) const;

  /// Generates the columns for a single staging table.
  std::vector<std::string> make_staging_columns(const bool& _include_targets,
                                                const Schema& _schema) const;

  /// Generates the SQL code for the MODE aggreagation.
  std::string mode_aggregation(const std::string& _colname1) const;

  /// Generates the separators for the text field splitter.
  std::string make_separators() const;

  /// Generates a single staging table.
  std::string make_staging_table(const bool& _include_targets,
                                 const Schema& _schema) const;

  /// Generates the code for a linear trend.
  std::string make_trend_aggregation(const std::string& _value,
                                     const std::string& _timestamp) const;

  /// Generates the SQL code for NUM_MAX and NUM_MIN.
  std::string num_max_min_aggregation(const std::string& _colname1,
                                      const bool _max) const;

  /// Returns a set of replace statements for the string separators.
  std::string replace_separators(const std::string& _col) const;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_SPARKSQLGENERATOR_HPP_

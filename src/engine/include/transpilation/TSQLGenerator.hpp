#ifndef SQL_TSQLGENERATOR_HPP_
#define SQL_TSQLGENERATOR_HPP_

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
#include "helpers/helpers.hpp"

// -------------------------------------------------------------------------

#include "transpilation/SQLDialectGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

// -------------------------------------------------------------------------

namespace transpilation {

class TSQLGenerator : public SQLDialectGenerator {
 public:
  explicit TSQLGenerator(const TranspilationParams& _params)
      : params_(_params) {}

  ~TSQLGenerator() = default;

 public:
  /// The first quotechar.
  std::string quotechar1() const final { return "["; }

  /// The second quotechar.
  std::string quotechar2() const final { return "]"; }

  /// How the SQL dialect expresses rowid
  std::string rowid() const final {
    return quotechar1() + "rowid" + quotechar2();
  }

  /// How the SQL dialect expresses rownum
  std::string rownum() const final { return rowid(); }

  /// The schema to precede any newly created tables.
  std::string schema() const final {
    return params_.schema_ == ""
               ? ""
               : quotechar1() + params_.schema_ + quotechar2() + ".";
  }

 public:
  /// Expresses an aggregation in the SQL dialect.
  std::string aggregation(
      const helpers::enums::Aggregation& _agg, const std::string& _colname1,
      const std::optional<std::string>& _colname2) const final;

  /// Generates a CREATE TABLE statement, to be used for a feature.
  std::string create_table(const helpers::enums::Aggregation& _agg,
                           const std::string& _feature_prefix,
                           const std::string& _feature_num) const final;

  /// Generates a DROP TABLE IF EXISTS statement.
  std::string drop_table_if_exists(const std::string& _table_name) const final;

  /// Generates the GROUP BY statement for the feature (it is not needed for
  /// some aggregations in some dialects, therefore it needs to be abstracted
  /// away.)
  std::string group_by(const helpers::enums::Aggregation _agg,
                       const std::string& _value_to_be_aggregated) const final;

  /// Generates the SQL code necessary for joining the mapping tables onto the
  /// staged table.
  std::string join_mapping(const std::string& _name,
                           const std::string& _colname,
                           const bool _is_text) const final;

  /// Removes the Macros from the colname and replaces it with proper TSQL
  /// code.
  std::string make_staging_table_column(const std::string& _raw_name,
                                        const std::string& _alias) const final;

  /// Makes a clean, but unique colname.
  std::string make_staging_table_colname(
      const std::string& _colname) const final;

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

  /// Generates the table header for the SQL code of the mapping.
  std::string make_mapping_table_header(const std::string& _name,
                                        const bool _key_is_num) const final;

  /// Generates the INSERT INTO for the SQL code of the mapping.
  std::string make_mapping_table_insert_into(
      const std::string& _name) const final;

  /// Generates the SQL code needed to impute the features and drop the
  /// feature tables.
  std::string make_postprocessing(
      const std::vector<std::string>& _sql) const final;

  /// Generates the select statement for the feature table.
  std::string make_select(
      const std::string& _main_table,
      const std::vector<std::string>& _autofeatures,
      const std::vector<std::string>& _targets,
      const std::vector<std::string>& _categorical,
      const std::vector<std::string>& _numerical) const final;

  /// Transpiles the features in TSQL code. This
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
      const helpers::Schema& _population_schema,
      const std::vector<helpers::Schema>& _peripheral_schema) const final;

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

  /// Generates the SQL code for the NUM_MAX and NUM_MIN aggregation.
  std::string num_max_min_aggregation(const std::string& _colname,
                                      const bool _max) const;

  /// Generates code for the text field splitter, and is also used by the
  /// mapping.
  std::string split_text_fields(
      const std::shared_ptr<helpers::ColumnDescription>& _desc,
      const bool _for_mapping) const final;

  /// Generates code to check whether a string contains another string.
  std::string string_contains(const std::string& _colname,
                              const std::string& _keyword,
                              const bool _contains) const final;

 private:
  /// Creates the indices for a staging table.
  std::string create_indices(const std::string& _table_name,
                             const helpers::Schema& _schema) const;

  /// Parses the prefix,  the new name and the postfix out of the
  /// raw name.
  std::tuple<std::string, std::string, std::string> demangle_colname(
      const std::string& _raw_name) const;

  /// Edits the pre- and postfix to account for the way TSQL deals with time
  /// intervals.
  std::pair<std::string, std::string> edit_prefix_postfix(
      const std::string& _raw_name, const std::string& _prefix,
      const std::string& _postfix) const;

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

  /// Generates the code for exponentially weighted moving averages.
  std::string make_ewma_aggregation(const helpers::enums::Aggregation& _agg,
                                    const std::string& _value,
                                    const std::string& _timestamp) const;

  /// Generates the aggregation for kurtosis.
  std::string make_kurtosis_aggregation(const std::string& _value) const;

  /// Generates the MODE aggregation
  std::string make_mode_aggregation(const std::string& _colname) const;

  /// Generates the columns to insert into ROW_NUMBER ORDER BY.
  std::string make_order_by(const helpers::Schema& _schema) const;

  /// Generates the percentile aggregation.
  std::string make_percentile_aggregation(const std::string& _colname1,
                                          const std::string& _q) const;

  /// Generate the outer aggregation, if needed.
  std::optional<std::string> make_outer_aggregation(
      const helpers::enums::Aggregation& _agg) const;

  /// Generates the aggregation for kurtosis.
  std::string make_skewness_aggregation(const std::string& _value) const;

  /// Generates the code for a linear trend.
  std::string make_trend_aggregation(const std::string& _value,
                                     const std::string& _timestamp) const;

  /// Generates the SQL code needed to insert the autofeatures into the
  /// FEATURES table.
  std::string make_updates(const std::vector<std::string>& _autofeatures,
                           const std::string& _prefix) const;

  /// Generates the columns for a single staging table.
  std::vector<std::string> make_staging_columns(
      const bool& _include_targets, const helpers::Schema& _schema) const;

  /// Generates a single staging table.
  std::string make_staging_table(const bool& _include_targets,
                                 const helpers::Schema& _schema) const;

  /// Returns a set of replace statements for the string separators.
  std::string replace_separators(const std::string& _col) const;

 private:
  /// The underlying parameters
  const TranspilationParams params_;
};

// -------------------------------------------------------------------------
}  // namespace transpilation

#endif  // SQL_TSQLGENERATOR_HPP_

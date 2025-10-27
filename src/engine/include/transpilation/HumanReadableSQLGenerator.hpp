// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef SQL_HUMANREADABLESQLGENERATOR_HPP_
#define SQL_HUMANREADABLESQLGENERATOR_HPP_

#include "helpers/ColumnDescription.hpp"
#include "helpers/Schema.hpp"
#include "helpers/enums/Aggregation.hpp"
#include "transpilation/FeatureTableParams.hpp"
#include "transpilation/HumanReadableTrimming.hpp"
#include "transpilation/SQLDialectGenerator.hpp"
#include "transpilation/SQLParams.hpp"

#include <rfl/Ref.hpp>

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

namespace transpilation {

class HumanReadableSQLGenerator final : public SQLDialectGenerator {
 public:
  HumanReadableSQLGenerator() = default;

  ~HumanReadableSQLGenerator() final = default;

 public:
  /// Generates the GROUP BY statement for the feature (it is not needed for
  /// some aggregations in some dialects, therefore it needs to be abstracted
  /// away.)
  std::string group_by(const helpers::enums::Aggregation,
                       const std::string& = "") const final;

  /// The first quotechar.
  std::string quotechar1() const final { return "\""; }

  /// The second quotechar.
  std::string quotechar2() const final { return "\""; }

  /// How the SQL dialect expresses rowid
  std::string rowid() const final { return "rowid"; }

  /// How the SQL dialect expresses rownum
  std::string rownum() const final { return "rownum"; }

  /// The schema to precede any newly created tables.
  std::string schema() const final { return ""; }

  /// Only needed for the CategoryTrimmer preprocesser.
  rfl::Ref<TrimmingGenerator> trimming() const final {
    return rfl::Ref<HumanReadableTrimming>::make(this);
  };

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

  /// Removes the Macros from the colname and replaces it with proper SQLite3
  /// code.
  std::string make_staging_table_column(const std::string& _raw_name,
                                        const std::string& _alias) const final;

  /// Makes a clean, but unique colname.
  std::string make_staging_table_colname(
      const std::string& _colname) const final;

  /// Generates the table that contains all the features.
  std::string make_feature_table(const FeatureTableParams& _params) const final;

  /// Generates the joins to be included in every single .
  std::string make_joins(const std::string& _output_name,
                         const std::string& _input_name,
                         const std::string& _output_join_keys_name,
                         const std::string& _input_join_keys_name) const final;

  /// Generates the SQL code needed to impute the features and drop the
  /// feature tables.
  std::string make_postprocessing(
      const std::vector<std::string>& _sql) const final;

  /// Generates the select statement for the feature table.
  std::string make_select(const FeatureTableParams& _params) const final;

  /// Transpiles the features in SQLite3 code. This
  /// is supposed to replicate the .transform(...) method
  /// of a pipeline.
  std::string make_sql(const SQLParams& _params) const final;

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
};

// -------------------------------------------------------------------------
}  // namespace transpilation

#endif  // SQL_HUMANREADABLESQLGENERATOR_HPP_

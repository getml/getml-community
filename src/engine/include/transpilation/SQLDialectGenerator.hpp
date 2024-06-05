// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef SQL_SQLDIALECTGENERATOR_HPP_
#define SQL_SQLDIALECTGENERATOR_HPP_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "helpers/ColumnDescription.hpp"
#include "helpers/Schema.hpp"
#include "helpers/enums/Aggregation.hpp"
#include <rfl/Ref.hpp>
#include "transpilation/FeatureTableParams.hpp"
#include "transpilation/SQLParams.hpp"
#include "transpilation/TrimmingGenerator.hpp"

namespace transpilation {

class SQLDialectGenerator {
 public:
  virtual ~SQLDialectGenerator() = default;

  /// Expresses an aggregation in the SQL dialect.
  virtual std::string aggregation(
      const helpers::enums::Aggregation& _agg, const std::string& _colname1,
      const std::optional<std::string>& _colname2) const = 0;

  /// Generates a CREATE TABLE statement, to be used for a feature.
  virtual std::string create_table(const helpers::enums::Aggregation& _agg,
                                   const std::string& _feature_prefix,
                                   const std::string& _feature_num) const = 0;

  /// Generates a DROP TABLE IF EXISTS statement.
  virtual std::string drop_table_if_exists(
      const std::string& _table_name) const = 0;

  /// Generates the GROUP BY statement for the feature (it is not needed for
  /// some aggregations in some dialects, therefore it needs to be abstracted
  /// away.)
  virtual std::string group_by(
      const helpers::enums::Aggregation _agg,
      const std::string& _value_to_be_aggregated = "") const = 0;

  /// Removes the Macros from the colname and replaces it with proper SQLite3
  /// code.
  virtual std::string make_staging_table_column(
      const std::string& _raw_name, const std::string& _alias) const = 0;

  /// Makes a clean, but unique colname.
  virtual std::string make_staging_table_colname(
      const std::string& _colname) const = 0;

  /// Generates the table that contains all the features.
  virtual std::string make_feature_table(
      const FeatureTableParams& _params) const = 0;

  /// Generates the joins to be included in every single .
  virtual std::string make_joins(
      const std::string& _output_name, const std::string& _input_name,
      const std::string& _output_join_keys_name,
      const std::string& _input_join_keys_name) const = 0;

  /// Generates the SQL code needed to impute the features and drop the
  /// feature tables.
  virtual std::string make_postprocessing(
      const std::vector<std::string>& _sql) const = 0;

  /// Generates the select statement for the feature table.
  virtual std::string make_select(const FeatureTableParams& _params) const = 0;

  /// Transpiles the features in SQL code. This
  /// is supposed to replicate the .transform(...) method
  /// of a pipeline.
  virtual std::string make_sql(const SQLParams& _params) const = 0;

  /// Generates the staging tables.
  virtual std::vector<std::string> make_staging_tables(
      const bool _population_needs_targets,
      const std::vector<bool>& _peripheral_needs_targets,
      const helpers::Schema& _population_schema,
      const std::vector<helpers::Schema>& _peripheral_schema) const = 0;

  /// Generates the code for joining the subfeature tables.
  virtual std::string make_subfeature_joins(
      const std::string& _feature_prefix, const size_t _peripheral_used,
      const std::string& _alias = "t2",
      const std::string& _feature_postfix = "") const = 0;

  /// Generates the code for the time stamp conditions.
  virtual std::string make_time_stamps(
      const std::string& _time_stamp_name,
      const std::string& _lower_time_stamp_name,
      const std::string& _upper_time_stamp_name,
      const std::string& _output_alias, const std::string& _input_alias,
      const std::string& _t1_or_t2) const = 0;

  /// How the SQL dialect expresses rowid
  virtual std::string rowid() const = 0;

  /// How the SQL dialect expresses rownum
  virtual std::string rownum() const = 0;

  /// The first quotechar.
  virtual std::string quotechar1() const = 0;

  /// The second quotechar.
  virtual std::string quotechar2() const = 0;

  /// The schema to precede any newly created tables.
  virtual std::string schema() const = 0;

  /// Generates code for the text field splitter, and is also used by the
  /// mapping.
  virtual std::string split_text_fields(
      const std::shared_ptr<helpers::ColumnDescription>& _desc,
      const bool _for_mapping = false) const = 0;

  /// Generates code to check whether a string contains another string.
  virtual std::string string_contains(const std::string& _colname,
                                      const std::string& _keyword,
                                      const bool _contains) const = 0;

  /// Only needed for the CategoryTrimmer preprocesser.
  virtual rfl::Ref<TrimmingGenerator> trimming() const = 0;
};

}  // namespace transpilation

#endif  // SQL_SQLDIALECTGENERATOR_HPP_

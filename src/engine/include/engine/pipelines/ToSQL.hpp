// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_PIPELINES_TOSQL_HPP_
#define ENGINE_PIPELINES_TOSQL_HPP_

// ----------------------------------------------------------------------------

#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/FittedPipeline.hpp"
#include "engine/pipelines/Pipeline.hpp"
#include "engine/pipelines/ToSQLParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

class ToSQL {
 public:
  /// Expresses features as SQL code.
  static std::string to_sql(const ToSQLParams& _params);

 private:
  /// Expresses the feature learners as SQL code.
  static std::vector<std::string> feature_learners_to_sql(
      const ToSQLParams& _params,
      const fct::Ref<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator);

  /// Generates the names of the autofeatures to be included in the transpiled
  /// SQL code.
  static std::vector<std::string> make_autofeature_names(
      const FittedPipeline& _fitted);

  /// Generates the schemata needed for the SQL generation of the staging
  /// tables.
  static std::pair<containers::Schema, std::vector<containers::Schema>>
  make_staging_schemata(const FittedPipeline& _fitted);

  /// Sometimes features can get excessively long, which makes it hard to
  /// display them in the iPython notebook. This takes care of this problem.
  static std::vector<std::string> overwrite_oversized_features(
      const fct::Ref<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::vector<std::string>& _autofeatures,
      const std::optional<size_t> _size_threshold);

  /// Parses the feature name from the code.
  static std::string parse_feature_name(
      const fct::Ref<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const std::string& _code);

  /// Expresses the preprocessing part as SQL code.
  static std::vector<std::string> preprocessors_to_sql(
      const ToSQLParams& _params,
      const fct::Ref<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator);

  /// Expresses the staging part as SQL code.
  static std::vector<std::string> staging_to_sql(
      const ToSQLParams& _params,
      const fct::Ref<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator);
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_TOSQL_HPP_

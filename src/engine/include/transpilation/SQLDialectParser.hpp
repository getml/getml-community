// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef SQL_SQLDIALECTPARSER_HPP_
#define SQL_SQLDIALECTPARSER_HPP_

#include <cstddef>
#include <memory>
#include <string>

#include <rfl/Ref.hpp>
#include "transpilation/SQLDialectGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

struct SQLDialectParser {
  static constexpr const char* BIG_QUERY = "bigquery";
  static constexpr const char* HUMAN_READABLE_SQL = "human-readable sql";
  static constexpr const char* MYSQL = "mysql";
  static constexpr const char* POSTGRE_SQL = "postgres";
  static constexpr const char* SPARK_SQL = "spark sql";
  static constexpr const char* SQLITE3 = "sqlite3";
  static constexpr const char* TSQL = "tsql";

  /// Parse returns the SQLDialect generator for the dialect.
  static rfl::Ref<const SQLDialectGenerator> parse(
      const TranspilationParams& _params);
};

}  // namespace transpilation

#endif  // SQL_SQLDIALECTPARSER_HPP_

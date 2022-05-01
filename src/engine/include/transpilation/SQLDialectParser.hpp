#ifndef SQL_SQLDIALECTPARSER_HPP_
#define SQL_SQLDIALECTPARSER_HPP_

// -------------------------------------------------------------------------

#include <cstddef>

// -------------------------------------------------------------------------

#include <memory>
#include <string>

// -------------------------------------------------------------------------

#include "fct/Ref.hpp"

// -------------------------------------------------------------------------

#include "transpilation/SQLDialectGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

// -------------------------------------------------------------------------

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
  static fct::Ref<const SQLDialectGenerator> parse(
      const TranspilationParams& _params);
};

// -------------------------------------------------------------------------
}  // namespace transpilation

#endif  // SQL_SQLDIALECTPARSER_HPP_

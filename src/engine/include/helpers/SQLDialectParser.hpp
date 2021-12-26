#ifndef HELPERS_SQLDIALECTPARSER_HPP_
#define HELPERS_SQLDIALECTPARSER_HPP_

// -------------------------------------------------------------------------

#include <cstddef>

// -------------------------------------------------------------------------

#include <memory>
#include <string>

// -------------------------------------------------------------------------

#include "helpers/SQLDialectGenerator.hpp"

// -------------------------------------------------------------------------

namespace helpers {
// -------------------------------------------------------------------------

struct SQLDialectParser {
  static constexpr const char* SPARK_SQL = "spark sql";
  static constexpr const char* SQLITE3 = "sqlite3";

  /// Parse returns the SQLDialect generator for the dialect.
  static std::shared_ptr<const SQLDialectGenerator> parse(
      const std::string& _dialect);
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_SQLDIALECTPARSER_HPP_

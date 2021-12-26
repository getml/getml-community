#include "helpers/SQLDialectParser.hpp"

#include "helpers/SQLite3Generator.hpp"
#include "helpers/SparkSQLGenerator.hpp"

namespace helpers {
// ----------------------------------------------------------------------------

std::shared_ptr<const SQLDialectGenerator> SQLDialectParser::parse(
    const std::string& _dialect) {
  if (_dialect == SPARK_SQL) {
    return std::make_shared<const SparkSQLGenerator>();
  }

  if (_dialect == SQLITE3) {
    return std::make_shared<const SQLite3Generator>();
  }

  throw std::invalid_argument("Unknown SQL dialect: '" + _dialect + "'.");

  return std::shared_ptr<const SQLDialectGenerator>();
}

// ----------------------------------------------------------------------------
}  // namespace helpers

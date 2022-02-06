#include "transpilation/SQLDialectParser.hpp"

#include "transpilation/PostgreSQLGenerator.hpp"
#include "transpilation/SQLite3Generator.hpp"
#include "transpilation/SparkSQLGenerator.hpp"
#include "transpilation/TSQLGenerator.hpp"

namespace transpilation {

std::shared_ptr<const SQLDialectGenerator> SQLDialectParser::parse(
    const std::string& _dialect, const std::string& _schema) {
  if (_dialect == POSTGRE_SQL) {
    return std::make_shared<const PostgreSQLGenerator>(_schema);
  }

  if (_dialect == SPARK_SQL) {
    return std::make_shared<const SparkSQLGenerator>();
  }

  if (_dialect == SQLITE3) {
    return std::make_shared<const SQLite3Generator>();
  }

  if (_dialect == TSQL) {
    return std::make_shared<const TSQLGenerator>();
  }

  throw std::invalid_argument("Unknown SQL dialect: '" + _dialect + "'.");

  return std::shared_ptr<const SQLDialectGenerator>();
}

}  // namespace transpilation

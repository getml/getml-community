#include "transpilation/SQLDialectParser.hpp"

#include "transpilation/BigQueryGenerator.hpp"
#include "transpilation/MySQLGenerator.hpp"
#include "transpilation/PostgreSQLGenerator.hpp"
#include "transpilation/SQLite3Generator.hpp"
#include "transpilation/SparkSQLGenerator.hpp"
#include "transpilation/TSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

std::shared_ptr<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  if (_params.dialect_ == BIG_QUERY) {
    return std::make_shared<const BigQueryGenerator>(_params);
  }

  if (_params.dialect_ == MYSQL) {
    return std::make_shared<const MySQLGenerator>(_params);
  }

  if (_params.dialect_ == POSTGRE_SQL) {
    return std::make_shared<const PostgreSQLGenerator>(_params);
  }

  if (_params.dialect_ == SPARK_SQL) {
    return std::make_shared<const SparkSQLGenerator>();
  }

  if (_params.dialect_ == SQLITE3) {
    return std::make_shared<const SQLite3Generator>();
  }

  if (_params.dialect_ == TSQL) {
    return std::make_shared<const TSQLGenerator>(_params);
  }

  throw std::invalid_argument("Unknown SQL dialect: '" + _params.dialect_ +
                              "'.");

  return std::shared_ptr<const SQLDialectGenerator>();
}

}  // namespace transpilation

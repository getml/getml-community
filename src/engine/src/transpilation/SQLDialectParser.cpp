#include "transpilation/SQLDialectParser.hpp"

#include "transpilation/BigQueryGenerator.hpp"
#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/MySQLGenerator.hpp"
#include "transpilation/PostgreSQLGenerator.hpp"
#include "transpilation/SparkSQLGenerator.hpp"
#include "transpilation/TSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

fct::Ref<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  if (_params.dialect_ == BIG_QUERY) {
    return fct::Ref<const BigQueryGenerator>::make(_params);
  }

  if (_params.dialect_ == HUMAN_READABLE_SQL) {
    return fct::Ref<const HumanReadableSQLGenerator>::make();
  }

  if (_params.dialect_ == MYSQL) {
    return fct::Ref<const MySQLGenerator>::make(_params);
  }

  if (_params.dialect_ == POSTGRE_SQL) {
    return fct::Ref<const PostgreSQLGenerator>::make(_params);
  }

  if (_params.dialect_ == SPARK_SQL) {
    return fct::Ref<const SparkSQLGenerator>::make();
  }

  if (_params.dialect_ == SQLITE3) {
    return fct::Ref<const HumanReadableSQLGenerator>::make();
  }

  if (_params.dialect_ == TSQL) {
    return fct::Ref<const TSQLGenerator>::make(_params);
  }

  throw std::runtime_error("Unknown SQL dialect: '" + _params.dialect_ + "'.");
}

}  // namespace transpilation

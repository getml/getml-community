#include "transpilation/SQLDialectParser.hpp"

#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

fct::Ref<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  if (_params.dialect_ == HUMAN_READABLE_SQL) {
    return fct::Ref<const HumanReadableSQLGenerator>::make();
  }

  if (_params.dialect_ == BIG_QUERY || _params.dialect_ == MYSQL ||
      _params.dialect_ == POSTGRE_SQL || _params.dialect_ == SPARK_SQL ||
      _params.dialect_ == SQLITE3 || _params.dialect_ == TSQL) {
    throw std::runtime_error(
        "Transpiling to '" + _params.dialect_ +
        "' is not supported in the getML community edition.");
  }

  throw std::runtime_error("Unknown SQL dialect: '" + _params.dialect_ + "'.");
}

}  // namespace transpilation

// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "transpilation/SQLDialectParser.hpp"

#include "transpilation/HumanReadableSQLGenerator.hpp"
#include "transpilation/TranspilationParams.hpp"

namespace transpilation {

fct::Ref<const SQLDialectGenerator> SQLDialectParser::parse(
    const TranspilationParams& _params) {
  if (_params.dialect_ == HUMAN_READABLE_SQL || _params.dialect_ == SQLITE3) {
    return fct::Ref<const HumanReadableSQLGenerator>::make();
  }

  if (_params.dialect_ == BIG_QUERY || _params.dialect_ == MYSQL ||
      _params.dialect_ == POSTGRE_SQL || _params.dialect_ == SPARK_SQL ||
      _params.dialect_ == TSQL) {
    throw std::runtime_error(
        "Transpiling to '" + _params.dialect_ +
        "' is not supported in the getML community edition.");
  }

  throw std::runtime_error("Unknown SQL dialect: '" + _params.dialect_ + "'.");
}

}  // namespace transpilation

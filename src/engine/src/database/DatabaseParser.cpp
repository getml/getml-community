// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/DatabaseParser.hpp"

#include "database/MySQL.hpp"
#include "database/Postgres.hpp"
#include "database/Sqlite3.hpp"
#include "fct/visit.hpp"

namespace database {

fct::Ref<Connector> DatabaseParser::parse(
    const typename Command::NamedTupleType& _cmd,
    const std::string& _password) {
  const auto handle = [&_password](const auto& _obj) -> fct::Ref<Connector> {
    using Type = std::decay_t<decltype(_obj)>;

    if constexpr (std::is_same<Type, typename Command::MySQLOp>()) {
      return fct::Ref<MySQL>::make(_obj, _password);
    }

    if constexpr (std::is_same<Type, typename Command::PostgresOp>()) {
      return fct::Ref<Postgres>::make(_obj, _password);
    }

    if constexpr (std::is_same<Type, typename Command::SQLite3Op>()) {
      return fct::Ref<Sqlite3>::make(_obj);
    }
  };

  return fct::visit(handle, _cmd);
}

}  // namespace database

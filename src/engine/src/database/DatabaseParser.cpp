// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/DatabaseParser.hpp"

#include "database/MySQL.hpp"
#include "database/Postgres.hpp"
#include "database/Sqlite3.hpp"

#include <rfl/visit.hpp>

namespace database {

rfl::Ref<Connector> DatabaseParser::parse(
    const typename Command::ReflectionType& _cmd,
    const std::string& _password) {
  const auto handle = [&_password](const auto& _obj) -> rfl::Ref<Connector> {
    using Type = std::decay_t<decltype(_obj)>;

    if constexpr (std::is_same<Type, typename Command::MySQLOp>()) {
      return rfl::Ref<MySQL>::make(_obj, _password);
    }

    if constexpr (std::is_same<Type, typename Command::PostgresOp>()) {
      return rfl::Ref<Postgres>::make(_obj, _password);
    }

    if constexpr (std::is_same<Type, typename Command::SQLite3Op>()) {
      return rfl::Ref<Sqlite3>::make(_obj);
    }
  };

  return rfl::visit(handle, _cmd);
}

}  // namespace database

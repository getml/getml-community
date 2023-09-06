// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_COMMAND_HPP_
#define DATABASE_COMMAND_HPP_

#include <optional>
#include <string>
#include <vector>

#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/TaggedUnion.hpp"
#include "rfl/define_named_tuple.hpp"

namespace database {

struct Command {
  /// Needed by every command to create a new database.
  using NewDBBasicOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.new">>,
                      rfl::Field<"conn_id_", std::string>>;

  /// The operation needed to create a MySQL connection.
  using MySQLOp = rfl::define_named_tuple_t<
      NewDBBasicOp, rfl::Field<"db_", rfl::Literal<"mysql", "mariadb">>,
      rfl::Field<"dbname_", std::string>, rfl::Field<"host_", std::string>,
      rfl::Field<"port_", unsigned int>,
      rfl::Field<"time_formats_", std::vector<std::string>>,
      rfl::Field<"unix_socket_", std::string>,
      rfl::Field<"user_", std::string>>;

  /// The operation needed to create a Postgres connection.
  using PostgresOp = rfl::define_named_tuple_t<
      NewDBBasicOp, rfl::Field<"db_", rfl::Literal<"postgres">>,
      rfl::Field<"dbname_", std::string>,
      rfl::Field<"host_", std::optional<std::string>>,
      rfl::Field<"hostaddr_", std::optional<std::string>>,
      rfl::Field<"port_", size_t>,
      rfl::Field<"time_formats_", std::vector<std::string>>,
      rfl::Field<"user_", std::string>>;

  /// The operation needed to create an SQLITE3 connection.
  using SQLite3Op = rfl::define_named_tuple_t<
      NewDBBasicOp, rfl::Field<"db_", rfl::Literal<"sqlite3">>,
      rfl::Field<"name_", std::string>,
      rfl::Field<"time_formats_", std::vector<std::string>>>;

  using NamedTupleType =
      rfl::TaggedUnion<"db_", MySQLOp, PostgresOp, SQLite3Op>;

  /// The underlying value
  const NamedTupleType val_;
};

}  // namespace database

#endif

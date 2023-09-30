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
  /// The operation needed to create a MySQL connection.
  struct MySQLOp {
    rfl::Field<"type_", rfl::Literal<"Database.new">> type;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"db_", rfl::Literal<"mysql", "mariadb">> db;
    rfl::Field<"dbname_", std::string> dbname;
    rfl::Field<"host_", std::string> host;
    rfl::Field<"port_", unsigned int> port;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
    rfl::Field<"unix_socket_", std::string> unix_socket;
    rfl::Field<"user_", std::string> user;
  };

  /// The operation needed to create a Postgres connection.
  struct PostgresOp {
    rfl::Field<"type_", rfl::Literal<"Database.new">> type;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"db_", rfl::Literal<"postgres">> db;
    rfl::Field<"dbname_", std::string> dbname;
    rfl::Field<"host_", std::optional<std::string>> host;
    rfl::Field<"hostaddr_", std::optional<std::string>> hostaddr;
    rfl::Field<"port_", size_t> port;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
    rfl::Field<"user_", std::string> user;
  };

  /// The operation needed to create an SQLITE3 connection.
  struct SQLite3Op {
    rfl::Field<"type_", rfl::Literal<"Database.new">> type;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"db_", rfl::Literal<"sqlite3">> db;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"time_formats_", std::vector<std::string>> time_formats;
  };

  using ReflectionType =
      rfl::TaggedUnion<"db_", MySQLOp, PostgresOp, SQLite3Op>;

  /// The underlying value
  const ReflectionType val_;
};

}  // namespace database

#endif

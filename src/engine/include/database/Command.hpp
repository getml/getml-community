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

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"

namespace database {

struct Command {
  /// Needed by every command to create a new database.
  using NewDBBasicOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.new">>,
                      fct::Field<"conn_id_", std::string>>;

  /// The operation needed to create a MySQL connection.
  using MySQLOp = fct::define_named_tuple_t<
      NewDBBasicOp, fct::Field<"db_", fct::Literal<"mysql", "mariadb">>,
      fct::Field<"dbname_", std::string>, fct::Field<"host_", std::string>,
      fct::Field<"port_", unsigned int>,
      fct::Field<"time_formats_", std::vector<std::string>>,
      fct::Field<"unix_socket_", std::string>,
      fct::Field<"user_", std::string>>;

  /// The operation needed to create a Postgres connection.
  using PostgresOp = fct::define_named_tuple_t<
      NewDBBasicOp, fct::Field<"db_", fct::Literal<"postgres">>,
      fct::Field<"dbname_", std::string>,
      fct::Field<"host_", std::optional<std::string>>,
      fct::Field<"hostaddr_", std::optional<std::string>>,
      fct::Field<"port_", size_t>,
      fct::Field<"time_formats_", std::vector<std::string>>,
      fct::Field<"user_", std::string>>;

  /// The operation needed to create an SQLITE3 connection.
  using SQLite3Op = fct::define_named_tuple_t<
      NewDBBasicOp, fct::Field<"db_", fct::Literal<"sqlite3">>,
      fct::Field<"name_", std::string>,
      fct::Field<"time_formats_", std::vector<std::string>>>;

  using NamedTupleType =
      fct::TaggedUnion<"db_", MySQLOp, PostgresOp, SQLite3Op>;

  /// The underlying value
  const NamedTupleType val_;
};

}  // namespace database

#endif

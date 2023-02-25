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
#include "fct/NamedTuple.hpp"
#include "fct/TaggedUnion.hpp"

namespace database {

struct Command {
  /// The operation needed to create a MySQL connection.
  using MySQLOp = fct::NamedTuple<
      fct::Field<"db_", fct::Literal<"mysql", "mariadb">>,
      fct::Field<"conn_id_", std::string>, fct::Field<"dbname_", std::string>,
      fct::Field<"host_", std::string>, fct::Field<"port_", unsigned int>,
      fct::Field<"time_formats_", std::vector<std::string>>,
      fct::Field<"unix_socket_", std::string>,
      fct::Field<"user_", std::string>>;

  /// The operation needed to create a Postgres connection.
  using PostgresOp =
      fct::NamedTuple<fct::Field<"db_", fct::Literal<"postgres">>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"dbname_", std::string>,
                      fct::Field<"host_", std::optional<std::string>>,
                      fct::Field<"hostaddr_", std::optional<std::string>>,
                      fct::Field<"port_", size_t>,
                      fct::Field<"time_formats_", std::vector<std::string>>,
                      fct::Field<"user_", std::string>>;

  /// The operation needed to create an SQLITE3 connection.
  using SQLite3Op =
      fct::NamedTuple<fct::Field<"db_", fct::Literal<"sqlite3">>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"time_formats_", std::vector<std::string>>>;

  using NamedTupleType =
      fct::TaggedUnion<"db_", MySQLOp, PostgresOp, SQLite3Op>;

  /// The underlying value
  const NamedTupleType val_;
};

}  // namespace database

#endif

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATABASECOMMAND_HPP_
#define COMMANDS_DATABASECOMMAND_HPP_

#include <Poco/JSON/Object.h>

#include <string>

#include "commands/Int.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "json/json.hpp"

namespace commands {

/// Any command to be handled by the DatabaseHandler.
struct DatabaseCommand {
  /// The operation needed to copy a table.
  using CopyTableOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.copy_table">>,
                      fct::Field<"source_conn_id_", std::string>,
                      fct::Field<"source_table_", std::string>,
                      fct::Field<"target_conn_id_", std::string>,
                      fct::Field<"target_table_", std::string>>;

  /// The operation needed to describe a connection.
  using DescribeConnectionOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Database.describe_connection">>,
      fct::Field<"name_", std::string>>;

  /// The operation needed to drop a table.
  using DropTableOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.drop_table">>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"name_", std::string>>;

  /// The operation needed to execute a query.
  using ExecuteOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.execute">>,
                      fct::Field<"name_", std::string>>;

  /// The operation needed to get a table.
  using GetOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.get">>,
                      fct::Field<"name_", std::string>>;

  /// The operation needed to get colnames.
  using GetColnamesOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Database.get_colnames">>,
      fct::Field<"conn_id_", std::string>, fct::Field<"name_", std::string>,
      fct::Field<"query_", std::optional<std::string>>>;

  /// The operation needed to get the content of a table.
  using GetContentOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.get_content">>,
                      fct::Field<"name_", std::string>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"draw_", Int>, fct::Field<"length_", Int>,
                      fct::Field<"start_", Int>>;

  /// The operation needed to the number of rows.
  using GetNRowsOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.get_nrows">>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"name_", std::string>>;

  /// The operation needed to list the connections.
  using ListConnectionsOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Database.list_connections">>>;

  /// The operation needed to list the tables.
  using ListTablesOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.list_tables">>,
                      fct::Field<"name_", std::string>>;

  /// The operation needed to create a new connection.
  using NewDBOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.new">>,
                      fct::Field<"conn_id_", std::string>>;

  /// The operation needed to read data from a CSV file.
  using ReadCSVOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Database.read_csv">>,
      fct::Field<"colnames_", std::optional<std::vector<std::string>>>,
      fct::Field<"conn_id_", std::string>,
      fct::Field<"fnames_", std::vector<std::string>>,
      fct::Field<"name_", std::string>, fct::Field<"num_lines_read_", size_t>,
      fct::Field<"quotechar_", std::string>, fct::Field<"sep_", std::string>,
      fct::Field<"skip_", size_t>>;

  /// The operation needed to refresh the contents of the database.
  using RefreshOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.refresh">>>;

  /// The operation needed to sniff the data in a CSV file.
  using SniffCSVOp = fct::NamedTuple<
      fct::Field<"type_", fct::Literal<"Database.sniff_csv">>,
      fct::Field<"colnames_", std::optional<std::vector<std::string>>>,
      fct::Field<"conn_id_", std::string>,
      fct::Field<"dialect_", std::optional<fct::Literal<"python">>>,
      fct::Field<"fnames_", std::vector<std::string>>,
      fct::Field<"name_", std::string>,
      fct::Field<"num_lines_sniffed_", size_t>,
      fct::Field<"quotechar_", std::string>, fct::Field<"sep_", std::string>,
      fct::Field<"skip_", size_t>>;

  /// The operation needed to sniff a table in the database.
  using SniffTableOp =
      fct::NamedTuple<fct::Field<"type_", fct::Literal<"Database.sniff_table">>,
                      fct::Field<"conn_id_", std::string>,
                      fct::Field<"name_", std::string>>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", CopyTableOp, DescribeConnectionOp, DropTableOp,
                       ExecuteOp, GetOp, GetColnamesOp, GetContentOp,
                       GetNRowsOp, ListConnectionsOp, ListTablesOp, NewDBOp,
                       ReadCSVOp, RefreshOp, SniffCSVOp, SniffTableOp>;

  static DatabaseCommand from_json(const Poco::JSON::Object& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATABASECOMMAND_HPP_

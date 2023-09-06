// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATABASECOMMAND_HPP_
#define COMMANDS_DATABASECOMMAND_HPP_

#include <string>

#include "commands/Int.hpp"
#include "database/Command.hpp"
#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/Literal.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"
#include "rfl/TaggedUnion.hpp"

namespace commands {

/// Any command to be handled by the DatabaseHandler.
struct DatabaseCommand {
  /// The operation needed to copy a table.
  using CopyTableOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.copy_table">>,
                      rfl::Field<"source_conn_id_", std::string>,
                      rfl::Field<"source_table_", std::string>,
                      rfl::Field<"target_conn_id_", std::string>,
                      rfl::Field<"target_table_", std::string>>;

  /// The operation needed to describe a connection.
  using DescribeConnectionOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Database.describe_connection">>,
      rfl::Field<"name_", std::string>>;

  /// The operation needed to drop a table.
  using DropTableOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.drop_table">>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"name_", std::string>>;

  /// The operation needed to execute a query.
  using ExecuteOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.execute">>,
                      rfl::Field<"name_", std::string>>;

  /// The operation needed to get a table.
  using GetOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.get">>,
                      rfl::Field<"name_", std::string>>;

  /// The operation needed to get colnames.
  using GetColnamesOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Database.get_colnames">>,
      rfl::Field<"conn_id_", std::string>, rfl::Field<"name_", std::string>,
      rfl::Field<"query_", std::optional<std::string>>>;

  /// The operation needed to get the content of a table.
  using GetContentOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.get_content">>,
                      rfl::Field<"name_", std::string>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"draw_", Int>, rfl::Field<"length_", Int>,
                      rfl::Field<"start_", Int>>;

  /// The operation needed to the number of rows.
  using GetNRowsOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.get_nrows">>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"name_", std::string>>;

  /// The operation needed to list the connections.
  using ListConnectionsOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Database.list_connections">>>;

  /// The operation needed to list the tables.
  using ListTablesOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.list_tables">>,
                      rfl::Field<"name_", std::string>>;

  /// The operation needed to create a new connection.
  using NewDBOp = typename database::Command::NamedTupleType;

  /// The operation needed to read data from a CSV file.
  using ReadCSVOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Database.read_csv">>,
      rfl::Field<"colnames_", std::optional<std::vector<std::string>>>,
      rfl::Field<"conn_id_", std::string>,
      rfl::Field<"fnames_", std::vector<std::string>>,
      rfl::Field<"name_", std::string>, rfl::Field<"num_lines_read_", size_t>,
      rfl::Field<"quotechar_", std::string>, rfl::Field<"sep_", std::string>,
      rfl::Field<"skip_", size_t>>;

  /// The operation needed to refresh the contents of the database.
  using RefreshOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.refresh">>>;

  /// The operation needed to sniff the data in a CSV file.
  using SniffCSVOp = rfl::NamedTuple<
      rfl::Field<"type_", rfl::Literal<"Database.sniff_csv">>,
      rfl::Field<"colnames_", std::optional<std::vector<std::string>>>,
      rfl::Field<"conn_id_", std::string>,
      rfl::Field<"dialect_", std::optional<rfl::Literal<"python">>>,
      rfl::Field<"fnames_", std::vector<std::string>>,
      rfl::Field<"name_", std::string>,
      rfl::Field<"num_lines_sniffed_", size_t>,
      rfl::Field<"quotechar_", std::string>, rfl::Field<"sep_", std::string>,
      rfl::Field<"skip_", size_t>>;

  /// The operation needed to sniff a table in the database.
  using SniffTableOp =
      rfl::NamedTuple<rfl::Field<"type_", rfl::Literal<"Database.sniff_table">>,
                      rfl::Field<"conn_id_", std::string>,
                      rfl::Field<"name_", std::string>>;

  using NamedTupleType =
      rfl::TaggedUnion<"type_", CopyTableOp, DescribeConnectionOp, DropTableOp,
                       ExecuteOp, GetOp, GetColnamesOp, GetContentOp,
                       GetNRowsOp, ListConnectionsOp, ListTablesOp, NewDBOp,
                       ReadCSVOp, RefreshOp, SniffCSVOp, SniffTableOp>;

  using InputVarType = typename json::Reader::InputVarType;

  static DatabaseCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATABASECOMMAND_HPP_

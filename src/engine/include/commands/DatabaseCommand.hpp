// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATABASECOMMAND_HPP_
#define COMMANDS_DATABASECOMMAND_HPP_

#include "commands/Int.hpp"
#include "database/Command.hpp"

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <rfl/TaggedUnion.hpp>
#include <rfl/json/Reader.hpp>

#include <string>

namespace commands {

/// Any command to be handled by the DatabaseHandler.
struct DatabaseCommand {
  /// The operation needed to copy a table.
  struct CopyTableOp {
    using Tag = rfl::Literal<"Database.copy_table">;
    rfl::Field<"source_conn_id_", std::string> source_conn_id;
    rfl::Field<"source_table_", std::string> source_table;
    rfl::Field<"target_conn_id_", std::string> target_conn_id;
    rfl::Field<"target_table_", std::string> target_table;
  };

  /// The operation needed to describe a connection.
  struct DescribeConnectionOp {
    using Tag = rfl::Literal<"Database.describe_connection">;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to drop a table.
  struct DropTableOp {
    using Tag = rfl::Literal<"Database.drop_table">;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to execute a query.
  struct ExecuteOp {
    using Tag = rfl::Literal<"Database.execute">;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to get a table.
  struct GetOp {
    using Tag = rfl::Literal<"Database.get">;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to get colnames.
  struct GetColnamesOp {
    using Tag = rfl::Literal<"Database.get_colnames">;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"query_", std::optional<std::string>> query;
  };

  /// The operation needed to get the content of a table.
  struct GetContentOp {
    using Tag = rfl::Literal<"Database.get_content">;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"draw_", Int> draw;
    rfl::Field<"length_", Int> length;
    rfl::Field<"start_", Int> start;
  };

  /// The operation needed to the number of rows.
  struct GetNRowsOp {
    using Tag = rfl::Literal<"Database.get_nrows">;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to list the connections.
  struct ListConnectionsOp {
    using Tag = rfl::Literal<"Database.list_connections">;
    rfl::Field<"dummy_", std::optional<int>> dummy;
  };

  /// The operation needed to list the tables.
  struct ListTablesOp {
    using Tag = rfl::Literal<"Database.list_tables">;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to read data from a CSV file.
  struct ReadCSVOp {
    using Tag = rfl::Literal<"Database.read_csv">;
    rfl::Field<"colnames_", std::optional<std::vector<std::string>>> colnames;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"fnames_", std::vector<std::string>> fnames;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"num_lines_read_", size_t> num_lines_read;
    rfl::Field<"quotechar_", std::string> quotechar;
    rfl::Field<"sep_", std::string> sep;
    rfl::Field<"skip_", size_t> skip;
  };

  /// The operation needed to refresh the contents of the database.
  struct RefreshOp {
    using Tag = rfl::Literal<"Database.refresh">;
    rfl::Field<"dummy_", std::optional<int>> dummy;
  };

  /// The operation needed to sniff the data in a CSV file.
  struct SniffCSVOp {
    using Tag = rfl::Literal<"Database.sniff_csv">;
    rfl::Field<"colnames_", std::optional<std::vector<std::string>>> colnames;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"dialect_", std::optional<rfl::Literal<"python">>> dialect;
    rfl::Field<"fnames_", std::vector<std::string>> fnames;
    rfl::Field<"name_", std::string> name;
    rfl::Field<"num_lines_sniffed_", size_t> num_lines_sniffed;
    rfl::Field<"quotechar_", std::string> quotechar;
    rfl::Field<"sep_", std::string> sep;
    rfl::Field<"skip_", size_t> skip;
  };

  /// The operation needed to sniff a query in the database.
  struct SniffQueryOp {
    using Tag = rfl::Literal<"Database.sniff_query">;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"name_", std::string> name;
  };

  /// The operation needed to sniff a table in the database.
  struct SniffTableOp {
    using Tag = rfl::Literal<"Database.sniff_table">;
    rfl::Field<"conn_id_", std::string> conn_id;
    rfl::Field<"name_", std::string> name;
  };

  using TaggedUnionType =
      rfl::TaggedUnion<"type_", CopyTableOp, DescribeConnectionOp, DropTableOp,
                       ExecuteOp, GetOp, GetColnamesOp, GetContentOp,
                       GetNRowsOp, ListConnectionsOp, ListTablesOp, ReadCSVOp,
                       RefreshOp, SniffCSVOp, SniffQueryOp, SniffTableOp>;

  using NewDBOp = typename database::Command::ReflectionType;

  using ReflectionType = std::variant<NewDBOp, TaggedUnionType>;

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static DatabaseCommand from_json_obj(const InputVarType& _obj);

  /// The underlying value
  ReflectionType val_;
};

}  // namespace commands

#endif  // COMMANDS_DATABASECOMMAND_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_ADDDFCOMMAND_HPP_
#define COMMANDS_ADDDFCOMMAND_HPP_

#include <Poco/JSON/Object.h>

#include <optional>
#include <string>
#include <vector>

#include "commands/DataFrameOrView.hpp"
#include "commands/Int.hpp"
#include "commands/Pipeline.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "fct/TaggedUnion.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Schema.hpp"

namespace commands {

/// Any command to be handled by the ProjectManager for adding a new data frame.
struct AddDfCommand {
  /// The command to add a data frame from arrow.
  using AddDfFromArrowOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_arrow">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>>;

  /// The command to add a data frame from CSV.
  using AddDfFromCSVOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.read_csv">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"colnames_", std::optional<std::vector<std::string>>>,
      fct::Field<"fnames_", std::vector<std::string>>,
      fct::Field<"num_lines_read_", size_t>,
      fct::Field<"quotechar_", std::string>, fct::Field<"sep_", std::string>,
      fct::Field<"skip_", size_t>,
      fct::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from a database.
  using AddDfFromDBOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_db">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"conn_id_", std::string>,
      fct::Field<"table_name_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromJSONOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_json">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"time_formats_", std::vector<std::string>>>;

  /// The command to add a data frame from parquet.
  using AddDfFromParquetOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.read_parquet">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"fname_", std::string>>;

  /// The command to add a data frame from JSON.
  using AddDfFromQueryOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_query">>,
      typename helpers::Schema::NamedTupleType, fct::Field<"append_", bool>,
      fct::Field<"conn_id_", std::string>, fct::Field<"query_", std::string>>;

  /// The command to add a data frame from a View.
  using AddDfFromViewOp = fct::define_named_tuple_t<
      fct::Field<"type_", fct::Literal<"DataFrame.from_view">>,
      fct::Field<"append_", bool>, fct::Field<"name_", std::string>,
      fct::Field<"view_", DataFrameOrView>>;

  using NamedTupleType =
      fct::TaggedUnion<"type_", AddDfFromArrowOp, AddDfFromCSVOp, AddDfFromDBOp,
                       AddDfFromJSONOp, AddDfFromParquetOp, AddDfFromQueryOp,
                       AddDfFromViewOp>;

  static AddDfCommand from_json(const Poco::JSON::Object& _obj);

  /// The underlying value
  NamedTupleType val_;
};

}  // namespace commands

#endif  // COMMANDS_ADDDFCOMMAND_HPP_


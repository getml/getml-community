// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATAMODEL_HPP_
#define COMMANDS_DATAMODEL_HPP_

#include <optional>
#include <vector>

#include "commands/Float.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/define_named_tuple.hpp"
#include "helpers/Placeholder.hpp"

namespace commands {

/// Represents the data model actually
/// sent by the Python API. Issues like memory and horizon are not handled
/// yet, which is these additional fields are contained.
struct DataModel {
  using RelationshipLiteral =
      fct::Literal<"many-to-many", "many-to-one", "one-to-many", "one-to-one",
                   "propositionalization">;

  /// The horizon of the join.
  using f_horizon = fct::Field<"horizon_", std::vector<Float>>;

  /// The tables joined to this data model. Note the recursive definition.
  using f_joined_tables = fct::Field<"joined_tables_", std::vector<DataModel>>;

  /// The memory of the join.
  using f_memory = fct::Field<"memory_", std::vector<Float>>;

  /// The relationship used for the join.
  using f_relationship =
      fct::Field<"relationship_", std::vector<RelationshipLiteral>>;

  /// Needed for the JSON parsing to work.
  using NamedTupleType = fct::define_named_tuple_t<
      fct::remove_fields_t<typename helpers::Placeholder::NamedTupleType,
                           "joined_tables_">,
      f_horizon, f_joined_tables, f_memory, f_relationship>;

  /// The underlying value.
  const NamedTupleType val_;
};

}  // namespace commands
#endif

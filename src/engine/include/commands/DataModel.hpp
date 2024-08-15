// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATAMODEL_HPP_
#define COMMANDS_DATAMODEL_HPP_

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/define_named_tuple.hpp>
#include <rfl/json/Reader.hpp>
#include <vector>

#include "commands/Float.hpp"
#include "commands/Roles.hpp"
#include "helpers/Placeholder.hpp"

namespace commands {

/// Represents the data model actually
/// sent by the Python API. Issues like memory and horizon are not handled
/// yet, which is these additional fields are contained.
struct DataModel {
  using RelationshipLiteral =
      rfl::Literal<"many-to-many", "many-to-one", "one-to-many", "one-to-one",
                   "propositionalization">;

  /// The horizon of the join.
  using f_horizon = rfl::Field<"horizon_", std::vector<Float>>;

  /// The tables joined to this data model. Note the recursive definition.
  using f_joined_tables = rfl::Field<"joined_tables_", std::vector<DataModel>>;

  /// The memory of the join.
  using f_memory = rfl::Field<"memory_", std::vector<Float>>;

  /// The relationship used for the join.
  using f_relationship =
      rfl::Field<"relationship_", std::vector<RelationshipLiteral>>;

  /// The fields used to determine the roles.
  using f_roles = rfl::Field<"roles_", Roles>;

  /// Needed for the JSON parsing to work.
  using ReflectionType = rfl::define_named_tuple_t<
      rfl::remove_fields_t<typename helpers::Placeholder::NeededForTraining,
                           "joined_tables_", "propositionalization_">,
      f_horizon, f_joined_tables, f_memory, f_relationship, f_roles>;

  /// The DataModel requires additional checks after parsing,
  /// which is why we have a default constructor.
  DataModel(const ReflectionType& _val) : val_(_val) {
    check_length<"allow_lagged_targets_">();
    check_length<"join_keys_used_">();
    check_length<"other_join_keys_used_">();
    check_length<"time_stamps_used_">();
    check_length<"other_time_stamps_used_">();
    check_length<"upper_time_stamps_used_">();
    check_length<"horizon_">();
    check_length<"memory_">();
    check_length<"relationship_">();
  }

  using InputVarType = typename rfl::json::Reader::InputVarType;

  static DataModel from_json_obj(const InputVarType& _json_obj);

  ~DataModel() = default;

  /// Helper function to check the length of the inputs.
  template <rfl::internal::StringLiteral _name>
  void check_length() const {
    const size_t expected = val_.get<"joined_tables_">().size();
    const size_t actual = val_.get<_name>().size();

    if (actual != expected) {
      throw std::runtime_error(
          "Length of '" + _name.str() +
          "' does not match length "
          "of 'joined_tables_'. " +
          "Length of 'joined_tables_': " + std::to_string(expected) +
          ", length of '" + _name.str() + "': " + std::to_string(actual) + ".");
    }
  }

  /// The underlying value.
  const ReflectionType val_;
};

}  // namespace commands
#endif

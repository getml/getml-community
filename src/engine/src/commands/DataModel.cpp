// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataModel.hpp"

#include <rfl/json/read.hpp>

namespace commands {

DataModel::DataModel(const ReflectionType& _val) : val_(_val) {
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

DataModel DataModel::from_json_obj(const InputVarType& _json_obj) {
  return DataModel(rfl::json::read<ReflectionType>(_json_obj).value());
}
}  // namespace commands

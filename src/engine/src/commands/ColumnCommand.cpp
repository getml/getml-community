// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/ColumnCommand.hpp"

#include "rfl/json.hpp"

namespace commands {

ColumnCommand ColumnCommand::from_json_obj(const InputVarType& _obj) {
  return ColumnCommand(rfl::json::read<ReflectionType>(_obj).value());
}

}  // namespace commands

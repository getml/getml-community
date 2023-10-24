// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/ProjectCommand.hpp"

#include "json/json.hpp"

namespace commands {

ProjectCommand ProjectCommand::from_json_obj(const InputVarType& _obj) {
  return ProjectCommand(json::from_json<NamedTupleType>(_obj));
}

}  // namespace commands

// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/PipelineCommand.hpp"

#include <rfl/json/read.hpp>

namespace commands {

PipelineCommand PipelineCommand::from_json_obj(const InputVarType& _obj) {
  return PipelineCommand{rfl::json::read<ReflectionType>(_obj).value()};
}

}  // namespace commands

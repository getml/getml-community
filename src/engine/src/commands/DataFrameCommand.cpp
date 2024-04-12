// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataFrameCommand.hpp"

namespace commands {

DataFrameCommand DataFrameCommand::from_json_obj(const InputVarType& _obj) {
  std::cout << "DataFrameCommand1" << std::endl;
  auto cmd = DataFrameCommand(rfl::json::read<ReflectionType>(_obj).value());
  std::cout << "DataFrameCommand1" << std::endl;
  return cmd;
}

}  // namespace commands

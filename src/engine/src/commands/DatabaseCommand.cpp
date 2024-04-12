// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DatabaseCommand.hpp"

namespace commands {

DatabaseCommand DatabaseCommand::from_json_obj(const InputVarType& _obj) {
  std::cout << "DatabaseCommand1" << std::endl;
  auto cmd = DatabaseCommand(rfl::json::read<ReflectionType>(_obj).value());
  std::cout << "DatabaseCommand2" << std::endl;
  return cmd;
}

}  // namespace commands

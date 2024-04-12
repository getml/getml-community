// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataModel.hpp"

namespace commands {

DataModel DataModel::from_json_obj(const InputVarType& _json_obj) {
  std::cout << "datamodel1" << std::endl;
  auto cmd = DataModel(rfl::json::read<ReflectionType>(_json_obj).value());
  std::cout << "datamodel2" << std::endl;
  return cmd;
}

}  // namespace commands

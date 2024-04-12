// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/FloatColumnOrFloatColumnView.hpp"

namespace commands {

FloatColumnOrFloatColumnView FloatColumnOrFloatColumnView::from_json_obj(
    const InputVarType& _obj) {
  std::cout << "FloatColumnOrFloatColumnView1" << std::endl;
  auto cmd = FloatColumnOrFloatColumnView(
      rfl::json::read<ReflectionType>(_obj).value());
  std::cout << "FloatColumnOrFloatColumnView2" << std::endl;
  return cmd;
}

}  // namespace commands

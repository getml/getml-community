// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/StringColumnOrStringColumnView.hpp"

namespace commands {

StringColumnOrStringColumnView StringColumnOrStringColumnView::from_json_obj(
    const InputVarType& _obj) {
  std::cout << "StringColumnOrStringColumnView1" << std::endl;
  auto cmd = StringColumnOrStringColumnView(
      rfl::json::read<ReflectionType>(_obj).value());
  std::cout << "StringColumnOrStringColumnView2" << std::endl;
  return cmd;
}

}  // namespace commands

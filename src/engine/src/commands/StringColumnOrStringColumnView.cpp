// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/StringColumnOrStringColumnView.hpp"

#include <rfl/json/read.hpp>

namespace commands {

StringColumnOrStringColumnView StringColumnOrStringColumnView::from_json_obj(
    const InputVarType& _obj) {
  return StringColumnOrStringColumnView{
      rfl::json::read<ReflectionType>(_obj).value()};
}

}  // namespace commands

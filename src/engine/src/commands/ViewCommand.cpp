// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/ViewCommand.hpp"

#include "json/json.hpp"

namespace commands {

ViewCommand ViewCommand::from_json_obj(const Poco::Dynamic::Var& _obj) {
  return ViewCommand(json::from_json<NamedTupleType>(_obj));
}

}  // namespace commands

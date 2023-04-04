// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataFrameCommand.hpp"

#include "json/json.hpp"

namespace commands {

DataFrameCommand DataFrameCommand::from_json(const Poco::JSON::Object& _obj) {
  return json::from_json<DataFrameCommand>(_obj);
}

}  // namespace commands

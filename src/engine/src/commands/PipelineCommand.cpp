// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/PipelineCommand.hpp"

#include "json/json.hpp"

namespace commands {

PipelineCommand PipelineCommand::from_json(const Poco::JSON::Object& _obj) {
  return json::from_json<PipelineCommand>(_obj);
}

}  // namespace commands

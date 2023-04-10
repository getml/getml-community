// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataModel.hpp"

#include <Poco/JSON/Parser.h>

#include "json/json.hpp"

namespace commands {

DataModel DataModel::from_json_obj(const Poco::Dynamic::Var& _json_obj) {
  static_assert(json::has_from_json_obj_v<DataModel>, "This should work");
  return DataModel(json::from_json<NamedTupleType>(_json_obj));
}

}  // namespace commands

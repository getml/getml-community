// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef COMMANDS_DATA_FRAME_FROM_JSON_HPP_
#define COMMANDS_DATA_FRAME_FROM_JSON_HPP_

#include <map>
#include <string>
#include <variant>
#include <vector>

#include "commands/Float.hpp"

namespace commands {

using DataFrameFromJSON =
    std::map<std::string,
             std::variant<std::vector<Float>, std::vector<std::string>>>;

}  // namespace commands

#endif  // COMMANDS_DATA_FRAME_FROM_JSON_HPP_

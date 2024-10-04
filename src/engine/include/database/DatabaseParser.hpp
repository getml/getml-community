// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_DATABASEPARSER_HPP_
#define DATABASE_DATABASEPARSER_HPP_

#include <memory>
#include <string>

#include "database/Command.hpp"
#include "database/Connector.hpp"
#include "io/io.hpp"

namespace database {

struct DatabaseParser {
  /// Given a command, the DatabaseParser returns the correct
  /// database connector.
  static rfl::Ref<Connector> parse(const typename Command::ReflectionType& _cmd,
                                   const std::string& _password);
};

}  // namespace database

#endif  // DATABASE_DATABASEPARSER_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_DATABASESNIFFER_HPP_
#define DATABASE_DATABASESNIFFER_HPP_

#include <memory>
#include <string>

#include "database/Connector.hpp"

namespace database {

struct DatabaseSniffer {
  /// Returns the datatype associate
  static std::string sniff(const fct::Ref<const Connector>& _conn,
                           const std::string& _dialect,
                           const std::string& _source_table_name,
                           const std::string& _target_table_name);
};

}  // namespace database

#endif  // DATABASE_DATABASESNIFFER_HPP_

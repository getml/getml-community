// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_SNIFF_HPP_
#define DATABASE_SNIFF_HPP_

#include <memory>
#include <string>

#include "database/Connector.hpp"

namespace database::sniff {

/// Returns metainformation related to the query.
std::string query(const rfl::Ref<const Connector>& _conn,
                  const std::string& _dialect, const std::string& _query,
                  const std::string& _target_table_name);

/// Returns metainformation related to the table.
std::string table(const rfl::Ref<const Connector>& _conn,
                  const std::string& _dialect,
                  const std::string& _source_table_name,
                  const std::string& _target_table_name);

};  // namespace database::sniff

#endif  // DATABASE_DATABASESNIFFER_HPP_

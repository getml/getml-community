// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/sniff.hpp"

namespace database::sniff {

std::string query(const rfl::Ref<const Connector>& _conn,
                  const std::string& _dialect, const std::string& _query,
                  const std::string& _target_table_name) {
  const auto colnames = _conn->get_colnames_from_query(_query);

  const auto coltypes = _conn->get_coltypes_from_query(_query, colnames);

  assert_true(colnames.size() == coltypes.size());

  return io::StatementMaker::make_statement(_target_table_name, _dialect,
                                            colnames, coltypes);
}

// ---------------------------------------------------------------------------

std::string table(const rfl::Ref<const Connector>& _conn,
                  const std::string& _dialect,
                  const std::string& _source_table_name,
                  const std::string& _target_table_name) {
  const auto colnames = _conn->get_colnames_from_table(_source_table_name);

  const auto coltypes =
      _conn->get_coltypes_from_table(_source_table_name, colnames);

  assert_true(colnames.size() == coltypes.size());

  return io::StatementMaker::make_statement(_target_table_name, _dialect,
                                            colnames, coltypes);
}

}  // namespace database::sniff

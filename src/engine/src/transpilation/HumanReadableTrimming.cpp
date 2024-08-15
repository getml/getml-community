// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "transpilation/HumanReadableTrimming.hpp"

#include <sstream>

// ----------------------------------------------------------------------------

namespace transpilation {

// ----------------------------------------------------------------------------

std::string HumanReadableTrimming::join(const std::string& _staging_table,
                                        const std::string& _colname,
                                        const std::string& _replacement) const {
  const auto trimming_table = make_trimming_table(_staging_table, _colname);

  const auto q1 = sql_dialect_generator_->quotechar1();

  const auto q2 = sql_dialect_generator_->quotechar2();

  const auto replace_table =
      SQLGenerator::to_upper(_staging_table + "__REPLACE_" + _colname);

  std::stringstream stream;

  stream << sql_dialect_generator_->drop_table_if_exists(replace_table)
         << "CREATE TABLE " << q1 << replace_table << q2 << " AS" << std::endl
         << "SELECT t1." << q1 << _colname << q2 << " AS " << q1 << "key" << q2
         << std::endl
         << "MAX( CASE WHEN t2.key IS NULL THEN '" << _replacement
         << "' ELSE t2.key END ) AS " << q1 << "value" << q2 << std::endl
         << "FROM " << q1 << _staging_table << q2 << " t1" << std::endl
         << "LEFT JOIN " << q1 << trimming_table << q2 << " t2" << std::endl
         << "ON t1." << q1 << _colname << q2 << " = t2." << q1 << "key" << q2
         << std::endl
         << "GROUP BY t1." << q1 << _colname << q2 << ";" << std::endl
         << std::endl;

  stream << "UPDATE " << q1 << _staging_table << q2 << std::endl
         << "SET " << q1 << _staging_table << q2 << " = t2." << q1 << "value"
         << q2 << std::endl
         << "FROM " << q1 << replace_table << q2 << " AS t2" << std::endl
         << "WHERE " << q1 << _staging_table << q2 << "." << q1 << _colname
         << q2 << " = t2." << q1 << "key" << q2 << ";" << std::endl
         << std::endl;

  stream << sql_dialect_generator_->drop_table_if_exists(replace_table)
         << sql_dialect_generator_->drop_table_if_exists(trimming_table)
         << std::endl
         << std::endl;

  return stream.str();
}

// ----------------------------------------------------------------------------

std::string HumanReadableTrimming::make_header(
    const std::string& _staging_table, const std::string& _colname) const {
  const auto trimming_table = make_trimming_table(_staging_table, _colname);

  const auto q1 = sql_dialect_generator_->quotechar1();

  const auto q2 = sql_dialect_generator_->quotechar2();

  std::stringstream sql;

  sql << sql_dialect_generator_->drop_table_if_exists(trimming_table);

  sql << "CREATE TABLE " << q1 << trimming_table << q2
      << "(key TEXT NOT NULL PRIMARY KEY);" << std::endl
      << std::endl;

  return sql.str();
}

// ----------------------------------------------------------------------------

std::string HumanReadableTrimming::make_insert_into(
    const std::string& _staging_table, const std::string& _colname) const {
  const auto trimming_table = make_trimming_table(_staging_table, _colname);

  const auto q1 = sql_dialect_generator_->quotechar1();

  const auto q2 = sql_dialect_generator_->quotechar2();

  std::stringstream sql;

  sql << "INSERT INTO " << q1 << trimming_table << q2 << " (key)" << std::endl
      << "VALUES";

  return sql.str();
}

// ----------------------------------------------------------------------------
}  // namespace transpilation

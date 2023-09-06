// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "io/StatementMaker.hpp"

#include "json/json.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"

namespace io {

size_t StatementMaker::find_max_size(
    const std::vector<std::string>& _colnames) {
  if (_colnames.size() == 0) {
    return 0;
  } else {
    const auto comp = [](const std::string& str1, const std::string& str2) {
      return (str1.size() < str2.size());
    };

    const auto it = std::max_element(_colnames.begin(), _colnames.end(), comp);

    return it->size();
  }
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement(
    const std::string& _table_name, const std::string& _dialect,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  if (_dialect == MYSQL) {
    return make_statement_mysql(_table_name, _colnames, _datatypes);
  }

  if (_dialect == POSTGRES) {
    return make_statement_postgres(_table_name, _colnames, _datatypes);
  }

  if (_dialect == PYTHON) {
    return make_statement_python(_colnames, _datatypes);
  }

  if (_dialect == SQLITE3) {
    return make_statement_sqlite(_table_name, _colnames, _datatypes);
  }

  throw std::runtime_error("SQL dialect '" + _dialect + "' not known!");

  return "";
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_mysql(
    const std::string& _table_name, const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  assert_true(_colnames.size() == _datatypes.size());

  const auto max_size = find_max_size(_colnames);

  std::stringstream statement;

  statement << "DROP TABLE IF EXISTS `" << _table_name << "`;" << std::endl
            << std::endl;

  statement << "CREATE TABLE `" << _table_name << "`(" << std::endl;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    statement << "    `" << _colnames[i] << "` "
              << make_gap(_colnames[i], max_size)
              << to_string_mysql(_datatypes[i]);

    if (i < _colnames.size() - 1) {
      statement << "," << std::endl;
    } else {
      statement << ");" << std::endl;
    }
  }

  return statement.str();
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_postgres(
    const std::string& _table_name, const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  assert_true(_colnames.size() == _datatypes.size());

  const auto max_size = find_max_size(_colnames);

  const auto table_name = handle_schema(_table_name, "\"", "\"");

  std::stringstream statement;

  statement << "DROP TABLE IF EXISTS \"" << table_name << "\";" << std::endl
            << std::endl;

  statement << "CREATE TABLE \"" << table_name << "\"(" << std::endl;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    statement << "    \"" << _colnames[i] << "\" "
              << make_gap(_colnames[i], max_size)
              << to_string_postgres(_datatypes[i]);

    if (i < _colnames.size() - 1) {
      statement << "," << std::endl;
    } else {
      statement << ");" << std::endl;
    }
  }

  return statement.str();
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_python(
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  assert_true(_colnames.size() == _datatypes.size());

  std::vector<std::string> unused_floats;
  std::vector<std::string> unused_strings;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    switch (_datatypes[i]) {
      case Datatype::double_precision:
      case Datatype::integer:
        unused_floats.push_back(_colnames[i]);
        break;

      default:
        unused_strings.push_back(_colnames[i]);
        break;
    }
  }

  const auto obj = rfl::make_field<"unused_float">(unused_floats) *
                   rfl::make_field<"unused_string">(unused_strings);

  return json::to_json(obj);
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_sqlite(
    const std::string& _table_name, const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  assert_true(_colnames.size() == _datatypes.size());

  const auto max_size = find_max_size(_colnames);

  std::stringstream statement;

  statement << "DROP TABLE IF EXISTS \"" << _table_name << "\";" << std::endl
            << std::endl;

  statement << "CREATE TABLE \"" << _table_name << "\"(" << std::endl;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    statement << "    \"" << _colnames.at(i) << "\" "
              << make_gap(_colnames.at(i), max_size)
              << to_string_sqlite(_datatypes.at(i));

    if (i < _colnames.size() - 1) {
      statement << "," << std::endl;
    } else {
      statement << ");" << std::endl;
    }
  }

  return statement.str();
}

// ----------------------------------------------------------------------------

std::string StatementMaker::to_string_mysql(const Datatype _type) {
  switch (_type) {
    case Datatype::double_precision:
      return "DOUBLE";

    case Datatype::integer:
      return "INT";

    case Datatype::time_stamp:
      return "TIMESTAMP";

    case Datatype::string:
      return "TEXT";

    case Datatype::unknown:
      assert_true(false);
      return "";
  }

  assert_true(false);
  return "";
}

// ----------------------------------------------------------------------------

std::string StatementMaker::to_string_postgres(const Datatype _type) {
  switch (_type) {
    case Datatype::double_precision:
      return "DOUBLE PRECISION";

    case Datatype::integer:
      return "INTEGER";

    case Datatype::time_stamp:
      return "TIMESTAMP";

    case Datatype::string:
      return "TEXT";

    case Datatype::unknown:
      assert_true(false);
      return "";
  }

  assert_true(false);
  return "";
}

// ----------------------------------------------------------------------------

std::string StatementMaker::to_string_sqlite(const Datatype _type) {
  switch (_type) {
    case Datatype::double_precision:
      return "REAL";

    case Datatype::integer:
      return "INTEGER";

    case Datatype::time_stamp:
    case Datatype::string:
      return "TEXT";

    case Datatype::unknown:
      assert_true(false);
      return "";
  }

  assert_true(false);
  return "";
}

// ----------------------------------------------------------------------------
}  // namespace io

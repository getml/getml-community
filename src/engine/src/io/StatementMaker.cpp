#include "io/StatementMaker.hpp"

#include "jsonutils/jsonutils.hpp"

namespace io {
// ----------------------------------------------------------------------------

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
    const Poco::JSON::Object& _description,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  if (_dialect == MYSQL) {
    return make_statement_mysql(_table_name, _colnames, _datatypes);
  }

  if (_dialect == ODBC) {
    return make_statement_odbc(_table_name, _colnames, _datatypes,
                               _description);
  }

  if (_dialect == POSTGRES) {
    return make_statement_postgres(_table_name, _colnames, _datatypes);
  }

  if (_dialect == PYTHON) {
    return make_statement_python(_colnames, _datatypes);
  }

  if (_dialect == SAP_HANA) {
    const auto default_schema =
        jsonutils::JSON::get_value<std::string>(_description, "default_schema");

    return make_statement_sap_hana(_table_name, default_schema, _colnames,
                                   _datatypes);
  }

  if (_dialect == SQLITE3) {
    return make_statement_sqlite(_table_name, _colnames, _datatypes);
  }

  throw std::invalid_argument("SQL dialect '" + _dialect + "' not known!");

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

std::string StatementMaker::make_statement_odbc(
    const std::string& _table_name, const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes,
    const Poco::JSON::Object& _description) {
  assert_true(_colnames.size() == _datatypes.size());

  const auto double_precision =
      jsonutils::JSON::get_value<std::string>(_description, "double_precision");

  const auto escape_chars =
      jsonutils::JSON::get_value<std::string>(_description, "escape_chars");

  const auto integer =
      jsonutils::JSON::get_value<std::string>(_description, "integer");

  const auto text =
      jsonutils::JSON::get_value<std::string>(_description, "text");

  assert_true(escape_chars.size() == 2);

  const auto escape_char1 = escape_chars[0];
  const auto escape_char2 = escape_chars[1];

  const auto max_size = find_max_size(_colnames);

  std::stringstream statement;

  statement << "CREATE TABLE ";

  if (escape_char1 != ' ') {
    statement << escape_char1;
  }

  statement << _table_name;

  if (escape_char2 != ' ') {
    statement << escape_char2;
  }

  statement << "(" << std::endl;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    statement << "    ";

    if (escape_char1 != ' ') {
      statement << escape_char1;
    }

    statement << _colnames[i];

    if (escape_char2 != ' ') {
      statement << escape_char2;
    }

    statement << " " << make_gap(_colnames[i], max_size)
              << to_string_odbc(_datatypes[i], double_precision, integer, text);

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

  Poco::JSON::Array unused_floats;
  Poco::JSON::Array unused_strings;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    switch (_datatypes[i]) {
      case Datatype::double_precision:
      case Datatype::integer:
        unused_floats.add(_colnames[i]);
        break;

      default:
        unused_strings.add(_colnames[i]);
        break;
    }
  }

  Poco::JSON::Object obj;

  obj.set("unused_float", unused_floats);
  obj.set("unused_string", unused_strings);

  return jsonutils::JSON::stringify(obj);
}

// ----------------------------------------------------------------------------

std::string StatementMaker::make_statement_sap_hana(
    const std::string& _table_name, const std::string& _schema_name,
    const std::vector<std::string>& _colnames,
    const std::vector<Datatype>& _datatypes) {
  assert_true(_colnames.size() == _datatypes.size());

  const auto max_size = find_max_size(_colnames);

  std::stringstream statement;

  statement << "CREATE OR REPLACE PROCEDURE adhocdroptableifexists "
            << "LANGUAGE SQLSCRIPT AS myrowid INTEGER;" << std::endl
            << "BEGIN" << std::endl
            << "myrowid := 0;" << std::endl
            << "SELECT COUNT(*) INTO myrowid FROM PUBLIC.M_TABLES "
            << "WHERE SCHEMA_NAME = '" << _schema_name << "' AND TABLE_NAME = '"
            << _table_name << "';" << std::endl
            << "IF ( :myrowid > 0 ) THEN" << std::endl
            << "EXEC 'DROP TABLE " << _schema_name << "." << _table_name << "';"
            << std::endl
            << "END IF;" << std::endl
            << "END;" << std::endl
            << std::endl
            << "CALL \"ADHOCDROPTABLEIFEXISTS\"();" << std::endl
            << "DROP PROCEDURE ADHOCDROPTABLEIFEXISTS;" << std::endl
            << std::endl;

  statement << "CREATE TABLE \"" << _table_name << "\"(" << std::endl;

  for (size_t i = 0; i < _colnames.size(); ++i) {
    statement << "    \"" << _colnames.at(i) << "\" "
              << make_gap(_colnames.at(i), max_size)
              << to_string_sap_hana(_datatypes.at(i));

    if (i < _colnames.size() - 1) {
      statement << "," << std::endl;
    } else {
      statement << ");" << std::endl;
    }
  }

  return statement.str();
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

std::string StatementMaker::to_string_odbc(const Datatype _type,
                                           const std::string& _double_precision,
                                           const std::string& _integer,
                                           const std::string& _text) {
  switch (_type) {
    case Datatype::double_precision:
      return _double_precision;

    case Datatype::integer:
      return _integer;

    case Datatype::time_stamp:
    case Datatype::string:
      return _text;

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

std::string StatementMaker::to_string_sap_hana(const Datatype _type) {
  switch (_type) {
    case Datatype::double_precision:
      return "REAL";

    case Datatype::integer:
      return "INTEGER";

    case Datatype::time_stamp:
    case Datatype::string:
      return "NVARCHAR(5000)";

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

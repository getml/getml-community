#ifndef IO_STATEMENTMAKER_HPP_
#define IO_STATEMENTMAKER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "io/Datatype.hpp"

// ----------------------------------------------------------------------------

namespace io {
// ----------------------------------------------------------------------------

class StatementMaker {
 public:
  static constexpr const char* MYSQL = "mysql";
  static constexpr const char* ODBC = "odbc";
  static constexpr const char* POSTGRES = "postgres";
  static constexpr const char* PYTHON = "python";
  static constexpr const char* SAP_HANA = "sap_hana";
  static constexpr const char* SQLITE3 = "sqlite3";

 public:
  /// Produces the CREATE TABLE statement.
  static std::string make_statement(const std::string& _table_name,
                                    const std::string& _dialect,
                                    const Poco::JSON::Object& _description,
                                    const std::vector<std::string>& _colnames,
                                    const std::vector<Datatype>& _datatypes);

 private:
  /// Finds the maximum size of the colnames.
  static size_t find_max_size(const std::vector<std::string>& _colnames);

  /// Produces the CREATE TABLE statement for MySQL.
  static std::string make_statement_mysql(
      const std::string& _table_name, const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes);

  /// Produces the CREATE TABLE statement for ODBC.
  static std::string make_statement_odbc(
      const std::string& _table_name, const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes,
      const Poco::JSON::Object& _description);

  /// Produces the CREATE TABLE statement for postgres.
  static std::string make_statement_postgres(
      const std::string& _table_name, const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes);

  /// Produces the kwargs for an engine.DataFrame.
  static std::string make_statement_python(
      const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes);

  /// Produces the CREATE TABLE statement for SAP HANA.
  static std::string make_statement_sap_hana(
      const std::string& _table_name, const std::string& _schema_name,
      const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes);

  /// Produces the CREATE TABLE statement for sqlite.
  static std::string make_statement_sqlite(
      const std::string& _table_name, const std::vector<std::string>& _colnames,
      const std::vector<Datatype>& _datatypes);

  /// Transforms a datatype to the string required for the mysql dialect.
  static std::string to_string_mysql(const Datatype _type);

  /// Transforms a datatype to the string required for odbc.
  static std::string to_string_odbc(const Datatype _type,
                                    const std::string& _double_precision,
                                    const std::string& _integer,
                                    const std::string& _text);

  /// Transforms a datatype to the string required for the postgres dialect.
  static std::string to_string_postgres(const Datatype _type);

  /// Transforms a datatype to the string required for the sqlite dialect.
  static std::string to_string_sap_hana(const Datatype _type);

  /// Transforms a datatype to the string required for the sqlite dialect.
  static std::string to_string_sqlite(const Datatype _type);

  // -------------------------------

 private:
  /// Produces a gap to ensure that all types are aligned.
  static std::string make_gap(const std::string& _colname,
                              const size_t _max_size) {
    assert_true(_colname.size() <= _max_size);
    return std::string(_max_size - _colname.size(), ' ');
  }

  // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_STATEMENTMAKER_HPP_

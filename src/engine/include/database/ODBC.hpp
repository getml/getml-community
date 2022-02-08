#ifndef DATABASE_ODBC_HPP_
#define DATABASE_ODBC_HPP_

// ----------------------------------------------------------------------------

// For some weird reason, there are
// some conflicts between sql.h and
// arrow. These conflicts can be resolved
// by including arrow/api.h before sql.h.
#include <arrow/api.h>

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <sql.h>
#include <sqlext.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "jsonutils/jsonutils.hpp"

// ----------------------------------------------------------------------------

#include "database/Connector.hpp"
#include "database/DatabaseParser.hpp"
#include "database/Getter.hpp"
#include "database/ODBCConn.hpp"
#include "database/ODBCEnv.hpp"
#include "database/ODBCIterator.hpp"
#include "database/ODBCStmt.hpp"

// ----------------------------------------------------------------------------

namespace database {

class ODBC : public Connector {
 private:
  static constexpr bool NO_AUTOCOMMIT = true;

 public:
  ODBC(const Poco::JSON::Object& _obj, const std::string& _passwd,
       const std::vector<std::string>& _time_formats)
      : double_precision_(
            jsonutils::JSON::get_value<std::string>(_obj, "double_precision_")),
        env_(std::make_shared<ODBCEnv>()),
        integer_(jsonutils::JSON::get_value<std::string>(_obj, "integer_")),
        passwd_(_passwd),
        server_name_(
            jsonutils::JSON::get_value<std::string>(_obj, "server_name_")),
        text_(jsonutils::JSON::get_value<std::string>(_obj, "text_")),
        time_formats_(_time_formats),
        user_(jsonutils::JSON::get_value<std::string>(_obj, "user_")) {
    std::tie(escape_char1_, escape_char2_) = extract_escape_chars(_obj);
  }

  ~ODBC() = default;

  // -------------------------------

 public:
  /// Returns a Poco::JSON::Object describing the connection.
  Poco::JSON::Object describe() const final;

  /// Drops a table from the data base.
  void drop_table(const std::string& _tname) final;

  /// Executes an SQL query.
  void execute(const std::string& _query) final;

  /// Returns the descriptions of the columns.
  std::vector<
      std::tuple<SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLSMALLINT>>
  get_coldescriptions(const std::string& _table) const;

  /// Returns the names of the table columns.
  std::vector<std::string> get_colnames(const std::string& _table) const final;

  /// Returns the types of the table columns.
  std::vector<io::Datatype> get_coltypes(
      const std::string& _table,
      const std::vector<std::string>& _colnames) const final;

  /// Returns the content of a table in a format that is compatible
  /// with the DataTables.js server-side processing API.
  Poco::JSON::Object get_content(const std::string& _tname,
                                 const std::int32_t _draw,
                                 const std::int32_t _start,
                                 const std::int32_t _length) final;

  /// Lists the name of the tables held in the database.
  std::vector<std::string> list_tables() final;

  /// Reads a CSV file or another data source into a table.
  void read(const std::string& _table, const size_t _skip,
            io::Reader* _reader) final;

  // -------------------------------

 public:
  /// Returns the dialect of the connector.
  std::string dialect() const final { return DatabaseParser::ODBC_DIALECT; }

  /// Returns the number of rows in the table signified by _tname.
  std::int32_t get_nrows(const std::string& _tname) final {
    const auto tname = handle_schema(_tname);
    return select({"COUNT(*)"}, tname, "")->get_int();
  }

  /// Returns a shared_ptr containing a MySQLIterator.
  std::shared_ptr<Iterator> select(const std::vector<std::string>& _colnames,
                                   const std::string& _tname,
                                   const std::string& _where) final {
    return std::make_shared<ODBCIterator>(make_connection(), _colnames,
                                          time_formats_, _tname, _where,
                                          escape_char1_, escape_char2_);
  }

  /// Returns a shared_ptr containing an ORBCIterator.
  std::shared_ptr<Iterator> select(const std::string& _query) final {
    return std::make_shared<ODBCIterator>(make_connection(), _query,
                                          time_formats_);
  }

  /// Returns the time formats used.
  const std::vector<std::string>& time_formats() const { return time_formats_; }

  // -------------------------------

 private:
  /// Makes sure that the colnames of the CSV file match the colnames of the
  /// target table.
  void check_colnames(const std::vector<std::string>& _colnames,
                      io::Reader* _reader);

  /// Gets the next line from the reader.
  std::optional<std::vector<std::string>> get_next_line(
      const std::vector<io::Datatype>& _coltypes, const size_t _line_count,
      io::Reader* _reader) const;

  /// Generate an iterator that is limited, trying different SQL statements.
  std::optional<ODBCIterator> make_limited_iterator(const std::string& _table,
                                                    const size_t _begin,
                                                    const size_t _end) const;

  /// Make a simple limited SELECT query following the SQL standard.
  std::string simple_limit_standard(const std::string& _table,
                                    const size_t _end) const;

  /// Make a simple limited SELECT query using the LIMIT syntax supported by
  /// most databases, included IBM DB2.
  std::string simple_limit_most(const std::string& _table, const size_t _begin,
                                const size_t _end) const;

  /// Make a simple limited SELECT query using a syntax supported by Oracle.
  std::string simple_limit_oracle(const std::string& _table,
                                  const size_t _begin, const size_t _end) const;

  /// Make a simple limited SELECT query using a syntax supported by MSSQL.
  std::string simple_limit_mssql(const std::string& _table, const size_t _begin,
                                 const size_t _end) const;

  /// Make a simple SELECT.
  std::string simple_select(const std::string& _table) const;

  // -------------------------------

 private:
  /// Extract the escapte characters.
  std::pair<char, char> extract_escape_chars(
      const Poco::JSON::Object& _obj) const;

  /// Returns the catalogs.
  std::vector<std::string> get_catalogs() const;

  /// Returns a list of all schemas, given the catalog
  std::vector<std::string> get_schemas(const std::string& _catalog) const;

  /// Returns a list of all schemas, given the catalog and the schema.
  std::vector<std::string> get_tables(const std::string& _catalog,
                                      const std::string& _schema) const;

  /// Prepares a INSERT INTO .. VALUES ... query
  /// to insert a large CSV file.
  std::string make_bulk_insert_query(
      const std::string& _table,
      const std::vector<std::string>& _colnames) const;

  // -------------------------------

 private:
  /// Trivial accessor
  ODBCEnv& env() {
    assert_true(env_);
    return *env_;
  }

  /// Trivial accessor
  const ODBCEnv& env() const {
    assert_true(env_);
    return *env_;
  }

  /// Handle any schemata contained in _tname.
  std::string handle_schema(const std::string& _tname) const {
    if (escape_char1_ == ' ' || escape_char2_ == ' ') {
      return _tname;
    }
    return io::StatementMaker::handle_schema(
        _tname, std::string(1, escape_char1_), std::string(1, escape_char2_));
  }

  /// Returns a new connection. If the connection is meant for WRITING data,
  /// be sure to explicitly turn of the AUTOCOMMIT.
  std::shared_ptr<ODBCConn> make_connection(
      const bool _no_autocommit = false) const {
    return std::make_shared<ODBCConn>(env(), server_name_, user_, passwd_,
                                      _no_autocommit);
  }

  /// Helper function to turn a string into a ptr that can be passed to ODBC
  std::unique_ptr<SQLCHAR[]> to_ptr(const std::string& _str) const {
    auto ptr = std::make_unique<SQLCHAR[]>(_str.size() + 1);

    std::copy(reinterpret_cast<const SQLCHAR*>(_str.c_str()),
              reinterpret_cast<const SQLCHAR*>(_str.c_str()) + _str.size(),
              ptr.get());

    return ptr;
  }

  // -------------------------------

 private:
  /// The keyword used to mark double precision columns.
  const std::string double_precision_;

  /// The environment handle.
  const std::shared_ptr<ODBCEnv> env_;

  /// The first escape character - used to envelop table, schema, column
  /// names. According to ANSI SQL this should be '"', but we don't rely on
  /// that.
  char escape_char1_;

  /// The second escape character - used to envelop table, schema, column
  /// names. According to ANSI SQL this should be '"', but we don't rely on
  /// that.
  char escape_char2_;

  /// The keyword used to mark integer columns.
  const std::string integer_;

  /// The password used.
  const std::string passwd_;

  /// The server to be connect to.
  const std::string server_name_;

  /// The keyword used to mark text columns.
  const std::string text_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  /// The user name.
  const std::string user_;
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_ODBC_HPP_


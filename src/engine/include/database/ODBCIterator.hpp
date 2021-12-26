#ifndef DATABASE_ODBCITERATOR_HPP_
#define DATABASE_ODBCITERATOR_HPP_

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
#include <vector>

// ----------------------------------------------------------------------------

#include "io/io.hpp"

// ----------------------------------------------------------------------------

#include "database/Float.hpp"
#include "database/Getter.hpp"
#include "database/Int.hpp"
#include "database/Iterator.hpp"
#include "database/ODBCConn.hpp"
#include "database/ODBCEnv.hpp"
#include "database/ODBCStmt.hpp"

// ----------------------------------------------------------------------------

namespace database {
// -----------------------------------------------------------------------------

class ODBCIterator : public Iterator {
  // -------------------------------------------------------------------------

 public:
  ODBCIterator(const std::shared_ptr<ODBCConn>& _connection,
               const std::string& _query,
               const std::vector<std::string>& _time_formats);

  ODBCIterator(const std::shared_ptr<ODBCConn>& _connection,
               const std::vector<std::string>& _colnames,
               const std::vector<std::string>& _time_formats,
               const std::string& _tname, const std::string& _where,
               const char _escape_char1, const char _escape_char2);

  ~ODBCIterator();

  // -------------------------------------------------------------------------

 public:
  /// Returns the column descriptions of the query.
  std::vector<
      std::tuple<SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLSMALLINT>>
  coldescriptions() const;

  /// Returns the column names of the query.
  std::vector<std::string> colnames() const final;

  /// Returns the column types of the query.
  std::vector<io::Datatype> coltypes() const;

  /// Returns a double.
  Float get_double() final;

  /// Returns an int.
  Int get_int() final;

  /// Returns a time stamp.
  Float get_time_stamp() final;

  /// Returns a string.
  std::string get_string() final;

  // -------------------------------------------------------------------------

 public:
  /// Trivial accessor.
  const ODBCConn& connection() const {
    assert_true(connection_);
    return *connection_;
  }

  /// Whether the end is reached.
  bool end() const final { return end_; }

  // -------------------------------------------------------------------------

 private:
  /// Parses a field for the CSV reader.
  io::Datatype interpret_field_type(const SQLSMALLINT _type) const;

  /// Generates an SQL statement from the colnames, the table name and an
  /// optional _where.
  static std::string make_query(const std::vector<std::string>& _colnames,
                                const std::string& _tname,
                                const std::string& _where,
                                const char _escape_char1,
                                const char _escape_char2);

  // -------------------------------------------------------------------------

 private:
  /// Prevents segfaults before getting the next entry.
  void check() {
    if (end()) {
      throw std::invalid_argument("End of query is reached.");
    }
  }

  /// Fetches the next row.
  void fetch() {
    const auto ret = SQLFetch(stmt().handle_);

    if (ret != SQL_SUCCESS && ret != SQL_SUCCESS_WITH_INFO) {
      if (ret == SQL_NO_DATA) {
        end_ = true;
      } else {
        ODBCError::check(ret, "SQLFetch in fetch", stmt().handle_,
                         SQL_HANDLE_STMT);
      }
    }
  }

  /// Returns the raw value.
  std::pair<std::string, bool> get_value() {
    check();

    if (flen_.at(colnum_) == SQL_NULL_DATA) {
      increment();
      return std::pair<std::string, bool>("", true);
    }

    auto val = reinterpret_cast<const char*>(row_.at(colnum_).get());

    auto str = std::string(val);

    increment();

    return std::pair<std::string, bool>(str, false);
  }

  /// Increments the iterator.
  void increment() {
    if (++colnum_ == row_.size()) {
      colnum_ = 0;
      fetch();
    }
  }

  /// Trivial accessor.
  ODBCStmt& stmt() {
    assert_true(stmt_);
    return *stmt_;
  }

  /// Trivial accessor.
  const ODBCStmt& stmt() const {
    assert_true(stmt_);
    return *stmt_;
  }

  // -------------------------------------------------------------------------

 private:
  /// The current colnum.
  size_t colnum_;

  /// The connection used.
  const std::shared_ptr<ODBCConn> connection_;

  /// Bool whether the end is reached.
  bool end_;

  /// The respective length of each field
  std::vector<SQLLEN> flen_;

  /// The current row.
  std::vector<std::unique_ptr<SQLCHAR[]>> row_;

  /// SQL statement handle.
  std::shared_ptr<ODBCStmt> stmt_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_ODBCITERATOR_HPP_

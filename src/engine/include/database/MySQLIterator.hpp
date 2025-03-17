// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_MYSQLITERATOR_HPP_
#define DATABASE_MYSQLITERATOR_HPP_

#include "database/Iterator.hpp"
#include "debug/assert_true.hpp"

#include <mysql.h>

#include <cmath>
#include <memory>
#include <string>
#include <vector>

namespace database {
// -----------------------------------------------------------------------------

class MySQLIterator final : public Iterator {
  // -------------------------------------------------------------------------

 public:
  MySQLIterator(const std::shared_ptr<MYSQL>& _connection,
                const std::string& _sql,
                const std::vector<std::string>& _time_formats);

  MySQLIterator(const std::shared_ptr<MYSQL>& _connection,
                const std::vector<std::string>& _colnames,
                const std::vector<std::string>& _time_formats,
                const std::string& _tname, const std::string& _where);

  ~MySQLIterator() final = default;

  // -------------------------------------------------------------------------

 public:
  /// Returns the column names of the query.
  std::vector<std::string> colnames() const final;

  /// Returns a double.
  Float get_double() final;

  /// Returns an int.
  Int get_int() final;

  /// Returns a time stamp.
  Float get_time_stamp() final;

  /// Returns a string .
  std::string get_string() final;

  // -------------------------------------------------------------------------

 public:
  /// Trivial (private) accessor.
  MYSQL* connection() const {
    assert_true(connection_);
    return connection_.get();
  }

  /// Whether the end is reached.
  bool end() const final { return (row_ == NULL); }

  /// Throws an error.
  void throw_error(MYSQL* _conn) const {
    const std::string msg = "MySQL error (" +
                            std::to_string(mysql_errno(_conn)) + ") [" +
                            mysql_sqlstate(_conn) + "] " + mysql_error(_conn);

    throw std::runtime_error(msg);
  }

  // -------------------------------------------------------------------------

 private:
  /// Executes a command and returns a result set.
  std::shared_ptr<MYSQL_RES> execute(const std::string& _sql) const;

  /// Generates an SQL statement from the colnames, the table name and an
  /// optional _where.
  static std::string make_sql(const std::vector<std::string>& _colnames,
                              const std::string& _tname,
                              const std::string& _where);

  // -------------------------------------------------------------------------

 private:
  /// Prevents segfaults before getting the next entry.
  void check() {
    if (end()) {
      throw std::runtime_error("End of query is reached.");
    }

    if (colnum_ >= num_cols_) {
      throw std::runtime_error("Row number out of bounds.");
    }
  }

  /// Returns the raw value.
  std::pair<std::string, bool> get_value() {
    check();

    char* val = row_[colnum_];

    if (!val) {
      increment();
      return std::pair<std::string, bool>("", true);
    }

    auto str = std::string(val);

    increment();

    return std::pair<std::string, bool>(str, false);
  }

  /// Increments the iterator.
  void increment() {
    if (++colnum_ == num_cols_) {
      colnum_ = 0;

      row_ = mysql_fetch_row(result_.get());
    }
  }

  // -------------------------------------------------------------------------

 private:
  /// The current colnum.
  unsigned int colnum_;

  /// The connection used.
  const std::shared_ptr<MYSQL> connection_;

  /// The total number of columns.
  unsigned int num_cols_;

  /// Result of the query.
  std::shared_ptr<MYSQL_RES> result_;

  /// The current row.
  /// Will be free'd when result_ is free'd:
  /// https://mariadb.com/kb/en/library/mysql_fetch_row/
  MYSQL_ROW row_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  // -------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_MYSQLITERATOR_HPP_

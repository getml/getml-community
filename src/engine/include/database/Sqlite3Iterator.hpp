// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_SQLITE3ITERATOR_HPP_
#define DATABASE_SQLITE3ITERATOR_HPP_

extern "C" {
#include <sqlite3/sqlite3.h>
}

#include <memory>
#include <string>
#include <vector>

#include "database/Float.hpp"
#include "database/Getter.hpp"
#include "database/Int.hpp"
#include "database/Iterator.hpp"
#include "multithreading/multithreading.hpp"

namespace database {

class Sqlite3Iterator : public Iterator {
 public:
  Sqlite3Iterator(
      const std::shared_ptr<sqlite3>& _db, const std::string& _sql,
      const rfl::Ref<multithreading::ReadWriteLock>& _read_write_lock,
      const std::vector<std::string>& _time_formats);

  Sqlite3Iterator(
      const std::shared_ptr<sqlite3>& _db,
      const std::vector<std::string>& _colnames,
      const rfl::Ref<multithreading::ReadWriteLock>& _read_write_lock,
      const std::vector<std::string>& _time_formats, const std::string& _tname,
      const std::string& _where);

  ~Sqlite3Iterator() = default;

  // -------------------------------

 public:
  /// Returns the column names of the query.
  std::vector<std::string> colnames() const final;

  /// Returns a double and increments the iterator.
  Float get_double() final;

  /// Returns a double and increments the iterator.
  Int get_int() final;

  /// Returns a string and increments the iterator.
  std::string get_string() final;

  /// Returns a time stamps and increments the iterator.
  Float get_time_stamp() final;

  // -------------------------------

 public:
  /// Whether the end is reached.
  bool end() const final { return end_; }

  // -------------------------------

 private:
  /// Generates an SQL statement fro the colnames, the table name and an
  /// optional _where.
  static std::string make_sql(const std::vector<std::string>& _colnames,
                              const std::string& _tname,
                              const std::string& _where);

  // -------------------------------

 private:
  /// Trivial (private) accessor
  sqlite3* db() const { return db_.get(); }

  /// Iterates to the next row, if there is one.
  void next_row() { end_ = (sqlite3_step(stmt()) != SQLITE_ROW); }

  /// Trivial (private) accessor
  sqlite3_stmt* stmt() const {
    assert_true(stmt_);
    return stmt_.get();
  }

  // -------------------------------

 private:
  /// The current column.
  int colnum_;

  /// Shared ptr containing the database object.
  const std::shared_ptr<sqlite3> db_;

  /// Whether the end is reached.
  bool end_;

  /// The total number of columns.
  int num_cols_;

  /// For coordination.
  multithreading::ReadLock read_lock_;

  /// Unique ptr to the statement we are iterating through.
  std::unique_ptr<sqlite3_stmt, int (*)(sqlite3_stmt*)> stmt_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_SQLITE3ITERATOR_HPP_

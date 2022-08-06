// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "database/PostgresIterator.hpp"

namespace database {
// Refer to the following sources in the documentation:
// https://www.postgresql.org/docs/8.4/libpq-example.html
// https://www.postgresql.org/docs/8.1/sql-fetch.html

PostgresIterator::PostgresIterator(
    const std::shared_ptr<PGconn>& _connection, const std::string& _sql,
    const std::vector<std::string>& _time_formats, const std::int32_t _begin,
    const std::int32_t _end)
    : close_required_(false),
      colnum_(0),
      connection_(_connection),
      end_required_(false),
      rownum_(0),
      time_formats_(_time_formats) {
  execute("BEGIN");

  end_required_ = true;

  execute("DECLARE getmlcursor CURSOR FOR " + _sql);

  close_required_ = true;

  if (_begin >= 0 && _end >= _begin) {
    skip_next(_begin);

    // We fetch one extra row to prevent the iterator from unnecessarily
    // fetching 10000 extra rows.
    fetch_next(_end - _begin + 1);
  } else {
    fetch_next(10000);
  }

  num_cols_ = PQnfields(result());

  if (num_cols_ <= 0) {
    throw std::runtime_error(
        "Your query must contain at least"
        " one column!");
  }
}

// ----------------------------------------------------------------------------

PostgresIterator::PostgresIterator(
    const std::shared_ptr<PGconn>& _connection,
    const std::vector<std::string>& _colnames,
    const std::vector<std::string>& _time_formats, const std::string& _tname,
    const std::string& _where, const std::int32_t _begin,
    const std::int32_t _end)
    : PostgresIterator(_connection, make_sql(_colnames, _tname, _where),
                       _time_formats, _begin, _end) {}

// ----------------------------------------------------------------------------

PostgresIterator::~PostgresIterator() {
  if (close_required_) {
    close_cursor();
  }

  if (end_required_) {
    end_transaction();
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> PostgresIterator::colnames() const {
  std::vector<std::string> colnames(num_cols_);

  for (int i = 0; i < num_cols_; ++i) {
    colnames[i] = PQfname(result(), i);
  }

  return colnames;
}

// ----------------------------------------------------------------------------

std::shared_ptr<PGresult> PostgresIterator::execute(
    const std::string& _sql) const {
  auto raw_ptr = PQexec(connection(), _sql.c_str());

  auto result = std::shared_ptr<PGresult>(raw_ptr, PQclear);

  const auto status = PQresultStatus(result.get());

  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    const std::string error_msg = PQresultErrorMessage(result.get());

    throw std::runtime_error("Executing command in postgres iterator failed: " +
                             error_msg);
  }

  return result;
}

// ----------------------------------------------------------------------------

Float PostgresIterator::get_double() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_double(std::string(str));
}

// ----------------------------------------------------------------------------

Int PostgresIterator::get_int() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return 0;
  }

  return Getter::get_int(std::string(str));
}

// ----------------------------------------------------------------------------

std::string PostgresIterator::get_string() {
  const auto [val, is_null] = get_value();

  if (is_null) {
    return "NULL";
  }

  return val;
}

// ----------------------------------------------------------------------------

Float PostgresIterator::get_time_stamp() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_time_stamp(std::string(str), time_formats_);
}

// ----------------------------------------------------------------------------

std::string PostgresIterator::make_sql(
    const std::vector<std::string>& _colnames, const std::string& _tname,
    const std::string& _where) {
  std::string sql = "SELECT ";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    if (_colnames[i] == "COUNT(*)") {
      sql += _colnames[i];
    } else {
      sql += "\"";
      sql += _colnames[i];
      sql += "\"";
    }

    if (i + 1 < _colnames.size()) {
      sql += ", ";
    }
  }

  const auto tname = io::StatementMaker::handle_schema(_tname, "\"", "\"");

  sql += " FROM \"" + tname + "\"";

  if (_where != "") {
    sql += " WHERE " + _where;
  }

  sql += ";";

  return sql;
}

// ----------------------------------------------------------------------------

}  // namespace database

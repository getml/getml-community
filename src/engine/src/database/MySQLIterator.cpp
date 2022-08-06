// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "database/MySQLIterator.hpp"

#include <cmath>

#include "debug/debug.hpp"

namespace database {
// ----------------------------------------------------------------------------

MySQLIterator::MySQLIterator(const std::shared_ptr<MYSQL>& _connection,
                             const std::string& _sql,
                             const std::vector<std::string>& _time_formats)
    : colnum_(0),
      connection_(_connection),
      num_cols_(0),
      row_(NULL),
      time_formats_(_time_formats) {
  result_ = execute(_sql);

  if (!result_) {
    throw std::runtime_error("Query returned no result!");
  }

  num_cols_ = mysql_field_count(connection());

  if (num_cols_ == 0) {
    throw std::runtime_error(
        "Your query must contain at least"
        " one column!");
  }

  row_ = mysql_fetch_row(result_.get());

  if (mysql_errno(connection())) {
    throw_error(connection());
  }
}

// ----------------------------------------------------------------------------

MySQLIterator::MySQLIterator(const std::shared_ptr<MYSQL>& _connection,
                             const std::vector<std::string>& _colnames,
                             const std::vector<std::string>& _time_formats,
                             const std::string& _tname,
                             const std::string& _where)
    : MySQLIterator(_connection, make_sql(_colnames, _tname, _where),
                    _time_formats) {}

// ----------------------------------------------------------------------------

MySQLIterator::~MySQLIterator() {}

// ----------------------------------------------------------------------------

std::vector<std::string> MySQLIterator::colnames() const {
  auto colnames = std::vector<std::string>(num_cols_);

  for (unsigned int i = 0; i < num_cols_; ++i) {
    colnames[i] = mysql_fetch_field(result_.get())->name;
  }

  return colnames;
}

// ----------------------------------------------------------------------------

std::shared_ptr<MYSQL_RES> MySQLIterator::execute(
    const std::string& _sql) const {
  const auto len = static_cast<int>(_sql.size());

  const auto err = mysql_real_query(connection(), _sql.c_str(), len);

  if (err) {
    throw_error(connection());
  }

  auto result = std::shared_ptr<MYSQL_RES>();

  while (true) {
    const auto raw_ptr = mysql_store_result(connection());

    if (raw_ptr) {
      result = std::shared_ptr<MYSQL_RES>(raw_ptr, mysql_free_result);
    } else {
      result = std::shared_ptr<MYSQL_RES>();

      // if raw_ptr is null, that means that either some error
      // occurred, or there is no result.
      if (mysql_field_count(connection()) != 0) {
        throw_error(connection());
      }
    }

    const auto status = mysql_next_result(connection());

    // more results? -1 = no, > 0 = error, 0 = yes (keep looping)
    if (status < 0) {
      break;
    } else if (status > 0) {
      throw_error(connection());
    }
  }

  return result;
}

// ----------------------------------------------------------------------------

Float MySQLIterator::get_double() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_double(str);
}

// ----------------------------------------------------------------------------

Int MySQLIterator::get_int() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return 0;
  }

  return Getter::get_int(str);
}

// ----------------------------------------------------------------------------

std::string MySQLIterator::get_string() {
  const auto [val, is_null] = get_value();

  if (is_null) {
    return "NULL";
  }

  return val;
}

// ----------------------------------------------------------------------------

Float MySQLIterator::get_time_stamp() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_time_stamp(str, time_formats_);
}

// ----------------------------------------------------------------------------

std::string MySQLIterator::make_sql(const std::vector<std::string>& _colnames,
                                    const std::string& _tname,
                                    const std::string& _where) {
  std::string sql = "SELECT ";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    const auto& cname = _colnames[i];

    const bool is_not_count = (cname != "COUNT(*)");

    if (is_not_count) {
      sql += "`";
    }

    sql += cname;

    if (is_not_count) {
      sql += "`";
    }

    if (i + 1 < _colnames.size()) {
      sql += ", ";
    }
  }

  // Note that the user might want to pass information on the schema.
  const auto pos = _tname.find(".");

  if (pos != std::string::npos) {
    const auto schema = _tname.substr(0, pos);

    const auto table_name = _tname.substr(pos + 1);

    sql += " FROM `" + schema + "`.`" + table_name + "`";
  } else {
    sql += " FROM `" + _tname + "`";
  }

  if (_where != "") {
    sql += " WHERE " + _where;
  }

  sql += ";";

  return sql;
}

// ----------------------------------------------------------------------------

}  // namespace database

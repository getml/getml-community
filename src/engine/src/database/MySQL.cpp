// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "database/MySQL.hpp"

#include "database/CSVBuffer.hpp"
#include "database/ContentGetter.hpp"

#include <rfl/json/write.hpp>

namespace database {

void MySQL::check_colnames(const std::vector<std::string>& _colnames,
                           io::Reader* _reader) {
  const auto csv_colnames = _reader->colnames();

  if (csv_colnames.size() != _colnames.size()) {
    throw std::runtime_error("Wrong number of columns. Expected " +
                             std::to_string(_colnames.size()) + ", saw " +
                             std::to_string(csv_colnames.size()) + ".");
  }

  for (size_t i = 0; i < _colnames.size(); ++i) {
    if (csv_colnames.at(i) != _colnames.at(i)) {
      throw std::runtime_error("Column " + std::to_string(i + 1) +
                               " has wrong name. Expected '" + _colnames.at(i) +
                               "', saw '" + csv_colnames.at(i) + "'.");
    }
  }
}

// ----------------------------------------------------------------------------

std::string MySQL::describe() const {
  const auto description = rfl::make_field<"dbname">(dbname_) *
                           rfl::make_field<"dialect">(dialect()) *
                           rfl::make_field<"host">(host_) *
                           rfl::make_field<"port">(port_);
  return rfl::json::write(description);
}

// ----------------------------------------------------------------------------

std::shared_ptr<MYSQL_RES> MySQL::exec(
    const std::string& _sql, const std::shared_ptr<MYSQL>& _conn) const {
  const auto len = static_cast<int>(_sql.size());

  const auto err = mysql_real_query(_conn.get(), _sql.c_str(), len);

  if (err) {
    throw_error(_conn);
  }

  auto result = std::shared_ptr<MYSQL_RES>();

  while (true) {
    const auto raw_ptr = mysql_store_result(_conn.get());

    if (raw_ptr) {
      result = std::shared_ptr<MYSQL_RES>(raw_ptr, mysql_free_result);
    } else {
      result = std::shared_ptr<MYSQL_RES>();

      if (mysql_field_count(_conn.get()) != 0) {
        throw_error(_conn);
      }
    }

    const auto status = mysql_next_result(_conn.get());

    // more results? -1 = no, > 0 = error, 0 = yes (keep looping)
    if (status < 0) {
      break;
    } else if (status > 0) {
      throw_error(_conn);
    }
  }

  return result;
}

// ----------------------------------------------------------------------------

std::vector<std::string> MySQL::get_colnames_from_query(
    const std::string& _query) const {
  const auto conn = make_connection();

  const auto result = exec(_query, conn);

  if (!result) {
    throw std::runtime_error("Query returned no result!");
  }

  const auto num_cols = mysql_num_fields(result.get());

  auto colnames = std::vector<std::string>(num_cols);

  for (unsigned int i = 0; i < num_cols; ++i) {
    colnames[i] = mysql_fetch_field(result.get())->name;
  }

  return colnames;
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> MySQL::get_coltypes_from_query(
    const std::string& _query, const std::vector<std::string>&) const {
  const auto conn = make_connection();

  const auto result = exec(_query, conn);

  if (!result) {
    throw std::runtime_error("Query returned no result!");
  }

  const auto num_cols = mysql_num_fields(result.get());

  auto coltypes = std::vector<io::Datatype>(num_cols);

  for (unsigned int i = 0; i < num_cols; ++i) {
    coltypes[i] = interpret_field_type(mysql_fetch_field(result.get())->type);
  }

  return coltypes;
}

// ----------------------------------------------------------------------------

TableContent MySQL::get_content(const std::string& _tname,
                                const std::int32_t _draw,
                                const std::int32_t _start,
                                const std::int32_t _length) {
  const auto nrows = static_cast<std::int32_t>(get_nrows(_tname));

  if (_length < 0) {
    throw std::runtime_error("length must be positive!");
  }

  if (_start < 0) {
    throw std::runtime_error("start must be positive!");
  }

  if (_start >= nrows) {
    throw std::runtime_error("start must be smaller than number of rows!");
  }

  const auto colnames = get_colnames_from_table(_tname);

  const auto ncols = colnames.size();

  const auto end = (_start + _length > nrows) ? nrows : _start + _length;

  const auto query = make_get_content_query(_tname, colnames, _start, end);

  const auto iter =
      rfl::Ref<MySQLIterator>::make(make_connection(), query, time_formats_);

  return ContentGetter::get_content(iter, _draw, end - _start, nrows, ncols);
}

// ----------------------------------------------------------------------------

io::Datatype MySQL::interpret_field_type(const enum_field_types _type) const {
  // https://dev.mysql.com/doc/refman/5.7/en/c-api-prepared-statement-type-codes.html
  switch (_type) {
    case MYSQL_TYPE_FLOAT:
    case MYSQL_TYPE_DOUBLE:
      return io::Datatype::double_precision;

    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      return io::Datatype::integer;

    default:
      return io::Datatype::string;
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> MySQL::list_tables() {
  auto iterator = std::make_shared<MySQLIterator>(
      make_connection(), "SHOW TABLES;", time_formats_);

  auto tnames = std::vector<std::string>(0);

  while (!iterator->end()) {
    tnames.push_back(iterator->get_string());
  }

  return tnames;
}

// ----------------------------------------------------------------------------

std::string MySQL::make_get_content_query(
    const std::string& _table, const std::vector<std::string>& _colnames,
    const std::int32_t _begin, const std::int32_t _end) const {
  assert_true(_end >= _begin);

  std::string query = "SELECT ";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    query += "`";
    query += _colnames[i];
    query += "`";

    if (i != _colnames.size() - 1) {
      query += ",";
    }

    query += " ";
  }

  query += "FROM `";

  query += _table;

  query += "` LIMIT " + std::to_string(_begin);

  query += "," + std::to_string(_end - _begin);

  query += ";";

  return query;
}

// ----------------------------------------------------------------------------

std::string MySQL::make_bulk_insert_query(
    const std::string& _table,
    const std::vector<std::string>& _colnames) const {
  std::string query = "INSERT INTO `" + _table + "` (";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    query += "`";
    query += _colnames[i];
    query += "`";

    if (i + 1 < _colnames.size()) {
      query += ",";
    }
  }

  query += ")";

  query += " VALUES ";

  return query;
}

// ----------------------------------------------------------------------------

void MySQL::read(const std::string& _table, const size_t _skip,
                 io::Reader* _reader) {
  const std::vector<std::string> colnames = get_colnames_from_table(_table);

  const std::vector<io::Datatype> coltypes =
      get_coltypes_from_table(_table, colnames);

  assert_true(colnames.size() == coltypes.size());

  check_colnames(colnames, _reader);

  // Skip lines, if necessary.
  size_t line_count = 0;

  for (size_t i = 0; i < _skip; ++i) {
    _reader->next_line();
    ++line_count;
  }

  // Create a temporary file and insert the parsed CSV, line by line.
  const auto query = make_bulk_insert_query(_table, colnames);

  execute("START TRANSACTION;");

  while (!_reader->eof()) {
    const size_t bulk_size = 100000;

    auto current_query = query;

    for (size_t i = 0; i < bulk_size; ++i) {
      const std::vector<std::string> line = _reader->next_line();

      ++line_count;

      if (line.size() == 0) {
        continue;
      } else if (line.size() != coltypes.size()) {
        std::cout << "Corrupted line: " << line_count << ". Expected "
                  << colnames.size() << " fields, saw " << line.size() << "."
                  << std::endl;

        continue;
      }

      const std::string buffer =
          CSVBuffer::make_buffer(line, coltypes, ',', '"', true, true);

      current_query += '(' + buffer;

      // The last line of the buffer
      // is always a newline character.
      current_query.back() = ')';

      current_query += ',';

      if (_reader->eof()) {
        break;
      }
    }

    current_query.back() = ';';

    execute(current_query);
  }

  execute("COMMIT;");

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace database

#include "database/ODBC.hpp"

#include "helpers/NullChecker.hpp"
#include "io/Datatype.hpp"

namespace database {

void ODBC::check_colnames(const std::vector<std::string>& _colnames,
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

Poco::JSON::Object ODBC::describe() const {
  Poco::JSON::Object obj;

  std::string escape_chars;

  escape_chars += escape_char1_;
  escape_chars += escape_char2_;

  obj.set("dialect", dialect());

  obj.set("double_precision", double_precision_);

  obj.set("escape_chars", escape_chars);

  obj.set("integer", integer_);

  obj.set("server_name", server_name_);

  obj.set("text", text_);

  obj.set("user", user_);

  return obj;
}

// ----------------------------------------------------------------------------

void ODBC::drop_table(const std::string& _tname) {
  std::string query = "DROP TABLE ";

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _tname;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += ";";

  execute(query);
}

// ----------------------------------------------------------------------------

void ODBC::execute(const std::string& _queries) {
  const auto conn = make_connection();

  assert_true(conn);

  std::string query;

  for (char c : _queries) {
    if (c == ';') {
      query = io::Parser::trim(query);

      if (query.size() > 0) {
        query += ';';
        ODBCStmt(*conn, query);
      }

      query.clear();
    } else {
      query += c;
    }
  }

  query = io::Parser::trim(query);

  if (query.size() > 0) {
    query += ';';
    ODBCStmt(*conn, query);
  }
}

// ----------------------------------------------------------------------------

std::pair<char, char> ODBC::extract_escape_chars(
    const Poco::JSON::Object& _obj) const {
  const auto escape_chars =
      jsonutils::JSON::get_value<std::string>(_obj, "escape_chars_");

  switch (escape_chars.size()) {
    case 0:
      return std::make_pair(' ', ' ');

    case 1:
      return std::make_pair(escape_chars[0], escape_chars[0]);

    case 2:
      return std::make_pair(escape_chars[0], escape_chars[1]);

    default:
      throw std::invalid_argument(
          "escape_chars cannot contain more than two "
          "characters.");
  }

  return std::make_pair(' ', ' ');
}

// ----------------------------------------------------------------------------

std::vector<
    std::tuple<SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLSMALLINT>>
ODBC::get_coldescriptions(const std::string& _table) const {
  const auto opt = make_limited_iterator(_table, 0, 2);

  if (opt) {
    return opt->coldescriptions();
  }

  const auto iter =
      ODBCIterator(make_connection(), simple_select(_table), time_formats_);

  return iter.coldescriptions();
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_colnames(const std::string& _table) const {
  const auto opt = make_limited_iterator(_table, 0, 2);

  if (opt) {
    return opt->colnames();
  }

  const auto iter =
      ODBCIterator(make_connection(), simple_select(_table), time_formats_);

  return iter.colnames();
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> ODBC::get_coltypes(
    const std::string& _table,
    const std::vector<std::string>& _colnames) const {
  const auto opt = make_limited_iterator(_table, 0, 2);

  if (opt) {
    return opt->coltypes();
  }

  const auto iter =
      ODBCIterator(make_connection(), simple_select(_table), time_formats_);

  return iter.coltypes();
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_catalogs() const {
  constexpr SQLSMALLINT ncols = 5;

  const auto conn = ODBCConn(env(), server_name_, user_, passwd_);

  auto stmt = ODBCStmt(conn);

  auto ret =
      SQLTables(stmt.handle_, (SQLCHAR*)SQL_ALL_CATALOGS, SQL_NTS, (SQLCHAR*)"",
                SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);

  ODBCError::check(ret, "SQLTables in get_catalogs", stmt.handle_,
                   SQL_HANDLE_STMT);

  auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

  auto lens = std::array<SQLLEN, ncols>();

  for (SQLSMALLINT i = 0; i < ncols; ++i) {
    data[i] = std::make_unique<SQLCHAR[]>(1024);

    ret = SQLBindCol(stmt.handle_, i + 1, SQL_C_CHAR, data[i].get(), 1024,
                     &lens[i]);

    ODBCError::check(ret, "SQLBindCol in get_catalogs", stmt.handle_,
                     SQL_HANDLE_STMT);
  }

  auto vec = std::vector<std::string>({""});

  while (true) {
    ret = SQLFetch(stmt.handle_);

    if (ret == SQL_NO_DATA) {
      return vec;
    }

    ODBCError::check(ret, "SQLFetch in get_catalogs", stmt.handle_,
                     SQL_HANDLE_STMT);

    if (std::get<0>(lens) != SQL_NULL_DATA) {
      vec.push_back(
          std::string(reinterpret_cast<const char*>(std::get<0>(data).get())));
    }
  }

  return vec;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object ODBC::get_content(const std::string& _tname,
                                     const std::int32_t _draw,
                                     const std::int32_t _start,
                                     const std::int32_t _length) {
  // ----------------------------------------

  const auto nrows = get_nrows(_tname);

  const auto colnames = get_colnames(_tname);

  const auto ncols = colnames.size();

  // ----------------------------------------

  Poco::JSON::Object obj;

  // ----------------------------------------

  obj.set("draw", _draw);

  obj.set("recordsTotal", nrows);

  obj.set("recordsFiltered", nrows);

  if (nrows == 0) {
    obj.set("data", Poco::JSON::Array());
    return obj;
  }

  // ----------------------------------------

  if (_length < 0) {
    throw std::invalid_argument("length must be positive!");
  }

  if (_start < 0) {
    throw std::invalid_argument("start must be positive!");
  }

  if (_start >= nrows) {
    throw std::invalid_argument("start must be smaller than number of rows!");
  }

  // ----------------------------------------

  const auto begin = _start;

  const auto end = (_start + _length > nrows) ? nrows : _start + _length;

  auto iterator = make_limited_iterator(_tname, begin, end);

  if (!iterator) {
    throw std::runtime_error(
        "Unable to select a limited subset from table '" + _tname +
        "'. This might be because the table does not exist, because "
        "your escape_chars are not properly set or because you are "
        "using an ODBC driver that does not support this kind of SQL "
        "syntax.");
  }

  // ----------------------------------------

  Poco::JSON::Array data;

  for (auto i = begin; i < end; ++i) {
    Poco::JSON::Array row;

    for (size_t j = 0; j < ncols; ++j) {
      row.add(iterator->get_string());
    }

    data.add(row);
  }

  // ----------------------------------------

  obj.set("data", data);

  // ----------------------------------------

  return obj;

  // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_schemas(const std::string& _catalog) const {
  constexpr SQLSMALLINT ncols = 5;

  const auto catalog = to_ptr(_catalog);

  const auto conn = ODBCConn(env(), server_name_, user_, passwd_);

  auto stmt = ODBCStmt(conn);

  auto ret =
      SQLTables(stmt.handle_, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)SQL_ALL_SCHEMAS,
                SQL_NTS, (SQLCHAR*)"", SQL_NTS, (SQLCHAR*)"", SQL_NTS);

  ODBCError::check(ret, "SQLTables in get_schemas", stmt.handle_,
                   SQL_HANDLE_STMT);

  auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

  auto lens = std::array<SQLLEN, ncols>();

  for (SQLSMALLINT i = 0; i < ncols; ++i) {
    data[i] = std::make_unique<SQLCHAR[]>(1024);

    ret = SQLBindCol(stmt.handle_, i + 1, SQL_C_CHAR, data[i].get(), 1024,
                     &lens[i]);

    ODBCError::check(ret, "SQLBindCol in get_schemas", stmt.handle_,
                     SQL_HANDLE_STMT);
  }

  auto vec = std::vector<std::string>({""});

  while (true) {
    ret = SQLFetch(stmt.handle_);

    if (ret == SQL_NO_DATA) {
      return vec;
    }

    ODBCError::check(ret, "SQLFetch in get_schemas", stmt.handle_,
                     SQL_HANDLE_STMT);

    if (std::get<1>(lens) != SQL_NULL_DATA) {
      vec.push_back(
          std::string(reinterpret_cast<const char*>(std::get<1>(data).get())));
    }
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_tables(const std::string& _catalog,
                                          const std::string& _schema) const {
  constexpr SQLSMALLINT ncols = 5;

  const auto catalog = to_ptr(_catalog);

  const auto schema = to_ptr(_catalog);

  const auto conn = ODBCConn(env(), server_name_, user_, passwd_);

  auto stmt = ODBCStmt(conn);

  auto ret = SQLTables(stmt.handle_, catalog.get(), SQL_NTS, schema.get(),
                       SQL_NTS, (SQLCHAR*)SQL_ALL_TABLE_TYPES, SQL_NTS,
                       (SQLCHAR*)"'TABLE'", SQL_NTS);

  ODBCError::check(ret, "SQLTables in get_tables", stmt.handle_,
                   SQL_HANDLE_STMT);

  auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

  auto lens = std::array<SQLLEN, ncols>();

  auto vec = std::vector<std::string>();

  while (true) {
    ret = SQLFetch(stmt.handle_);

    if (ret == SQL_NO_DATA) {
      return vec;
    }

    ODBCError::check(ret, "SQLFetch in get_tables", stmt.handle_,
                     SQL_HANDLE_STMT);

    if (std::get<0>(lens) != SQL_NULL_DATA) {
      vec.push_back(
          std::string(reinterpret_cast<const char*>(std::get<0>(data).get())));
    }
  }
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::list_tables() {
  auto all_tables = std::vector<std::string>();

  const auto catalogs = get_catalogs();

  for (const auto& cat : catalogs) {
    const auto schemas = get_schemas(cat);

    for (const auto& sch : schemas) {
      const auto tables = get_tables(cat, sch);

      for (const auto& table : tables) {
        std::string tname;

        if (cat != "") {
          tname += cat + ".";
        }

        if (sch != "") {
          tname += sch + ".";
        }

        tname += table;

        all_tables.push_back(tname);
      }
    }
  }

  // ------------------------------------------------------

  if (all_tables.size() > 0) {
    return all_tables;
  }

  // ------------------------------------------------------
  // MySQL / MariaDB style

  try {
    auto iter = ODBCIterator(make_connection(), "SHOW TABLES;", time_formats_);

    while (!iter.end()) {
      all_tables.push_back(iter.get_string());
    }
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  // ------------------------------------------------------
  // Oracle style

  try {
    auto iter = ODBCIterator(
        make_connection(), "SELECT table_name FROM all_tables;", time_formats_);

    while (!iter.end()) {
      all_tables.push_back(iter.get_string());
    }
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  // ------------------------------------------------------

  if (all_tables.size() > 0) {
    return all_tables;
  }

  // ------------------------------------------------------
  // Postgres, MSSQL style

  try {
    auto iter = ODBCIterator(
        make_connection(), "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES;",
        time_formats_);

    while (!iter.end()) {
      all_tables.push_back(iter.get_string());
    }
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  // ------------------------------------------------------

  if (all_tables.size() > 0) {
    return all_tables;
  }

  // ------------------------------------------------------
  // IBM DB2 style (untested)

  try {
    auto iter = ODBCIterator(make_connection(), "LIST TABLES;", time_formats_);

    while (!iter.end()) {
      iter.get_string();                        // TABLE_SCHEM
      all_tables.push_back(iter.get_string());  // TABLE_NAME
      iter.get_string();                        // TABLE_TYPE
    }
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }
  // ------------------------------------------------------

  return all_tables;

  // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string ODBC::make_bulk_insert_query(
    const std::string& _table,
    const std::vector<std::string>& _colnames) const {
  std::string query = "INSERT INTO ";

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _table;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += " (";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    if (escape_char1_ != ' ') {
      query += escape_char1_;
    }

    query += _colnames[i];

    if (escape_char2_ != ' ') {
      query += escape_char2_;
    }

    if (i + 1 < _colnames.size()) {
      query += ",";
    }
  }

  query += ")";

  query += " VALUES ";

  query += " (";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    query += "?";

    if (i + 1 < _colnames.size()) {
      query += ",";
    }
  }

  query += ");";

  return query;
}

// ----------------------------------------------------------------------------

std::optional<ODBCIterator> ODBC::make_limited_iterator(
    const std::string& _table, const size_t _begin, const size_t _end) const {
  if (_begin == 0) {
    try {
      return std::make_optional<ODBCIterator>(
          make_connection(), simple_limit_standard(_table, _end),
          time_formats_);
    } catch (std::exception& e) {
      if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
        throw std::runtime_error(e.what());
      }
    }
  }

  try {
    return std::make_optional<ODBCIterator>(
        make_connection(), simple_limit_most(_table, _begin, _end),
        time_formats_);
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  try {
    return std::make_optional<ODBCIterator>(
        make_connection(), simple_limit_oracle(_table, _begin, _end),
        time_formats_);
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  try {
    return std::make_optional<ODBCIterator>(
        make_connection(), simple_limit_mssql(_table, _begin, _end),
        time_formats_);
  } catch (std::exception& e) {
    if (std::string(e.what()).find("(SQL_ERROR)") == std::string::npos) {
      throw std::runtime_error(e.what());
    }
  }

  return std::optional<ODBCIterator>();
}

// ----------------------------------------------------------------------------

void ODBC::read(const std::string& _table, const size_t _skip,
                io::Reader* _reader) {
  const auto colnames = get_colnames(_table);

  const auto coldesc = get_coldescriptions(_table);

  if (colnames.size() != coldesc.size()) {
    throw std::runtime_error(
        "The number of retrieved column names does not match the "
        "number of retrieved column descriptions.");
  }

  check_colnames(colnames, _reader);

  size_t line_count = 0;

  for (size_t i = 0; i < _skip; ++i) {
    _reader->next_line();
    ++line_count;
  }

  auto conn = make_connection(NO_AUTOCOMMIT);

  assert_true(conn);

  auto stmt = ODBCStmt(*conn);

  auto fields = std::vector<std::unique_ptr<SQLCHAR[]>>(colnames.size());

  auto flen = std::vector<SQLLEN>(colnames.size());

  constexpr SQLLEN buffer_length = 1024;

  for (size_t i = 0; i < colnames.size(); ++i) {
    fields.at(i) = std::make_unique<SQLCHAR[]>(buffer_length);

    // https://docs.microsoft.com/en-us/transpilation/odbc/reference/syntax/sqlbindparameter-function?view=sql-server-ver15
    const auto ret = SQLBindParameter(
        stmt.handle_, static_cast<SQLUSMALLINT>(i + 1), SQL_PARAM_INPUT,
        SQL_C_CHAR, std::get<1>(coldesc.at(i)), std::get<2>(coldesc.at(i)),
        std::get<3>(coldesc.at(i)), fields.at(i).get(), buffer_length,
        &flen.at(i));

    ODBCError::check(ret, "SQLBindParameter in read", stmt.handle_,
                     SQL_HANDLE_DBC);
  }

  const auto query = to_ptr(make_bulk_insert_query(_table, colnames));

  auto ret = SQLPrepare(stmt.handle_, query.get(), SQL_NTS);

  ODBCError::check(ret, "SQLPrepare in read", stmt.handle_, SQL_HANDLE_DBC);

  const auto coltypes = get_coltypes(_table, colnames);

  assert_true(coltypes.size() == colnames.size());

  while (true) {
    const auto line = get_next_line(coltypes, line_count, _reader);

    if (!line) {
      break;
    }

    if (line->size() != coltypes.size()) {
      continue;
    }

    ++line_count;

    for (size_t i = 0; i < fields.size(); ++i) {
      if (helpers::NullChecker::is_null(line->at(i))) {
        flen[i] = SQL_NULL_DATA;
        continue;
      }

      flen[i] =
          std::min(buffer_length - 1, static_cast<SQLLEN>(line->at(i).size()));

      std::copy(line->at(i).begin(), line->at(i).begin() + flen[i],
                reinterpret_cast<char*>(fields[i].get()));
    }

    ret = SQLExecute(stmt.handle_);

    ODBCError::check(ret, "SQLExecute in read", stmt.handle_, SQL_HANDLE_DBC);
  }

  ret = SQLEndTran(SQL_HANDLE_DBC, conn->handle_, SQL_COMMIT);

  ODBCError::check(ret, "SQLEndTran(SQL_COMMIT) in read", conn->handle_,
                   SQL_HANDLE_DBC);
}

// ----------------------------------------------------------------------------

std::optional<std::vector<std::string>> ODBC::get_next_line(
    const std::vector<io::Datatype>& _coltypes, const size_t _line_count,
    io::Reader* _reader) const {
  if (_reader->eof()) {
    return std::nullopt;
  }

  auto line = _reader->next_line();

  if (line.size() == 0) {
    return line;
  }

  if (line.size() != _coltypes.size()) {
    std::cout << "Corrupted line: " << _line_count + 1 << ". Expected "
              << _coltypes.size() << " fields, saw " << line.size() << "."
              << std::endl;

    return line;
  }

  return line;
}

// ----------------------------------------------------------------------------

std::string ODBC::simple_limit_standard(const std::string& _table,
                                        const size_t _end) const {
  auto query = std::string("SELECT * FROM ");

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _table;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += " FETCH FIRST ";

  query += std::to_string(_end);

  query += " ROWS ONLY;";

  return query;
}

// ----------------------------------------------------------------------------

std::string ODBC::simple_limit_most(const std::string& _table,
                                    const size_t _begin,
                                    const size_t _end) const {
  auto query = std::string("SELECT * FROM ");

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _table;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += " LIMIT ";

  query += std::to_string(_end - _begin);

  if (_begin > 0) {
    query += " OFFSET ";

    query += std::to_string(_begin);
  }

  query += ";";

  return query;
}

// ----------------------------------------------------------------------------

std::string ODBC::simple_limit_oracle(const std::string& _table,
                                      const size_t _begin,
                                      const size_t _end) const {
  auto query = std::string("SELECT * FROM ");

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _table;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += " WHERE ROWNUM <= ";

  query += std::to_string(_end);

  if (_begin > 0) {
    query += " AND ROWNUM > ";

    query += std::to_string(_begin);
  }

  query += ";";

  return query;
}

// ----------------------------------------------------------------------------

std::string ODBC::simple_limit_mssql(const std::string& _table,
                                     const size_t _begin,
                                     const size_t _end) const {
  if (_begin == 0) {
    std::string query = "SELECT TOP ";

    query += std::to_string(_end);

    query += " * FROM ";

    if (escape_char1_ != ' ') {
      query += escape_char1_;
    }

    query += _table;

    if (escape_char2_ != ' ') {
      query += escape_char2_;
    }

    query += ";";

    return query;
  } else {
    const auto colnames = get_colnames(_table);

    std::string all_colnames;

    for (size_t i = 0; i < colnames.size(); ++i) {
      if (escape_char1_ != ' ') {
        all_colnames += escape_char1_;
      }

      all_colnames += colnames[i];

      if (escape_char2_ != ' ') {
        all_colnames += escape_char2_;
      }

      if (i < colnames.size() - 1) {
        all_colnames += ",";
      }
    }

    std::string query = "SELECT ";

    query += all_colnames;

    query += " FROM( SELECT ";

    query += all_colnames;

    query +=
        ", ROW_NUMBER() OVER( ORDER BY getml_sort ) AS getml_rownum "
        "FROM( SELECT ";

    query += all_colnames;

    query += ",0 AS getml_sort FROM ";

    if (escape_char1_ != ' ') {
      query += escape_char1_;
    }

    query += _table;

    if (escape_char2_ != ' ') {
      query += escape_char2_;
    }

    query += " ) a ) b WHERE getml_rownum > ";

    query += std::to_string(_begin);

    query += " AND getml_rownum <= ";

    query += std::to_string(_end);

    query += ";";

    return query;
  }
}

// ----------------------------------------------------------------------------

std::string ODBC::simple_select(const std::string& _table) const {
  auto query = std::string("SELECT * FROM ");

  if (escape_char1_ != ' ') {
    query += escape_char1_;
  }

  query += _table;

  if (escape_char2_ != ' ') {
    query += escape_char2_;
  }

  query += ";";

  return query;
}

// ----------------------------------------------------------------------------
}  // namespace database

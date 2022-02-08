#include "database/ODBCIterator.hpp"

#include "io/StatementMaker.hpp"

namespace database {
// ----------------------------------------------------------------------------

ODBCIterator::ODBCIterator(const std::shared_ptr<ODBCConn>& _connection,
                           const std::string& _query,
                           const std::vector<std::string>& _time_formats)
    : colnum_(0),
      connection_(_connection),
      end_(false),
      time_formats_(_time_formats) {
  stmt_ = std::make_shared<ODBCStmt>(connection(), _query);

  SQLSMALLINT ncols = 0;

  auto ret = SQLNumResultCols(stmt().handle_, &ncols);

  ODBCError::check(ret, "SQLNumResultCols in ODBCIterator", stmt().handle_,
                   SQL_HANDLE_STMT);

  row_ = std::vector<std::unique_ptr<SQLCHAR[]>>(ncols);

  flen_ = std::vector<SQLLEN>(ncols);

  for (SQLSMALLINT i = 0; i < ncols; ++i) {
    row_[i] = std::make_unique<SQLCHAR[]>(1024);

    ret = SQLBindCol(stmt().handle_, i + 1, SQL_C_CHAR, row_[i].get(), 1024,
                     &flen_[i]);

    ODBCError::check(ret, "SQLBindCol in ODBCIterator", stmt().handle_,
                     SQL_HANDLE_STMT);
  }

  fetch();
}

// ----------------------------------------------------------------------------

ODBCIterator::ODBCIterator(const std::shared_ptr<ODBCConn>& _connection,
                           const std::vector<std::string>& _colnames,
                           const std::vector<std::string>& _time_formats,
                           const std::string& _tname, const std::string& _where,
                           const char _escape_char1, const char _escape_char2)
    : ODBCIterator(
          _connection,
          make_query(_colnames, _tname, _where, _escape_char1, _escape_char2),
          _time_formats) {}

// ----------------------------------------------------------------------------

ODBCIterator::~ODBCIterator() = default;

// ----------------------------------------------------------------------------

std::vector<
    std::tuple<SQLSMALLINT, SQLSMALLINT, SQLULEN, SQLSMALLINT, SQLSMALLINT>>
ODBCIterator::coldescriptions() const {
  SQLSMALLINT name_length = 0;
  SQLSMALLINT data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;

  auto buffer = std::make_unique<SQLCHAR[]>(1024);

  auto coldesc = std::vector<std::tuple<SQLSMALLINT, SQLSMALLINT, SQLULEN,
                                        SQLSMALLINT, SQLSMALLINT>>();

  for (size_t i = 0; i < row_.size(); ++i) {
    const auto ret = SQLDescribeCol(
        stmt().handle_, static_cast<SQLSMALLINT>(i + 1), buffer.get(), 1024,
        &name_length, &data_type, &column_size, &decimal_digits, &nullable);

    ODBCError::check(ret, "SQLDescribeCol in get_coltypes", stmt().handle_,
                     SQL_HANDLE_STMT);

    coldesc.push_back(std::make_tuple(name_length, data_type, column_size,
                                      decimal_digits, nullable));
  }

  return coldesc;
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBCIterator::colnames() const {
  SQLSMALLINT name_length = 0;
  SQLSMALLINT data_type = 0;
  SQLULEN column_size = 0;
  SQLSMALLINT decimal_digits = 0;
  SQLSMALLINT nullable = 0;

  assert_true(row_.size() == flen_.size());

  auto buffer = std::make_unique<SQLCHAR[]>(1024);

  auto names = std::vector<std::string>(row_.size());

  for (size_t i = 0; i < names.size(); ++i) {
    const auto ret = SQLDescribeCol(
        stmt().handle_, static_cast<SQLUSMALLINT>(i + 1), buffer.get(), 1024,
        &name_length, &data_type, &column_size, &decimal_digits, &nullable);

    ODBCError::check(ret, "SQLDescribeCol in colnames", stmt().handle_,
                     SQL_HANDLE_STMT);

    if (name_length >= 1023) {
      reinterpret_cast<char*>(buffer.get())[1023] = '\0';
    } else {
      reinterpret_cast<char*>(buffer.get())[name_length + 1] = '\0';
    }

    names[i] = std::string(reinterpret_cast<const char*>(buffer.get()));
  }

  return names;
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> ODBCIterator::coltypes() const {
  const auto coldesc = coldescriptions();

  assert_true(coldesc.size() == row_.size());

  auto coltypes = std::vector<io::Datatype>(row_.size());

  for (size_t i = 0; i < coltypes.size(); ++i) {
    coltypes.at(i) = interpret_field_type(std::get<1>(coldesc.at(i)));
  }

  return coltypes;
}

// ----------------------------------------------------------------------------

Float ODBCIterator::get_double() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_double(str);
}

// ----------------------------------------------------------------------------

Int ODBCIterator::get_int() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return 0;
  }

  return Getter::get_int(str);
}

// ----------------------------------------------------------------------------

std::string ODBCIterator::get_string() {
  const auto [val, is_null] = get_value();

  if (is_null) {
    return "NULL";
  }

  return val;
}

// ----------------------------------------------------------------------------

Float ODBCIterator::get_time_stamp() {
  const auto [str, is_null] = get_value();

  if (is_null) {
    return static_cast<Float>(NAN);
  }

  return Getter::get_time_stamp(str, time_formats_);
}

// ----------------------------------------------------------------------------

io::Datatype ODBCIterator::interpret_field_type(const SQLSMALLINT _type) const {
  switch (_type) {
    case SQL_DECIMAL:
    case SQL_NUMERIC:
    case SQL_REAL:
    case SQL_FLOAT:
    case SQL_DOUBLE:
      return io::Datatype::double_precision;

    case SQL_SMALLINT:
    case SQL_INTEGER:
    case SQL_TINYINT:
    case SQL_BIGINT:
      return io::Datatype::integer;

    default:
      return io::Datatype::string;
  }
}

// ----------------------------------------------------------------------------

std::string ODBCIterator::make_query(const std::vector<std::string>& _colnames,
                                     const std::string& _tname,
                                     const std::string& _where,
                                     const char _escape_char1,
                                     const char _escape_char2) {
  std::string query = "SELECT ";

  for (size_t i = 0; i < _colnames.size(); ++i) {
    const auto& cname = _colnames[i];

    const bool is_not_count = (cname != "COUNT(*)");

    if (is_not_count && _escape_char1 != ' ') {
      query += _escape_char1;
    }

    query += cname;

    if (is_not_count && _escape_char2 != ' ') {
      query += _escape_char2;
    }

    if (i + 1 < _colnames.size()) {
      query += ", ";
    }
  }

  const auto tname = _escape_char1 != ' ' && _escape_char2 != ' '
                         ? io::StatementMaker::handle_schema(
                               _tname, std::string(1, _escape_char1),
                               std::string(1, _escape_char2))
                         : _tname;

  query += std::string(" FROM ");

  if (_escape_char1 != ' ') {
    query += _escape_char1;
  }

  query += tname;

  if (_escape_char2 != ' ') {
    query += _escape_char2;
  }

  if (_where != "") {
    query += std::string(" WHERE ") + _where;
  }

  query += ";";

  return query;
}

// ----------------------------------------------------------------------------
}  // namespace database

#ifndef DATABASE_ODBCCONN_HPP_
#define DATABASE_ODBCCONN_HPP_

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
#include <string>

// ----------------------------------------------------------------------------

#include "database/ODBCEnv.hpp"
#include "database/ODBCError.hpp"

// ----------------------------------------------------------------------------

namespace database {
// ----------------------------------------------------------------------------

struct ODBCConn {
  ODBCConn(const ODBCEnv& _env, const std::string& _server_name,
           const std::string& _user, const std::string& _passwd,
           const bool _no_autocommit = false)
      : handle_(SQL_NULL_HDBC) {
    auto ret = SQLAllocHandle(SQL_HANDLE_DBC, _env.handle_, &handle_);

    ODBCError::check(ret, "SQLAllocHandle(SQL_HANDLE_DBC)", handle_,
                     SQL_HANDLE_DBC);

    ret = SQLSetConnectAttr(handle_, SQL_LOGIN_TIMEOUT, (SQLPOINTER)5, 0);

    ODBCError::check(ret, "SQLSetConnectAttr(SQL_LOGIN_TIMEOUT)", handle_,
                     SQL_HANDLE_DBC);

    if (_no_autocommit) {
      ret = SQLSetConnectAttr(handle_, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)0, 0);

      ODBCError::check(ret, "SQLSetConnectAttr(SQL_ATTR_AUTOCOMMIT)", handle_,
                       SQL_HANDLE_DBC);
    }

    auto server_name = to_ptr(_server_name);
    auto user = to_ptr(_user);
    auto passwd = to_ptr(_passwd);

    ret = SQLConnect(handle_, server_name.get(),
                     static_cast<SQLSMALLINT>(_server_name.size()), user.get(),
                     static_cast<SQLSMALLINT>(_user.size()), passwd.get(),
                     static_cast<SQLSMALLINT>(_passwd.size()));

    ODBCError::check(ret, _server_name, handle_, SQL_HANDLE_DBC);
  }

  ~ODBCConn() {
    if (handle_ != SQL_NULL_HDBC) {
      SQLDisconnect(handle_);
      SQLFreeHandle(SQL_HANDLE_DBC, handle_);
    }
  }

  ODBCConn(const ODBCConn& _other) = delete;

  ODBCConn(ODBCConn&& _other) noexcept { handle_ = std::move(_other.handle_); }

  ODBCConn& operator=(const ODBCConn& _other) = delete;

  ODBCConn& operator=(ODBCConn&& _other) noexcept {
    if (this == &_other) {
      return *this;
    }

    handle_ = std::move(_other.handle_);

    return *this;
  }

  std::unique_ptr<SQLCHAR[]> to_ptr(const std::string& _str) const {
    auto ptr = std::make_unique<SQLCHAR[]>(_str.size() + 1);

    std::copy(reinterpret_cast<const SQLCHAR*>(_str.c_str()),
              reinterpret_cast<const SQLCHAR*>(_str.c_str()) + _str.size(),
              ptr.get());

    return ptr;
  }

  SQLHDBC handle_;
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_ODBCCONN_HPP_

#ifndef DATABASE_DATABASE_HPP_
#define DATABASE_DATABASE_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <array>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include <mysql.h>

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
#define _WINSOCKAPI_    // stops windows.h including winsock.h
#include <windows.h>
#include <winsock2.h>
#include <openssl/ossl_typ.h>
#endif

#include <libpq-fe.h>
#include <sql.h>
#include <sqlext.h>

#include <Poco/JSON/Object.h>
#include <Poco/TemporaryFile.h>

extern "C"
{
#include "sqlite3/sqlite3.h"
}

#include "debug/debug.hpp"

#include "io/io.hpp"

#include "jsonutils/jsonutils.hpp"

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "database/Float.hpp"
#include "database/Int.hpp"

#include "database/CSVBuffer.hpp"
#include "database/Getter.hpp"

#include "database/Iterator.hpp"

#include "database/Connector.hpp"

#include "database/Sqlite3Iterator.hpp"

#include "database/Sqlite3.hpp"

#include "database/MySQLIterator.hpp"

#include "database/MySQL.hpp"

#include "database/ODBCError.hpp"

#include "database/ODBCEnv.hpp"

#include "database/ODBCConn.hpp"

#include "database/ODBCStmt.hpp"

#include "database/ODBCIterator.hpp"

#include "database/ODBC.hpp"

#include "database/PostgresIterator.hpp"

#include "database/Postgres.hpp"

#include "database/DatabaseParser.hpp"
#include "database/DatabaseReader.hpp"
#include "database/DatabaseSniffer.hpp"

// ----------------------------------------------------------------------------

#endif  // DATABASE_DATABASE_HPP_

#ifndef DATABASE_DATABASE_HPP_
#define DATABASE_DATABASE_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cmath>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include <mysql.h>

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// postgres is not supported in windows
#else
#include <libpq-fe.h>
#endif

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

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// postgres is not supported in windows
#else
#include "database/PostgresIterator.hpp"

#include "database/Postgres.hpp"
#endif

#include "database/DatabaseParser.hpp"
#include "database/DatabaseSniffer.hpp"

// ----------------------------------------------------------------------------

#endif  // DATABASE_DATABASE_HPP_

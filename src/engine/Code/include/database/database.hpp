#ifndef DATABASE_DATABASE_HPP_
#define DATABASE_DATABASE_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <cassert>
#include <cmath>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include <libpq-fe.h>

#include "Poco/JSON/Object.h"

extern "C"
{
#include "sqlite3/sqlite3.h"
}

#include "csv/csv.hpp"

#include "jsonutils/jsonutils.hpp"

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "database/Float.hpp"
#include "database/Int.hpp"

#include "database/Iterator.hpp"

#include "database/Connector.hpp"

#include "database/PostgresIterator.hpp"
#include "database/Sqlite3Iterator.hpp"

#include "database/Postgres.hpp"
#include "database/Sqlite3.hpp"

#include "database/DatabaseParser.hpp"

// ----------------------------------------------------------------------------

#endif  // DATABASE_DATABASE_HPP_

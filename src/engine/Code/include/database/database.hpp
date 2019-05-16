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

extern "C"
{
#include "sqlite3/sqlite3.h"
}

#include "csv/csv.hpp"

// ----------------------------------------------------------------------------

#include "database/types.hpp"

#include "database/Iterator.hpp"

#include "database/Connector.hpp"

#include "database/Sqlite3Iterator.hpp"

#include "database/Sqlite3.hpp"

// ----------------------------------------------------------------------------

#endif  // DATABASE_DATABASE_HPP_

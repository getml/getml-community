#ifndef DATABASE_TESTS_TESTS_HPP_
#define DATABASE_TESTS_TESTS_HPP_

#include <Poco/JSON/Stringifier.h>
#include <filesystem>

#include "database/database.hpp"

// General tests
#include "database/tests/test5.hpp"

// Tests for sqlite
#include "database/tests/test1.hpp"
#include "database/tests/test2.hpp"
#include "database/tests/test3.hpp"
#include "database/tests/test4.hpp"
#include "database/tests/test6.hpp"
#include "database/tests/test7.hpp"

// Tests for postgres
#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// postgres is not supported in windows
#else
#include "database/tests/test8.hpp"
#include "database/tests/test9.hpp"

#include "database/tests/test10.hpp"
#include "database/tests/test11.hpp"
#include "database/tests/test12.hpp"
#include "database/tests/test13.hpp"
#include "database/tests/test14.hpp"
#endif

// Tests for MySQL
#include "database/tests/test15.hpp"

#endif  // DATABASE_TESTS_TESTS_HPP_

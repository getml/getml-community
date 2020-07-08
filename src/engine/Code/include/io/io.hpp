#ifndef IO_IO_HPP_
#define IO_IO_HPP_

// ----------------------------------------------------------------------------
// Dependencies

#include <algorithm>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <locale>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

#include <Poco/DateTime.h>
#include <Poco/DateTimeFormatter.h>
#include <Poco/DateTimeParser.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// goutils is not supported on windows
#else
#include <goutils.hpp>
#endif

#include "debug/debug.hpp"

#include "jsonutils/jsonutils.hpp"

// ----------------------------------------------------------------------------

#include "io/Float.hpp"
#include "io/Int.hpp"

#include "io/Datatype.hpp"
#include "io/Parser.hpp"
#include "io/Reader.hpp"
#include "io/StatementMaker.hpp"

#include "io/CSVReader.hpp"
#include "io/CSVWriter.hpp"

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// goutils is not supported on windows
#else
#include "io/S3Reader.hpp"
#endif

#include "io/Sniffer.hpp"

#include "io/CSVSniffer.hpp"

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
// goutils is not supported on windows
#else
#include "io/S3Sniffer.hpp"
#endif

// ----------------------------------------------------------------------------

#endif  // IO_IO_HPP_

#ifndef IO_IO_HPP_
#define IO_IO_HPP_

// ----------------------------------------------------------------------------

#include "io/CSVReader.hpp"
#include "io/CSVSniffer.hpp"
#include "io/CSVWriter.hpp"
#include "io/Datatype.hpp"
#include "io/Float.hpp"
#include "io/Int.hpp"
#include "io/Parser.hpp"
#include "io/Reader.hpp"
#include "io/Sniffer.hpp"
#include "io/StatementMaker.hpp"

#if (defined(_WIN32) || defined(_WIN64))
// goutils is not supported on windows
#else
#include "io/S3Reader.hpp"
#include "io/S3Sniffer.hpp"
#endif

// ----------------------------------------------------------------------------

#endif  // IO_IO_HPP_

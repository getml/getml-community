#ifndef IO_CSVSNIFFER_HPP_
#define IO_CSVSNIFFER_HPP_

#include "io/CSVReader.hpp"
#include "io/Sniffer.hpp"

namespace io {
typedef Sniffer<CSVReader> CSVSniffer;
}  // namespace io

#endif  // IO_CSVSNIFFER_HPP_

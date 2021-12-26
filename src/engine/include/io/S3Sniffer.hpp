#ifndef IO_S3SNIFFER_HPP_
#define IO_S3SNIFFER_HPP_

#include "io/CSVReader.hpp"
#include "io/Sniffer.hpp"

namespace io {
typedef Sniffer<S3Reader> S3Sniffer;
}  // namespace io

#endif  // IO_S3SNIFFER_HPP_

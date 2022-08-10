// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef IO_CSVSNIFFER_HPP_
#define IO_CSVSNIFFER_HPP_

#include "io/CSVReader.hpp"
#include "io/Sniffer.hpp"

namespace io {
typedef Sniffer<CSVReader> CSVSniffer;
}  // namespace io

#endif  // IO_CSVSNIFFER_HPP_

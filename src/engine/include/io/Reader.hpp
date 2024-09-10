// Copyright 2024 Code17 GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef IO_READER_HPP_
#define IO_READER_HPP_

// ----------------------------------------------------------------------------

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

namespace io {
// ----------------------------------------------------------------------------

class Reader {
 public:
  virtual ~Reader() = default;

  /// Returns the colnames.
  virtual std::vector<std::string> colnames() = 0;

  /// Whether the end has been reached.
  virtual bool eof() const = 0;

  /// Returns the next line.
  virtual std::vector<std::string> next_line() = 0;

  /// Trivial getter.
  virtual char quotechar() const = 0;

  /// Trivial getter.
  virtual char sep() const = 0;
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_READER_HPP_

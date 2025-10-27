// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_CSVBUFFER_HPP_
#define DATABASE_CSVBUFFER_HPP_

#include "io/Datatype.hpp"

#include <string>
#include <vector>

namespace database {
// ----------------------------------------------------------------------------

class CSVBuffer {
 public:
  /// Turns a line into a buffer to be send to the server.
  static std::string make_buffer(const std::vector<std::string>& _line,
                                 const std::vector<io::Datatype>& _coltypes,
                                 const char _sep, const char _quotechar,
                                 const bool _always_enclose_str,
                                 const bool _explicit_null);

 private:
  /// Parses a raw field according to its datatype.
  static std::string parse_field(const std::string& _raw_field,
                                 const io::Datatype _datatype, const char _sep,
                                 const char _quotechar,
                                 const bool _always_enclose_str,
                                 const bool _explicit_null);

  // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_CSVBUFFER_HPP_

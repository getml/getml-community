// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef SQL_TRIMMINGGENERATOR_HPP_
#define SQL_TRIMMINGGENERATOR_HPP_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace transpilation {

class TrimmingGenerator {
 public:
  virtual ~TrimmingGenerator() = default;

  /// Generates the SQL code necessary for joining the trimming tables onto the
  /// staged table.
  virtual std::string join(const std::string& _staging_table,
                           const std::string& _colname,
                           const std::string& _replacement) const = 0;

  /// Generates the table header for the resulting SQL code.
  virtual std::string make_header(const std::string& _staging_table,
                                  const std::string& _colname) const = 0;

  /// Generates the INSERT INTO for the SQL code of the trimming.
  virtual std::string make_insert_into(const std::string& _staging_table,
                                       const std::string& _colname) const = 0;
};

}  // namespace transpilation

#endif  // SQL_TRIMMINGGENERATOR_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef SQL_HUMANREADABLETRIMMING_HPP_
#define SQL_HUMANREADABLETRIMMING_HPP_

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "transpilation/SQLDialectGenerator.hpp"
#include "transpilation/SQLGenerator.hpp"
#include "transpilation/TrimmingGenerator.hpp"

namespace transpilation {

class HumanReadableTrimming : public TrimmingGenerator {
 public:
  explicit HumanReadableTrimming(
      const SQLDialectGenerator* _sql_dialect_generator)
      : sql_dialect_generator_(_sql_dialect_generator) {}

  ~HumanReadableTrimming() = default;

 public:
  /// Generates the SQL code necessary for joining the trimming tables onto the
  /// staged table.
  std::string join(const std::string& _staging_table,
                   const std::string& _colname,
                   const std::string& _replacement) const final;

  /// Generates the table header for the resulting SQL code.
  std::string make_header(const std::string& _staging_table,
                          const std::string& _colname) const final;

  /// Generates the INSERT INTO for the SQL code of the trimming.
  std::string make_insert_into(const std::string& _staging_table,
                               const std::string& _colname) const final;

 private:
  /// Generates the name of the trimming table
  std::string make_trimming_table(const std::string& _staging_table,
                                  const std::string& _colname) const {
    return SQLGenerator::to_upper(_staging_table + "__TRIMMING_" + _colname);
  }

 private:
  /// Pointer to the underlying generator.
  const SQLDialectGenerator* const sql_dialect_generator_;
};

}  // namespace transpilation

#endif  // SQL_HUMANREADABLETRIMMING_HPP_

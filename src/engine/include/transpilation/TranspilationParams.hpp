// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef TRANSPILATION_TRANSPILATIONPARAMS_HPP_
#define TRANSPILATION_TRANSPILATIONPARAMS_HPP_

#include <cstddef>
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <string>

namespace transpilation {

struct TranspilationParams {
  using DialectType = rfl::Literal<"bigquery", "human-readable sql", "mysql",
                                   "postgres", "spark sql", "sqlite3", "tsql">;

  /// The dialect used
  rfl::Field<"dialect_", DialectType> dialect;

  /// Number of characters in categorical columns.
  rfl::Field<"nchar_categorical_", size_t> nchar_categorical;

  /// Number of characters in join key columns.
  rfl::Field<"nchar_join_key_", size_t> nchar_join_key;

  /// Number of characters in text columns.
  rfl::Field<"nchar_text_", size_t> nchar_text;

  /// The schema used.
  rfl::Field<"schema_", std::string> schema;
};

}  // namespace transpilation

#endif  // TRANSPILATION_TRANSPILATIONPARAMS_HPP_

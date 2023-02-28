// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef TRANSPILATION_TRANSPILATIONPARAMS_HPP_
#define TRANSPILATION_TRANSPILATIONPARAMS_HPP_

#include <cstddef>
#include <string>

#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "jsonutils/jsonutils.hpp"

namespace transpilation {

struct TranspilationParams {
  using DialectType = fct::Literal<"human-readable sql", "sqlite3">;

  using NamedTupleType =
      fct::NamedTuple<fct::Field<"dialect_", DialectType>,
                      fct::Field<"nchar_categorical_", size_t>,
                      fct::Field<"nchar_join_key_", size_t>,
                      fct::Field<"nchar_text_", size_t>,
                      fct::Field<"schema_", std::string> >;

  /// Generates a new set of transpilation params from a JSON.
  TranspilationParams(const NamedTupleType& _cmd)
      : dialect_(_cmd.get<"dialect_">()),
        nchar_categorical_(_cmd.get<"nchar_categorical_">()),
        nchar_join_key_(_cmd.get<"nchar_join_key_">()),
        nchar_text_(_cmd.get<"nchar_text_">()),
        schema_(_cmd.get<"schema_">()){};

  ~TranspilationParams() = default;

  /// The dialect used
  const DialectType dialect_;

  /// Number of characters in categorical columns.
  const size_t nchar_categorical_;

  /// Number of characters in join key columns.
  const size_t nchar_join_key_;

  /// Number of characters in text columns.
  const size_t nchar_text_;

  /// The schema used.
  const std::string schema_;
};

}  // namespace transpilation

#endif  // TRANSPILATION_TRANSPILATIONPARAMS_HPP_

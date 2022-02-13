#ifndef TRANSPILATION_TRANSPILATIONPARAMS_HPP_
#define TRANSPILATION_TRANSPILATIONPARAMS_HPP_

// -------------------------------------------------------------------------

#include <cstddef>
#include <string>

// -------------------------------------------------------------------------

#include "jsonutils/jsonutils.hpp"

// -------------------------------------------------------------------------

namespace transpilation {

struct TranspilationParams {
  /// Generates a new set of transpilation params from a JSON.
  static TranspilationParams from_json(const Poco::JSON::Object& _cmd) {
    return TranspilationParams{
        .dialect_ = jsonutils::JSON::get_value<std::string>(_cmd, "dialect_"),
        .nchar_categorical_ =
            jsonutils::JSON::get_value<size_t>(_cmd, "nchar_categorical_"),
        .nchar_join_key_ =
            jsonutils::JSON::get_value<size_t>(_cmd, "nchar_join_key_"),
        .nchar_text_ = jsonutils::JSON::get_value<size_t>(_cmd, "nchar_text_"),
        .schema_ = jsonutils::JSON::get_value<std::string>(_cmd, "schema_")};
  }

  ~TranspilationParams() = default;

  /// The dialect used
  const std::string dialect_;

  /// Number of characters in categorical columns.
  const size_t nchar_categorical_;

  /// Number of characters in join key columns.
  const size_t nchar_join_key_;

  /// Number of characters in text columns.
  const size_t nchar_text_;

  /// The schema used.
  const std::string schema_;
};

// -------------------------------------------------------------------------
}  // namespace transpilation

#endif  // TRANSPILATION_TRANSPILATIONPARAMS_HPP_

// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_OUTPUTARRAY_HPP_
#define FLEXBUFFERS_OUTPUTARRAY_HPP_

#include <flatbuffers/flexbuffers.h>

#include <optional>
#include <string>

#include "flexbuffers/OutputVar.hpp"
#include "rfl/Ref.hpp"

namespace flexbuffers {

class OutputArray : public OutputVar {
 public:
  OutputArray() {}

  ~OutputArray() = default;

  /// Inserts all elements into the builder.
  void insert(const std::optional<std::string>& _key,
              flexbuffers::Builder* _fbb) final {
    /// We have to catch the edge case of an empty vector,
    /// because flexbuffers ignores empty vectors/maps.
    if (vars_.size() == 0) {
      if (_key) {
        _fbb->Null(_key->c_str());
      } else {
        _fbb->Null();
      }
      return;
    }

    const auto start =
        _key ? _fbb->StartVector(_key->c_str()) : _fbb->StartVector();

    for (const auto& var : vars_) {
      var->insert(std::nullopt, _fbb);
    }

    _fbb->EndVector(start, false, false);
  };

  /// Whether this is null.
  bool is_null() const final { return false; }

  /// Adds a new element to the vector.
  void push_back(const rfl::Ref<OutputVar>& _var) { vars_.push_back(_var); }

 private:
  /// The underlying variables.
  std::vector<rfl::Ref<OutputVar>> vars_;
};

}  // namespace flexbuffers

#endif

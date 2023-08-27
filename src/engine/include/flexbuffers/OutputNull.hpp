// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_OUTPUTNULL_HPP_
#define FLEXBUFFERS_OUTPUTNULL_HPP_

#include <flatbuffers/flexbuffers.h>

#include <optional>
#include <string>

#include "fct/Ref.hpp"
#include "flexbuffers/OutputVar.hpp"

namespace flexbuffers {

class OutputNull : public OutputVar {
 public:
  OutputNull() {}

  ~OutputNull() = default;

  /// Inserts all elements into the builder.
  void insert(const std::optional<std::string>& _key,
              flexbuffers::Builder* _fbb) final {
    if (_key) {
      _fbb->Null(_key->c_str());
    } else {
      _fbb->Null();
    }
  }

  /// Whether this is null.
  bool is_null() const final { return true; }
};

}  // namespace flexbuffers

#endif

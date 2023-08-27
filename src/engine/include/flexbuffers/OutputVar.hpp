// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_OUTPUTVAR_HPP_
#define FLEXBUFFERS_OUTPUTVAR_HPP_

#include <flatbuffers/flexbuffers.h>

#include <optional>
#include <string>

namespace flexbuffers {

class OutputVar {
 public:
  /// Inserts the value into the buffer.
  virtual void insert(const std::optional<std::string>& _key,
                      flexbuffers::Builder* _fbb) = 0;

  /// Whether this is null.
  virtual bool is_null() const = 0;
};

};  // namespace flexbuffers

#endif

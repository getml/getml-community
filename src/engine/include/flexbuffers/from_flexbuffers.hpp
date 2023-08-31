// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_
#define FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_

#include <flatbuffers/flexbuffers.h>

#include <cstddef>
#include <vector>

#include "flexbuffers/Parser.hpp"

namespace flexbuffers {

using InputVarType = typename Reader::InputVarType;

/// Parses an object from flexbuffers using reflection.
template <class T>
T from_flexbuffers(const std::byte* _bytes, const size_t _size) {
  InputVarType root =
      flexbuffers::GetRoot(reinterpret_cast<const uint8_t*>(_bytes), _size);
  const auto r = Reader();
  const auto result = Parser<T>::read(r, &root);
  return result.value();
}

/// Parses an object from flexbuffers using reflection.
template <class T>
T from_flexbuffers(const std::vector<std::byte>& _bytes) {
  return from_flexbuffers<T>(_bytes.data(), _bytes.size());
}

}  // namespace flexbuffers

#endif  // FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_

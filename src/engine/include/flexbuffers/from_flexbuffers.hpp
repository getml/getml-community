// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_
#define FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_

#include <flatbuffers/flexbuffers.h>

#include <vector>

#include "flexbuffers/Parser.hpp"

namespace flexbuffers {

using InputObjectType = typename Reader::InputObjectType;
using InputVarType = typename Reader::InputVarType;

/// Parses an object from FLEXBUFFERS using reflection.
template <class T>
T from_flexbuffers(const std::vector<uint8_t>& _bytes) {
  InputVarType root = flexbuffers::GetRoot(_bytes.data(), _bytes.size());
  const auto r = Reader();
  const auto result = Parser<T>::read(r, &root);
  return result.value();
}

}  // namespace flexbuffers

#endif  // FLEXBUFFERS_FROM_FLEXBUFFERS_HPP_

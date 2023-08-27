// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_TO_FLEXBUFFERS_HPP_
#define FLEXBUFFERS_TO_FLEXBUFFERS_HPP_

#include <flatbuffers/flexbuffers.h>

#include <sstream>
#include <vector>

#include "flexbuffers/Parser.hpp"

namespace flexbuffers {

/// Parses an object to FLEXBUFFERS.
template <class T>
std::vector<uint8_t> to_flexbuffers(const T& _obj) {
  auto w = Writer();
  const auto flexbuffers_obj = Parser<T>::to_flexbuffers(w, _obj);
  flexbuffers::Builder fbb;
  flexbuffers->insert(std::nullopt, &fbb);
  fbb.Finish();
  return fbb.GetBuffer();
}

}  // namespace flexbuffers

#endif  // FLEXBUFFERS_PARSER_HPP_

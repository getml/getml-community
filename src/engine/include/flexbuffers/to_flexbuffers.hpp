// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_TO_FLEXBUFFERS_HPP_
#define FLEXBUFFERS_TO_FLEXBUFFERS_HPP_

#include <flatbuffers/flexbuffers.h>

#include <cstddef>
#include <sstream>
#include <vector>

#include "flexbuffers/Parser.hpp"

namespace flexbuffers {

/// Writes an object to flexbuffers.
template <class T>
std::vector<unsigned char> to_flexbuffers(const T& _obj) {
  auto w = Writer();
  const auto flexbuffers_obj = Parser<T>::write(w, _obj);
  flexbuffers::Builder fbb;
  flexbuffers_obj->insert(std::nullopt, &fbb);
  fbb.Finish();
  const auto vec = fbb.GetBuffer();
  const auto data = reinterpret_cast<const unsigned char*>(vec.data());
  return std::vector<unsigned char>(data, data + vec.size());
}

}  // namespace flexbuffers

#endif  // FLEXBUFFERS_PARSER_HPP_

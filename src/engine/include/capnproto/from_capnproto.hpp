// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_FROM_CAPNPROTO_HPP_
#define CAPNPROTO_FROM_CAPNPROTO_HPP_

#include <yycapnproto.h>

#include <string>

#include "capnproto/Parser.hpp"

namespace capnproto {

using InputObjectType = typename Reader::InputObjectType;
using InputVarType = typename Reader::InputVarType;

/// Parses an object from CAPNPROTO using reflection.
template <class T>
T from_capnproto(const std::string& _capnproto_str) {
  yycapnproto_doc* doc =
      yycapnproto_read(_capnproto_str.c_str(), _capnproto_str.size(), 0);
  InputVarType root = InputVarType(yycapnproto_doc_get_root(doc));
  const auto r = Reader();
  const auto result = Parser<T>::from_capnproto(r, &root);
  yycapnproto_doc_free(doc);
  return result.value();
}

}  // namespace capnproto

#endif  // CAPNPROTO_FROM_CAPNPROTO_HPP_

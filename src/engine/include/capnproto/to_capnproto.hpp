// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_TO_CAPNPROTO_HPP_
#define CAPNPROTO_TO_CAPNPROTO_HPP_

#include <sstream>
#include <string>

#include "capnproto/Parser.hpp"

namespace capnproto {

/// Parses an object to CAPNPROTO.
template <class T>
std::string to_capnproto(const T& _obj) {
  auto w = Writer(yycapnproto_mut_doc_new(NULL));
  const auto capnproto_obj = Parser<T>::to_capnproto(w, _obj);
  yycapnproto_mut_doc_set_root(w.doc_, capnproto_obj.val_);
  const char* capnproto_c_str = yycapnproto_mut_write(w.doc_, 0, NULL);
  const auto capnproto_str = std::string(capnproto_c_str);
  free((void*)capnproto_c_str);
  yycapnproto_mut_doc_free(w.doc_);
  return capnproto_str;
}

}  // namespace capnproto

#endif  // CAPNPROTO_PARSER_HPP_

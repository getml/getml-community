// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CAPNPROTO_PARSER_HPP_
#define CAPNPROTO_PARSER_HPP_

#include "capnproto/Reader.hpp"
#include "capnproto/Writer.hpp"
#include "parsing/Parser.hpp"

namespace capnproto {

template <class T>
using Parser = parsing::Parser<Reader, Writer, T>;

};  // namespace capnproto

#endif  // CAPNPROTO_PARSER_HPP_

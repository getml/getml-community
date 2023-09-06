// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FLEXBUFFERS_PARSER_HPP_
#define FLEXBUFFERS_PARSER_HPP_

#include "flexbuffers/Reader.hpp"
#include "flexbuffers/Writer.hpp"
#include "rfl/parsing/Parser.hpp"

namespace flexbuffers {

template <class T>
using Parser = rfl::parsing::Parser<Reader, Writer, T>;

};  // namespace flexbuffers

#endif  // FLEXBUFFERS_PARSER_HPP_

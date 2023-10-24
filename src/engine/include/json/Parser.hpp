// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef JSON_PARSER_HPP_
#define JSON_PARSER_HPP_

#include "json/Reader.hpp"
#include "json/Writer.hpp"
#include "parsing/Parser.hpp"

namespace json {

template <class T>
using Parser = parsing::Parser<Reader, Writer, T>;

};  // namespace json

#endif  // JSON_PARSER_HPP_

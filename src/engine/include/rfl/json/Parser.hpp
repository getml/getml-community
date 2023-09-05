// Copyright 2022 The SQLNet Company GmbH

//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef RFL_JSON_PARSER_HPP_
#define RFL_JSON_PARSER_HPP_

#include "rfl/json/Reader.hpp"
#include "rfl/json/Writer.hpp"
#include "rfl/parsing/Parser.hpp"

namespace rfl {
namespace json {

template <class T>
using Parser = parsing::Parser<Reader, Writer, T>;

}
}  // namespace rfl

#endif

// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/StringIterator.hpp"

namespace helpers {

StringIterator::StringIterator(
    const std::function<strings::String(size_t)> _func, const size_t _size)
    : func_(_func), size_(_size) {}

}  // namespace helpers

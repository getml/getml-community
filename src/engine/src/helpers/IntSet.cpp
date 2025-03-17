// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/IntSet.hpp"

namespace helpers {

IntSet::IntSet(const size_t _maximum_value)
    : already_included_(std::vector<bool>(_maximum_value, false)),
      maximum_value_(_maximum_value) {}

}  // namespace helpers

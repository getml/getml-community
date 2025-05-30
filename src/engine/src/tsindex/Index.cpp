// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "tsindex/Index.hpp"

namespace tsindex {

Index::Index(const IndexParams& _params) : impl_(InMemoryIndex(_params)) {}

}  // namespace tsindex

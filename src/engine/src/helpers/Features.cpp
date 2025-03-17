// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/Features.hpp"

namespace helpers {

Features::Features(const std::vector<Feature<Float, false>>& _vec)
    : vec_(_vec) {}

Features::Features(const size_t _nrows, const size_t _ncols,
                   std::optional<std::string> _temp_dir)
    : Features(make_vec(_nrows, _ncols, _temp_dir)) {}

}  // namespace helpers

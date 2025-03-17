// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/Encoding.hpp"

#include <memory>

namespace predictors {

Encoding::Encoding() : max_(1), min_(0) {}

Encoding::Encoding(const ReflectionType& _nt)
    : max_(_nt.get<f_max>()), min_(_nt.get<f_min>()) {}

void Encoding::fit(const IntFeature& _col) {
  min_ = *std::min_element(_col.begin(), _col.end());
  max_ = *std::max_element(_col.begin(), _col.end());
}

// -----------------------------------------------------------------------------

IntFeature Encoding::transform(
    const IntFeature& _col, const std::shared_ptr<memmap::Pool>& _pool) const {
  auto output =
      _pool ? IntFeature(
                  std::make_shared<memmap::Vector<Int>>(_pool, _col.size()))
            : IntFeature(std::make_shared<std::vector<Int>>(_col.size()));

  for (size_t i = 0; i < _col.size(); ++i) {
    if (_col[i] < min_ || _col[i] > max_) {
      output[i] = -1;
    } else {
      output[i] = _col[i] - min_;
    }
  }

  return output;
}

// -----------------------------------------------------------------------------
}  // namespace predictors

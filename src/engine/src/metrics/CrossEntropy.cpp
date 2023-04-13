// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/CrossEntropy.hpp"

#include <cmath>

namespace metrics {

typename CrossEntropy::ResultType CrossEntropy::score(const Features _yhat,
                                                      const Features _y) {
  impl_.set_data(_yhat, _y);

  std::vector<Float> cross_entropy(ncols());

  for (size_t i = 0; i < nrows(); ++i) {
    for (size_t j = 0; j < ncols(); ++j) {
      if (y(i, j) == 0.0) {
        cross_entropy[j] -= std::log(1.0 - yhat(i, j));
      } else if (y(i, j) == 1.0) {
        cross_entropy[j] -= std::log(yhat(i, j));
      } else {
        throw std::runtime_error(
            "Target must either be 0 or 1 for "
            "cross entropy score to work!");
      }
    }
  }

  Float nrows_float = static_cast<Float>(nrows());

  if (impl_.has_comm()) {
    impl_.reduce(std::plus<Float>(), &cross_entropy);

    impl_.reduce(std::plus<Float>(), &nrows_float);
  }

  for (size_t j = 0; j < ncols(); ++j) {
    cross_entropy[j] /= nrows_float;
  }

  for (size_t j = 0; j < ncols(); ++j) {
    if (std::isinf(cross_entropy[j]) || std::isnan(cross_entropy[j])) {
      cross_entropy[j] = -1.0;
    }
  }

  return fct::make_field<"cross_entropy_">(cross_entropy);
}

}  // namespace metrics

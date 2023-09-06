// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/MAE.hpp"

namespace metrics {

typename MAE::ResultType MAE::score(const Features _yhat, const Features _y) {
  impl_.set_data(_yhat, _y);

  std::vector<Float> mae(ncols());

  for (size_t i = 0; i < nrows(); ++i) {
    for (size_t j = 0; j < ncols(); ++j) {
      mae[j] += std::abs(y(i, j) - yhat(i, j));
    }
  }

  Float nrows_float = static_cast<Float>(nrows());

  if (impl_.has_comm()) {
    impl_.reduce(std::plus<Float>(), &mae);

    impl_.reduce(std::plus<Float>(), &nrows_float);
  }

  for (size_t j = 0; j < ncols(); ++j) {
    mae[j] /= nrows_float;
  }

  return rfl::make_field<"mae_">(mae);
}

}  // namespace metrics

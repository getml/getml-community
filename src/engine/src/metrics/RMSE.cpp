// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/RMSE.hpp"

namespace metrics {

typename RMSE::ResultType RMSE::score(const Features _yhat, const Features _y) {
  impl_.set_data(_yhat, _y);

  std::vector<Float> rmse(ncols());

  for (size_t i = 0; i < nrows(); ++i) {
    for (size_t j = 0; j < ncols(); ++j) {
      rmse[j] += (y(i, j) - yhat(i, j)) * (y(i, j) - yhat(i, j));
    }
  }

  Float nrows_float = static_cast<Float>(nrows());

  if (impl_.has_comm()) {
    impl_.reduce(std::plus<Float>(), &rmse);

    impl_.reduce(std::plus<Float>(), &nrows_float);
  }

  for (size_t j = 0; j < ncols(); ++j) {
    rmse[j] /= nrows_float;

    rmse[j] = std::sqrt(rmse[j]);
  }

  return fct::make_field<"rmse_">(rmse);
}

}  // namespace metrics

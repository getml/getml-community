#include "metrics/MAE.hpp"

namespace metrics {
// ----------------------------------------------------------------------------

Poco::JSON::Object MAE::score(const Features _yhat, const Features _y) {
  // -----------------------------------------

  impl_.set_data(_yhat, _y);

  // -----------------------------------------

  std::vector<Float> mae(ncols());

  // -----------------------------------------------------

  for (size_t i = 0; i < nrows(); ++i) {
    for (size_t j = 0; j < ncols(); ++j) {
      mae[j] += std::abs(y(i, j) - yhat(i, j));
    }
  }

  // -----------------------------------------------------

  Float nrows_float = static_cast<Float>(nrows());

  // -----------------------------------------------------

  if (impl_.has_comm()) {
    impl_.reduce(std::plus<Float>(), &mae);

    impl_.reduce(std::plus<Float>(), &nrows_float);
  }

  // -----------------------------------------------------

  for (size_t j = 0; j < ncols(); ++j) {
    mae[j] /= nrows_float;
  }

  // -----------------------------------------------------

  Poco::JSON::Object obj;

  obj.set("mae_", jsonutils::JSON::vector_to_array_ptr(mae));

  // -----------------------------------------------------

  return obj;

  // -----------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace metrics

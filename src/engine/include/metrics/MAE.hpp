#ifndef METRICS_MAE_HPP_
#define METRICS_MAE_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <cstdint>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "metrics/Features.hpp"
#include "metrics/Float.hpp"
#include "metrics/Metric.hpp"
#include "metrics/MetricImpl.hpp"

// ----------------------------------------------------------------------------

namespace metrics {
// ----------------------------------------------------------------------------

class MAE : public Metric {
 public:
  MAE() {}

  MAE(multithreading::Communicator* _comm) : impl_(_comm) {}

  ~MAE() = default;

  // ------------------------------------------------------------------------

  /// This calculates the loss based on the predictions _yhat
  /// and the targets _y.
  Poco::JSON::Object score(const Features _yhat, const Features _y) final;

  // ------------------------------------------------------------------------

 private:
  /// Trivial getter
  multithreading::Communicator& comm() { return impl_.comm(); }

  /// Trivial getter
  size_t ncols() const { return impl_.ncols(); }

  /// Trivial getter
  size_t nrows() const { return impl_.nrows(); }

  /// Trivial getter
  Float yhat(size_t _i, size_t _j) const { return impl_.yhat(_i, _j); }

  /// Trivial getter
  Float y(size_t _i, size_t _j) const { return impl_.y(_i, _j); }

  // ------------------------------------------------------------------------

 private:
  /// Contains all the relevant data.
  MetricImpl impl_;

  // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_MAE_HPP_

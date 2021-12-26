#ifndef MULTIREL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_
#define MULTIREL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_

// ----------------------------------------------------------------------------

#include <cmath>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"
#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "multirel/lossfunctions/LossFunction.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace lossfunctions {
// ----------------------------------------------------------------------------

class CrossEntropyLoss : public LossFunction {
 public:
  CrossEntropyLoss(multithreading::Communicator* _comm);

  ~CrossEntropyLoss() = default;

  // -----------------------------------------

  // This calculates the gradient of the loss function w.r.t.
  // the current prediction
  std::vector<std::vector<Float>> calculate_residuals(
      const std::vector<std::vector<Float>>& _yhat_old,
      const containers::DataFrameView& _y) final;

  // This calculates the optimal update rate at which we need
  // to add _yhat to _yhat_old
  std::vector<Float> calculate_update_rates(
      const std::vector<std::vector<Float>>& _yhat_old,
      const std::vector<std::vector<Float>>& _predictions,
      const containers::DataFrameView& _y,
      const std::vector<Float>& _sample_weights) final;

  // -----------------------------------------

 public:
  std::string type() const final { return "CrossEntropyLoss"; }

  // -----------------------------------------

 private:
  /// Applies the logistic function.
  Float logistic_function(const Float& _val) {
    Float result = 1.0 / (1.0 + exp((-1.0) * _val));

    if (std::isnan(result) || std::isinf(result)) {
      if (_val > 0.0) {
        result = 1.0;
      } else {
        result = 0.0;
      }
    }

    return result;
  }

  // -----------------------------------------

 private:
  // Communicator object
  multithreading::Communicator* comm_;

  // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace multirel

#endif  // MULTIREL_LOSSFUNCTIONS_CROSSENTROPYLOSS_HPP_

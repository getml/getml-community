#ifndef MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_
#define MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_

// ----------------------------------------------------------------------------

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

class SquareLoss : public LossFunction {
 public:
  SquareLoss(multithreading::Communicator* _comm);

  ~SquareLoss() = default;

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

  std::string type() const final { return "SquareLoss"; }

  // -----------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace multirel

#endif  // MULTIREL_LOSSFUNCTIONS_SQUARELOSS_HPP_

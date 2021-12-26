#include "relmt/lossfunctions/SquareLoss.hpp"

namespace relmt {
namespace lossfunctions {
// ----------------------------------------------------------------------------

void SquareLoss::calc_gradients() {
  // ------------------------------------------------------------------------

  assert_true(yhat_old().size() == targets().size());

  // ------------------------------------------------------------------------
  // Resize, if necessary

  if (g_.size() != yhat_old().size()) {
    resize(yhat_old().size(), 1);
  }

  // ------------------------------------------------------------------------
  // Calculate g_.

  std::transform(yhat_old().begin(), yhat_old().end(), targets().begin(),
                 g_.begin(),
                 [](const Float& yhat, const Float& y) { return yhat - y; });

  // ------------------------------------------------------------------------
  // Calculate h_.

  std::fill(h_.begin(), h_.end(), 1.0);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relmt

#ifndef RELMT_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
#define RELMT_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Hyperparameters.hpp"

// ----------------------------------------------------------------------------

#include "relmt/lossfunctions/LossFunction.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace lossfunctions {
// ------------------------------------------------------------------------

struct LossFunctionParser {
  /// Returns the loss function associated with the type
  static std::shared_ptr<LossFunction> parse(
      const std::string& _type,
      const std::shared_ptr<const Hyperparameters>& _hyperparameters,
      const std::shared_ptr<std::vector<Float>> _targets);
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

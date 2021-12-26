#include "relboost/lossfunctions/LossFunctionParser.hpp"

// ----------------------------------------------------------------------------

#include "relboost/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "relboost/lossfunctions/CrossEntropyLoss.hpp"
#include "relboost/lossfunctions/SquareLoss.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace lossfunctions {
// ----------------------------------------------------------------------------

std::shared_ptr<LossFunction> LossFunctionParser::parse(
    const std::string& _type,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<std::vector<Float>> _targets) {
  if (_type == "CrossEntropyLoss") {
    return std::make_shared<CrossEntropyLoss>(_hyperparameters, _targets);
  } else if (_type == "SquareLoss") {
    return std::make_shared<SquareLoss>(_hyperparameters, _targets);
  } else {
    throw std::runtime_error("Unknown loss function: '" + _type + "' !");
    return std::shared_ptr<LossFunction>();
  }
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

#ifndef MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
#define MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "multirel/lossfunctions/LossFunction.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace lossfunctions {

struct LossFunctionParser {
  /// Turns a string describing the loss function into a proper loss function
  static std::shared_ptr<LossFunction> parse_loss_function(
      const std::string &_loss_function, multithreading::Communicator *_comm);
};

}  // namespace lossfunctions
}  // namespace multirel

#endif  // MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

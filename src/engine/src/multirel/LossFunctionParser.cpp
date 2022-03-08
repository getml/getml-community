#include "multirel/lossfunctions/LossFunctionParser.hpp"

#include "multirel/lossfunctions/CrossEntropyLoss.hpp"
#include "multirel/lossfunctions/SquareLoss.hpp"

namespace multirel {
namespace lossfunctions {

std::shared_ptr<LossFunction> LossFunctionParser::parse_loss_function(
    const std::string &_loss_function, multithreading::Communicator *_comm) {
  if (_loss_function == "CrossEntropyLoss") {
    return std::make_shared<CrossEntropyLoss>(_comm);
  } else if (_loss_function == "SquareLoss") {
    return std::make_shared<SquareLoss>(_comm);
  } else {
    std::string warning_message = "Loss Function of type '";
    warning_message.append(_loss_function);
    warning_message.append("' not known!");

    throw std::runtime_error(warning_message);
  }
}

}  // namespace lossfunctions
}  // namespace multirel

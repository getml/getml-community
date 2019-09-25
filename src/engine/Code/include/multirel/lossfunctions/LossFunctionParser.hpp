#ifndef MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
#define MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

namespace multirel
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

struct LossFunctionParser
{
    /// Turns a string describing the loss function into a proper loss function
    static std::shared_ptr<LossFunction> parse_loss_function(
        const std::string &_loss_function,
        multithreading::Communicator *_comm );
};

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace multirel

#endif  // MULTIREL_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

#ifndef RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
#define RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

// ----------------------------------------------------------------------------

namespace relcit
{
namespace lossfunctions
{
// ------------------------------------------------------------------------

struct LossFunctionParser
{
    /// Returns the loss function associated with the type
    static std::shared_ptr<LossFunction> parse(
        const std::string& _type,
        const std::shared_ptr<const Hyperparameters>& _hyperparameters,
        const std::shared_ptr<std::vector<Float>> _targets );
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relcit

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

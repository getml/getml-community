#ifndef RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
#define RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
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
        const std::shared_ptr<std::vector<RELBOOST_FLOAT>> _targets );
};

// ------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_LOSSFUNCTIONS_LOSSFUNCTIONPARSER_HPP_
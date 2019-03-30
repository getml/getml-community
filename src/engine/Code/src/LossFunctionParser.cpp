#include "lossfunctions/lossfunctions.hpp"

namespace relboost
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

std::shared_ptr<LossFunction> LossFunctionParser::parse(
    const std::string& _type,
    const std::shared_ptr<const Hyperparameters>& _hyperparameters,
    const std::shared_ptr<std::vector<RELBOOST_FLOAT>> _targets )
{
    if ( _type == "SquareLoss" )
        {
            return std::make_shared<SquareLoss>(
                _hyperparameters, _targets );
        }
    else
        {
            assert( false );
            return std::shared_ptr<LossFunction>();
        }
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relboost

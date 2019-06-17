#include "autosql/lossfunctions/lossfunctions.hpp"

namespace autosql
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

std::shared_ptr<LossFunction> LossFunctionParser::parse_loss_function(
    const std::string &_loss_function, multithreading::Communicator *comm_ )
{
    if ( _loss_function == "CrossEntropyLoss" )
        {
            return std::make_shared<CrossEntropyLoss>();
        }
    else if ( _loss_function == "SquareLoss" )
        {
            return std::make_shared<SquareLoss>();
        }
    else
        {
            std::string warning_message = "Loss Function of type '";
            warning_message.append( _loss_function );
            warning_message.append( "' not known!" );

            throw std::invalid_argument( warning_message );
        }
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace autosql
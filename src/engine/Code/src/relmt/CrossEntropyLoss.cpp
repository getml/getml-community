#include "relmt/lossfunctions/lossfunctions.hpp"

namespace relmt
{
namespace lossfunctions
{
// ----------------------------------------------------------------------------

void CrossEntropyLoss::calc_gradients()
{
    // ------------------------------------------------------------------------

    assert_true( yhat_old().size() == targets().size() );

    // ------------------------------------------------------------------------
    // Resize, if necessary

    if ( g_.size() != yhat_old().size() )
        {
            resize( yhat_old().size(), 1 );
        }

    // ------------------------------------------------------------------------
    // Calculate g_.

    std::transform(
        yhat_old().begin(),
        yhat_old().end(),
        targets().begin(),
        g_.begin(),
        [this]( const Float& yhat, const Float& y ) {
            return logistic_function( yhat ) - y;
        } );

    // ------------------------------------------------------------------------
    // Calculate h_.

    std::transform(
        yhat_old().begin(),
        yhat_old().end(),
        h_.begin(),
        [this]( const Float& yhat ) {
            const auto sigma_yhat = logistic_function( yhat );
            return sigma_yhat * ( 1.0 - sigma_yhat );
        } );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace lossfunctions
}  // namespace relmt

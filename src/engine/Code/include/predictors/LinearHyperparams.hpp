#ifndef PREDICTORS_LINEARHYPERPARMS_HPP_
#define PREDICTORS_LINEARHYPERPARMS_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Hyperparameters for Linear models.
struct LinearHyperparams
{
    // -----------------------------------------

    LinearHyperparams( const Float &_lambda, const Float &_learning_rate )
        : lambda_( _lambda ), learning_rate_( _learning_rate )
    {
    }

    LinearHyperparams( const Poco::JSON::Object &_json_obj )
        : lambda_( JSON::get_value<Float>( _json_obj, "lambda_" ) ),
          learning_rate_(
              JSON::get_value<Float>( _json_obj, "learning_rate_" ) )
    {
    }

    ~LinearHyperparams() = default;

    // -----------------------------------------

    /// L2 regularization term on weights
    const Float lambda_;

    /// Learning rate used for the updates
    const Float learning_rate_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARHYPERPARMS_HPP_

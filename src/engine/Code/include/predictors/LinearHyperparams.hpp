#ifndef PREDICTORS_LINEARHYPERPARMS_HPP_
#define PREDICTORS_LINEARHYPERPARMS_HPP_

namespace predictors
{
// ----------------------------------------------------------------------

/// Hyperparameters for Linear models.
struct LinearHyperparams
{
    // -----------------------------------------

    LinearHyperparams( const Float &_lambda ) : lambda_( _lambda ) {}

    LinearHyperparams( const Poco::JSON::Object &_json_obj )
        : lambda_( JSON::get_value<Float>( _json_obj, "lambda_" ) )
    {
    }

    ~LinearHyperparams() = default;

    // -----------------------------------------

    /// L2 regularization term on weights
    const Float lambda_;
};

// ------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_LINEARHYPERPARMS_HPP_

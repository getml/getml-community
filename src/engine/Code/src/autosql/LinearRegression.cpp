#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

void LinearRegression::fit(
    const std::vector<Float>& _new_feature,
    const std::vector<std::vector<Float>>& _residuals,
    const std::vector<Float>& _sample_weights )
{
    debug_log( "Fitting linear regression..." );

    // ----------------------------------------------------
    // Calculate sum_sample weights

    Float sum_sample_weights =
        std::accumulate( _sample_weights.begin(), _sample_weights.end(), 0.0 );

    utils::Reducer::reduce(
        std::plus<Float>(), &sum_sample_weights, comm_ );

    // ----------------------------------------------------
    // Calculate mean_new_feature

    assert_true( _new_feature.size() == _sample_weights.size() );

    Float mean_new_feature = 0.0;

    for ( size_t i = 0; i < _new_feature.size(); ++i )
        {
            mean_new_feature += _new_feature[i] * _sample_weights[i];
        }

    utils::Reducer::reduce(
        std::plus<Float>(), &mean_new_feature, comm_ );

    mean_new_feature /= sum_sample_weights;

    // ----------------------------------------------------
    // Calculate mean_residuals

    auto mean_residuals = std::vector<Float>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            assert_true( _residuals[i].size() == _sample_weights.size() );

            for ( size_t j = 0; j < _residuals[i].size(); ++j )
                {
                    mean_residuals[i] += _residuals[i][j] * _sample_weights[j];
                }
        }

    utils::Reducer::reduce(
        std::plus<Float>(), &mean_residuals, comm_ );

    std::for_each(
        mean_residuals.begin(),
        mean_residuals.end(),
        [sum_sample_weights]( Float& m ) { m /= sum_sample_weights; } );

    // ----------------------------------------------------
    // Calculate var_new_feature

    assert_true( _new_feature.size() == _sample_weights.size() );

    Float var_new_feature = 0.0;

    for ( size_t i = 0; i < _new_feature.size(); ++i )
        {
            var_new_feature += ( _new_feature[i] - mean_new_feature ) *
                               ( _new_feature[i] - mean_new_feature ) *
                               _sample_weights[i];
        }

    utils::Reducer::reduce(
        std::plus<Float>(), &var_new_feature, comm_ );

    var_new_feature /= sum_sample_weights;

    // ----------------------------------------------------
    // Calculate cov_new_feature

    auto cov_new_feature = std::vector<Float>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            for ( size_t j = 0; j < _residuals[i].size(); ++j )
                {
                    cov_new_feature[i] +=
                        ( _residuals[i][j] - mean_residuals[i] ) *
                        ( _new_feature[j] - mean_new_feature ) *
                        _sample_weights[j];
                }
        }

    utils::Reducer::reduce(
        std::plus<Float>(), &cov_new_feature, comm_ );

    std::for_each(
        cov_new_feature.begin(),
        cov_new_feature.end(),
        [sum_sample_weights]( Float& c ) { c /= sum_sample_weights; } );

    // ------------------
    // Calculate slopes_

    slopes_ = std::vector<Float>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            slopes_[i] = cov_new_feature[i] / var_new_feature;
        }

    // ---------------------
    // Calculate intercepts_

    intercepts_ = std::vector<Float>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            intercepts_[i] = mean_residuals[i] - slopes_[i] * mean_new_feature;
        }

    // ---------------------
    // Make sure that slopes_ and intercepts_ are in range

    auto in_range = []( Float& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( slopes_.begin(), slopes_.end(), in_range );

    std::for_each( intercepts_.begin(), intercepts_.end(), in_range );

    // ---------------------
}

// ----------------------------------------------------------------------------

LinearRegression LinearRegression::from_json_obj(
    const Poco::JSON::Object& _obj, multithreading::Communicator* _comm ) const
{
    // ----------------------------------------

    LinearRegression linear_regression( _comm );

    // ----------------------------------------

    linear_regression.slopes_ = JSON::array_to_vector<Float>(
        JSON::get_array( _obj, "update_rates_1_" ) );

    linear_regression.intercepts_ = JSON::array_to_vector<Float>(
        JSON::get_array( _obj, "update_rates_2_" ) );

    // ----------------------------------------

    return linear_regression;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>> LinearRegression::predict(
    const std::vector<Float>& _yhat ) const
{
    debug_log( "Predicting using linear regression..." );

    std::vector<std::vector<Float>> predictions( intercepts_.size() );

    for ( Int i = 0; i < predictions.size(); ++i )
        {
            std::vector<Float> new_prediction( _yhat.size() );

            for ( size_t j = 0; j < _yhat.size(); ++j )
                {
                    new_prediction[j] = slopes_[i] * _yhat[j] + intercepts_[i];
                }

            predictions[i] = std::move( new_prediction );
        }

    return predictions;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object LinearRegression::to_json_obj() const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "update_rates_1_", JSON::vector_to_array( slopes_ ) );

    obj.set( "update_rates_2_", JSON::vector_to_array( intercepts_ ) );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

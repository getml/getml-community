#include "autosql/utils/utils.hpp"

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

LinearRegression::LinearRegression(){};

// ----------------------------------------------------------------------------

LinearRegression::LinearRegression( size_t _ncols )
    : intercepts_( std::vector<AUTOSQL_FLOAT>( _ncols ) ),
      slopes_( std::vector<AUTOSQL_FLOAT>( _ncols ) ){};

// ----------------------------------------------------------------------------

void LinearRegression::apply_shrinkage( const AUTOSQL_FLOAT _shrinkage )
{
    // ----------------------------------------------------------------
    // Multiply by the shrinkage

    auto multiply_by_shrinkage = [_shrinkage]( AUTOSQL_FLOAT& val ) {
        val *= _shrinkage;
    };

    std::for_each( slopes_.begin(), slopes_.end(), multiply_by_shrinkage );

    std::for_each(
        intercepts_.begin(), intercepts_.end(), multiply_by_shrinkage );

    // ----------------------------------------------------------------
    // Get rid of out-of-range-values

    auto in_range = []( AUTOSQL_FLOAT& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( slopes_.begin(), slopes_.end(), in_range );

    std::for_each( intercepts_.begin(), intercepts_.end(), in_range );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void LinearRegression::fit(
    const std::vector<AUTOSQL_FLOAT>& _new_feature,
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _residuals,
    const std::vector<AUTOSQL_FLOAT>& _sample_weights )
{
    debug_log( "Fitting linear regression..." );

    // ----------------------------------------------------
    // Calculate sum_sample weights

    AUTOSQL_FLOAT sum_sample_weights =
        std::accumulate( _sample_weights.begin(), _sample_weights.end(), 0.0 );

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &sum_sample_weights, comm_ );

    // ----------------------------------------------------
    // Calculate mean_new_feature

    assert( _new_feature.size() == _sample_weights.size() );

    AUTOSQL_FLOAT mean_new_feature = 0.0;

    for ( size_t i = 0; i < _new_feature.size(); ++i )
        {
            mean_new_feature += _new_feature[i] * _sample_weights[i];
        }

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &mean_new_feature, comm_ );

    mean_new_feature /= sum_sample_weights;

    // ----------------------------------------------------
    // Calculate mean_residuals

    auto mean_residuals = std::vector<AUTOSQL_FLOAT>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            assert( _residuals[i].size() == _sample_weights.size() );

            for ( size_t j = 0; j < _residuals[i].size(); ++j )
                {
                    mean_residuals[i] += _residuals[i][j] * _sample_weights[j];
                }
        }

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &mean_residuals, comm_ );

    std::for_each(
        mean_residuals.begin(),
        mean_residuals.end(),
        [sum_sample_weights]( AUTOSQL_FLOAT& m ) { m /= sum_sample_weights; } );

    // ----------------------------------------------------
    // Calculate var_new_feature

    assert( _new_feature.size() == _sample_weights.size() );

    AUTOSQL_FLOAT var_new_feature = 0.0;

    for ( size_t i = 0; i < _new_feature.size(); ++i )
        {
            var_new_feature += ( _new_feature[i] - mean_new_feature ) *
                               ( _new_feature[i] - mean_new_feature ) *
                               _sample_weights[i];
        }

    utils::Reducer::reduce(
        std::plus<AUTOSQL_FLOAT>(), &var_new_feature, comm_ );

    var_new_feature /= sum_sample_weights;

    // ----------------------------------------------------
    // Calculate cov_new_feature

    auto cov_new_feature = std::vector<AUTOSQL_FLOAT>( _residuals.size() );

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
        std::plus<AUTOSQL_FLOAT>(), &cov_new_feature, comm_ );

    std::for_each(
        cov_new_feature.begin(),
        cov_new_feature.end(),
        [sum_sample_weights]( AUTOSQL_FLOAT& c ) { c /= sum_sample_weights; } );

    // ------------------
    // Calculate slopes_

    slopes_ = std::vector<AUTOSQL_FLOAT>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            slopes_[i] = cov_new_feature[i] / var_new_feature;
        }

    // ---------------------
    // Calculate intercepts_

    intercepts_ = std::vector<AUTOSQL_FLOAT>( _residuals.size() );

    for ( size_t i = 0; i < _residuals.size(); ++i )
        {
            intercepts_[i] = mean_residuals[i] - slopes_[i] * mean_new_feature;
        }

    // ---------------------
    // Make sure that slopes_ and intercepts_ are in range

    auto in_range = []( AUTOSQL_FLOAT& val ) {
        val = ( ( std::isnan( val ) || std::isinf( val ) ) ? 0.0 : val );
    };

    std::for_each( slopes_.begin(), slopes_.end(), in_range );

    std::for_each( intercepts_.begin(), intercepts_.end(), in_range );

    // ---------------------
}

// ----------------------------------------------------------------------------

std::vector<std::vector<AUTOSQL_FLOAT>> LinearRegression::predict(
    const std::vector<AUTOSQL_FLOAT>& _yhat ) const
{
    debug_log( "Predicting using linear regression..." );

    std::vector<std::vector<AUTOSQL_FLOAT>> predictions( intercepts_.size() );

    for ( AUTOSQL_INT i = 0; i < predictions.size(); ++i )
        {
            std::vector<AUTOSQL_FLOAT> new_prediction( _yhat.size() );

            for ( size_t j = 0; j < _yhat.size(); ++j )
                {
                    new_prediction[j] = slopes_[i] * _yhat[j] + intercepts_[i];
                }

            predictions[i] = std::move( new_prediction );
        }

    return predictions;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

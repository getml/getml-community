#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::string LinearRegression::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CFloatColumn>& _X,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------

    _logger->log( "Training LinearRegression arithmetically..." );

    solve_arithmetically( _X, _y );

    // -------------------------------------------------------------------------

    return "";

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LinearRegression::load( const std::string& _fname ) {}

// -----------------------------------------------------------------------------

CFloatColumn LinearRegression::predict(
    const std::vector<CFloatColumn>& _X ) const
{
    if ( weights_.size() == 0 )
        {
            throw std::runtime_error(
                "LinearRegression has not been trained!" );
        }

    if ( weights_.size() != _X.size() + 1 )
        {
            throw std::runtime_error(
                "Incorrect number of features! Expected " +
                std::to_string( weights_.size() - 1 ) + ", got " +
                std::to_string( _X.size() ) + "." );
        }

    auto predictions = CFloatColumn();

    for ( size_t j = 0; j < _X.size(); ++j )
        {
            assert( _X[0]->size() == _X[j]->size() );

            if ( predictions->size() != _X[j]->size() )
                {
                    predictions =
                        std::make_shared<std::vector<Float>>( _X[j]->size() );
                }

            for ( size_t i = 0; i < _X[j]->size(); ++i )
                {
                    ( *predictions )[i] += weights_[j] * ( *_X[j] )[i];
                }
        }

    for ( size_t i = 0; i < predictions->size(); ++i )
        {
            ( *predictions )[i] += weights_[_X.size()];
        }

    return predictions;
}

// -----------------------------------------------------------------------------

void LinearRegression::save( const std::string& _fname ) const {}

// -----------------------------------------------------------------------------

void LinearRegression::solve_arithmetically(
    const std::vector<CFloatColumn>& _X, const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------
    // Calculate XtX

    auto XtX = Eigen::Matrix<Float, Eigen::Dynamic, Eigen::Dynamic>(
        _X.size() + 1, _X.size() + 1 );

    for ( size_t i = 0; i < _X.size(); ++i )
        {
            assert( _X[i] );

            for ( size_t j = 0; j <= i; ++j )
                {
                    assert( _X[i]->size() == _X[j]->size() );

                    XtX( i, j ) = XtX( j, i ) = std::inner_product(
                        _X[i]->begin(), _X[i]->end(), _X[j]->begin(), 0.0 );
                }
        }

    for ( size_t i = 0; i < _X.size(); ++i )
        {
            XtX( _X.size(), i ) = XtX( i, _X.size() ) =
                std::accumulate( _X[i]->begin(), _X[i]->end(), 0.0 );
        }

    // -------------------------------------------------------------------------
    // Calculate Xy

    auto Xy = Eigen::Matrix<Float, Eigen::Dynamic, 1>( _X.size() + 1 );

    assert( _y );

    for ( size_t i = 0; i < _X.size(); ++i )
        {
            assert( _X[i]->size() == _y->size() );

            Xy( i ) = std::inner_product(
                _X[i]->begin(), _X[i]->end(), _y->begin(), 0.0 );
        }

    Xy( _X.size() ) = std::accumulate( _y->begin(), _y->end(), 0.0 );

    // -------------------------------------------------------------------------
    // Calculate weights_

    auto weights = XtX.fullPivLu().solve( Xy );

    weights_.resize( _X.size() + 1 );

    for ( size_t i = 0; i < weights_.size(); ++i )
        {
            weights_[i] = weights( i );
        }

    // -------------------------------------------------------------------------
    // Calculate feature_importances_

    // ...

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::string LinearRegression::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical, _y );

    // -------------------------------------------------------------------------

    if ( _X_categorical.size() > 0 )
        {
            if ( _logger )
                {
                    _logger->log( "Training LinearRegression numerically..." );
                }

            solve_numerically( _X_categorical, _X_numerical, _y );
        }
    else
        {
            if ( _logger )
                {
                    _logger->log(
                        "Training LinearRegression arithmetically..." );
                }

            solve_arithmetically( _X_numerical, _y );
        }

    // -------------------------------------------------------------------------

    return "";

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LinearRegression::load( const std::string& _fname )
{
    auto obj = load_json_obj( _fname + ".json" );

    auto arr = obj.getArray( "weights_" );

    if ( !arr )
        {
            throw std::runtime_error(
                _fname + " contains no Array named 'weights'!" );
        }

    weights_.clear();

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            weights_.push_back( arr->getElement<Float>( i ) );
        }
}

// -----------------------------------------------------------------------------

Poco::JSON::Object LinearRegression::load_json_obj(
    const std::string& _fname ) const
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    return *Poco::JSON::Parser()
                .parse( json.str() )
                .extract<Poco::JSON::Object::Ptr>();
}

// -----------------------------------------------------------------------------

CFloatColumn LinearRegression::predict(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    if ( weights_.size() == 0 )
        {
            throw std::runtime_error(
                "LinearRegression has not been trained!" );
        }

    impl().check_plausibility( _X_categorical, _X_numerical );

    if ( _X_categorical.size() > 0 )
        {
            return predict_sparse( _X_categorical, _X_numerical );
        }
    else
        {
            return predict_dense( _X_numerical );
        }
}

// -----------------------------------------------------------------------------

CFloatColumn LinearRegression::predict_dense(
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    // -------------------------------------------------------------------------
    // Make sure that the _X_numerical is plausible.

    if ( weights_.size() != _X_numerical.size() + 1 )
        {
            throw std::runtime_error(
                "Incorrect number of features! Expected " +
                std::to_string( weights_.size() - 1 ) + ", got " +
                std::to_string( _X_numerical.size() ) + "." );
        }

    // -------------------------------------------------------------------------
    // Calculate dot product with weights.

    auto predictions = CFloatColumn();

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            assert( _X_numerical[0]->size() == _X_numerical[j]->size() );

            if ( !predictions )
                {
                    predictions = std::make_shared<std::vector<Float>>(
                        _X_numerical[0]->size() );
                }

            for ( size_t i = 0; i < _X_numerical[j]->size(); ++i )
                {
                    ( *predictions )[i] +=
                        weights_[j] * ( *_X_numerical[j] )[i];
                }
        }

    // -------------------------------------------------------------------------
    // Add intercept.

    for ( size_t i = 0; i < predictions->size(); ++i )
        {
            ( *predictions )[i] += weights_[_X_numerical.size()];
        }

    // -------------------------------------------------------------------------

    return predictions;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

CFloatColumn LinearRegression::predict_sparse(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    // -------------------------------------------------------------------------
    // Build up CSRMatrix.

    const auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(
        _X_categorical, _X_numerical );

    // -------------------------------------------------------------------------
    // Make sure that the CSRMatrix is plausible.

    if ( weights_.size() != csr_mat.ncols() + 1 )
        {
            throw std::runtime_error(
                "Incorrect number of columns in CSRMatrix! Expected " +
                std::to_string( weights_.size() - 1 ) + ", got " +
                std::to_string( csr_mat.ncols() ) + "." );
        }

    // -------------------------------------------------------------------------
    // Initialize predictions.

    auto predictions = std::make_shared<std::vector<Float>>( csr_mat.nrows() );

    // -------------------------------------------------------------------------
    // Generate predictions.

    for ( size_t i = 0; i < csr_mat.nrows(); ++i )
        {
            ( *predictions )[i] = predict_sparse(
                csr_mat.indptr()[i],
                csr_mat.indptr()[i + 1],
                csr_mat.indices(),
                csr_mat.data() );
        }

    // -------------------------------------------------------------------------

    return predictions;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LinearRegression::save( const std::string& _fname ) const
{
    Poco::JSON::Object obj;

    obj.set( "weights_", weights_ );

    auto output = std::ofstream( _fname + ".json" );

    Poco::JSON::Stringifier::stringify( obj, output );
}

// -----------------------------------------------------------------------------

void LinearRegression::solve_arithmetically(
    const std::vector<CFloatColumn>& _X, const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------
    // Calculate XtX

    // CAREFUL: Do NOT use "auto"!
    Eigen::MatrixXd XtX = Eigen::MatrixXd( _X.size() + 1, _X.size() + 1 );

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

    XtX( _X.size(), _X.size() ) = static_cast<Float>( _y->size() );

    // -------------------------------------------------------------------------
    // Calculate Xy

    // CAREFUL: Do NOT use "auto"!
    Eigen::MatrixXd Xy = Eigen::MatrixXd( _X.size() + 1, 1 );

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

    // CAREFUL: Do NOT use "auto"!
    Eigen::MatrixXd weights = XtX.fullPivLu().solve( Xy );

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

void LinearRegression::solve_numerically(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------
    // Build up CSRMatrix.

    const auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(
        _X_categorical, _X_numerical );

    // -------------------------------------------------------------------------
    // Init weights.

    std::mt19937 rng;

    std::uniform_real_distribution<> dis( -1.0, 1.0 );

    weights_ = std::vector<Float>( csr_mat.ncols() + 1 );

    for ( auto& w : weights_ )
        {
            w = dis( rng );
        }

    // -------------------------------------------------------------------------
    // Use the SGD algorithm to find the weights.

    std::vector<Float> gradients( weights_.size() );

    auto optimizer =
        optimizers::Adam( 0.9, 0.999, 10.0, 1e-10, weights_.size() );

    for ( size_t epoch = 0; epoch < 1000; ++epoch )
        {
            const auto epoch_float = static_cast<Float>( epoch );

            for ( size_t i = 0; i < csr_mat.nrows(); ++i )
                {
                    const auto yhat = predict_sparse(
                        csr_mat.indptr()[i],
                        csr_mat.indptr()[i + 1],
                        csr_mat.indices(),
                        csr_mat.data() );

                    const auto delta = yhat - ( *_y )[i];

                    calculate_gradients(
                        csr_mat.indptr()[i],
                        csr_mat.indptr()[i + 1],
                        csr_mat.indices(),
                        csr_mat.data(),
                        delta,
                        &gradients );

                    if ( i % 200 == 199 || i == csr_mat.nrows() - 1 )
                        {
                            optimizer.update_weights(
                                epoch_float, gradients, &weights_ );

                            std::fill(
                                gradients.begin(), gradients.end(), 0.0 );
                        }
                }
        }

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

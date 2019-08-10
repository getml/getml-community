#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::vector<Float> LogisticRegression::feature_importances(
    const size_t _num_features ) const
{
    if ( weights_.size() == 0 )
        {
            throw std::invalid_argument(
                "Cannot retrieve feature importances! Linear Regression has "
                "not been trained!" );
        }

    if ( _num_features != weights_.size() - 1 )
        {
            throw std::invalid_argument(
                "Incorrect number of features when retrieving in feature "
                "importances! Expected " +
                std::to_string( weights_.size() - 1 ) + ", got " +
                std::to_string( _num_features ) + "." );
        }

    auto feature_importances = std::vector<Float>( _num_features );

    for ( size_t i = 0; i < feature_importances.size(); ++i )
        {
            feature_importances[i] = std::abs( weights_[i] );
        }

    const auto sum = std::accumulate(
        feature_importances.begin(), feature_importances.end(), 0.0 );

    for ( auto& f : feature_importances )
        {
            f /= sum;
        }

    return feature_importances;
}

// -----------------------------------------------------------------------------

std::string LogisticRegression::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical, _y );

    // -------------------------------------------------------------------------

    if ( _X_categorical.size() == 0 )
        {
            fit_dense( _logger, _X_numerical, _y );
        }
    else
        {
            fit_sparse( _logger, _X_categorical, _X_numerical, _y );
        }

    // -------------------------------------------------------------------------

    return "";

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LogisticRegression::fit_dense(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------

    if ( _logger )
        {
            _logger->log(
                "Training the logistic regression using the "
                "Broyden–Fletcher–Goldfarb–Shanno (BFGS) algorithm..." );
        }

    // -------------------------------------------------------------------------
    // Rescale training set.

    scaler_.fit( _X_numerical );

    const auto X = scaler_.transform( _X_numerical );

    // -------------------------------------------------------------------------
    // Init weights.

    std::mt19937 rng;

    std::uniform_real_distribution<> dis( -1.0, 1.0 );

    weights_ = std::vector<Float>( X.size() + 1 );

    for ( auto& w : weights_ )
        {
            w = dis( rng );
        }

    // -------------------------------------------------------------------------
    // Set some standard variables.

    const auto nrows = X[0]->size();

    const auto nrows_float = static_cast<Float>( nrows );

    // -------------------------------------------------------------------------
    // Use BFGS to find the weights.

    std::vector<Float> gradients( weights_.size() );

    auto optimizer = optimizers::BFGS( 1.0, weights_.size() );

    bool has_converged = false;

    for ( size_t epoch = 0; epoch < 1000; ++epoch )
        {
            const auto epoch_float = static_cast<Float>( epoch );

            for ( size_t i = 0; i < nrows; ++i )
                {
                    const auto yhat = predict_dense( X, i );

                    const auto delta = yhat - ( *_y )[i];

                    calculate_gradients( X, i, delta, &gradients );

                    if ( i == nrows - 1 )
                        {
                            for ( auto& g : gradients )
                                {
                                    g /= nrows_float;
                                }

                            calculate_regularization( 1.0, &gradients );

                            const auto gradients_rmse =
                                std::sqrt( std::inner_product(
                                    gradients.begin(),
                                    gradients.end(),
                                    gradients.begin(),
                                    0.0 ) );

                            if ( gradients_rmse < 1e-04 )
                                {
                                    has_converged = true;
                                    break;
                                }

                            optimizer.update_weights(
                                epoch_float, gradients, &weights_ );

                            std::fill(
                                gradients.begin(), gradients.end(), 0.0 );
                        }
                }

            if ( has_converged )
                {
                    if ( _logger )
                        {
                            _logger->log(
                                "Converged after " + std::to_string( epoch ) +
                                " updates." );
                        }

                    break;
                }
        }

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LogisticRegression::fit_sparse(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical,
    const CFloatColumn& _y )
{
    // -------------------------------------------------------------------------

    if ( _logger )
        {
            _logger->log(
                "Training the logistic regression using the Adaptive Moments "
                "(Adam) algorithm..." );
        }

    // -------------------------------------------------------------------------
    // Build up CSRMatrix.

    auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(
        _X_categorical, _X_numerical );

    // -------------------------------------------------------------------------
    // Rescale CSRMatrix.

    scaler_.fit( csr_mat );

    csr_mat = scaler_.transform( csr_mat );

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
    // Set some standard variables.

    const size_t batch_size = 200;

    const auto bsize_float = static_cast<Float>( batch_size );

    // -------------------------------------------------------------------------
    // Use Adam to find the weights.

    std::vector<Float> gradients( weights_.size() );

    auto optimizer = optimizers::Adam(
        hyperparams().learning_rate_, 0.999, 10.0, 1e-10, weights_.size() );

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

                    if ( i % batch_size == batch_size - 1 ||
                         i == csr_mat.nrows() - 1 )
                        {
                            calculate_regularization( bsize_float, &gradients );

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

void LogisticRegression::load( const std::string& _fname )
{
    const auto obj = load_json_obj( _fname + ".json" );

    hyperparams_ = std::make_shared<LinearHyperparams>(
        JSON::get_value<Float>( obj, "lambda_" ),
        JSON::get_value<Float>( obj, "learning_rate_" ) );

    scaler_ = StandardScaler( *JSON::get_object( obj, "scaler_" ) );

    weights_ =
        JSON::array_to_vector<Float>( JSON::get_array( obj, "weights_" ) );
}

// -----------------------------------------------------------------------------

Poco::JSON::Object LogisticRegression::load_json_obj(
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

CFloatColumn LogisticRegression::predict(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    if ( weights_.size() == 0 )
        {
            throw std::runtime_error(
                "LinearRegression has not been trained!" );
        }

    impl().check_plausibility( _X_categorical, _X_numerical );

    if ( _X_categorical.size() == 0 )
        {
            return predict_dense( _X_numerical );
        }
    else
        {
            return predict_sparse( _X_categorical, _X_numerical );
        }
}

// -----------------------------------------------------------------------------

CFloatColumn LogisticRegression::predict_dense(
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    // -------------------------------------------------------------------------
    // Make sure that the X is plausible.

    if ( weights_.size() != _X_numerical.size() + 1 )
        {
            throw std::runtime_error(
                "Incorrect number of features! Expected " +
                std::to_string( weights_.size() - 1 ) + ", got " +
                std::to_string( _X_numerical.size() ) + "." );
        }

    // -------------------------------------------------------------------------
    // Rescale input

    const auto X = scaler_.transform( _X_numerical );

    // -------------------------------------------------------------------------
    // Calculate dot product with weights.

    auto predictions = CFloatColumn();

    for ( size_t j = 0; j < X.size(); ++j )
        {
            assert( X[0]->size() == X[j]->size() );

            if ( !predictions )
                {
                    predictions =
                        std::make_shared<std::vector<Float>>( X[0]->size() );
                }

            for ( size_t i = 0; i < X[j]->size(); ++i )
                {
                    ( *predictions )[i] += weights_[j] * ( *X[j] )[i];
                }
        }

    // -------------------------------------------------------------------------
    // Add intercept and apply logistic_function

    for ( auto& yhat : *predictions )
        {
            yhat += weights_.back();

            yhat = logistic_function( yhat );
        }

    // -------------------------------------------------------------------------

    return predictions;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

CFloatColumn LogisticRegression::predict_sparse(
    const std::vector<CIntColumn>& _X_categorical,
    const std::vector<CFloatColumn>& _X_numerical ) const
{
    // -------------------------------------------------------------------------
    // Build up CSRMatrix.

    auto csr_mat = impl().make_csr<Float, unsigned int, size_t>(
        _X_categorical, _X_numerical );

    // -------------------------------------------------------------------------
    // Rescale CSRMatrix.

    csr_mat = scaler_.transform( csr_mat );

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

void LogisticRegression::save( const std::string& _fname ) const
{
    Poco::JSON::Object obj;

    obj.set( "lambda_", hyperparams().lambda_ );

    obj.set( "learning_rate_", hyperparams().learning_rate_ );

    obj.set( "scaler_", scaler_.to_json_obj() );

    obj.set( "weights_", weights_ );

    auto output = std::ofstream( _fname + ".json" );

    Poco::JSON::Stringifier::stringify( obj, output );
}

// -----------------------------------------------------------------------------
}  // namespace predictors

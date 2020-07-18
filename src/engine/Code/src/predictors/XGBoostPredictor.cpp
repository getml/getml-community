#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::unique_ptr<BoosterHandle, XGBoostPredictor::BoosterDestructor>
XGBoostPredictor::allocate_booster(
    const DMatrixHandle _dmats[], bst_ulong _len ) const
{
    BoosterHandle *booster = new BoosterHandle;

    if ( XGBoosterCreate( _dmats, _len, booster ) != 0 )
        {
            delete booster;

            throw std::runtime_error( "Could not create XGBoost handle!" );
        }

    return std::unique_ptr<BoosterHandle, XGBoostPredictor::BoosterDestructor>(
        booster, &XGBoostPredictor::delete_booster );
}

// -----------------------------------------------------------------------------

std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>
XGBoostPredictor::convert_to_dmatrix(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    if ( _X_categorical.size() > 0 )
        {
            return convert_to_dmatrix_sparse( _X_categorical, _X_numerical );
        }
    else
        {
            return convert_to_dmatrix_dense( _X_numerical );
        }
}

// -----------------------------------------------------------------------------

std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>
XGBoostPredictor::convert_to_dmatrix_dense(
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    if ( _X_numerical.size() == 0 )
        {
            throw std::invalid_argument(
                "You must provide at least one column of data!" );
        }

    std::vector<float> mat_float(
        _X_numerical.size() * _X_numerical[0]->size() );

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            if ( _X_numerical[j]->size() != _X_numerical[0]->size() )
                {
                    throw std::invalid_argument(
                        "All columns must have the same length!" );
                }

            for ( size_t i = 0; i < _X_numerical[j]->size(); ++i )
                {
                    mat_float[i * _X_numerical.size() + j] =
                        static_cast<float>( ( *_X_numerical[j] )[i] );
                }
        }

    DMatrixHandle *d_matrix = new DMatrixHandle;

    if ( XGDMatrixCreateFromMat(
             mat_float.data(),
             _X_numerical[0]->size(),
             _X_numerical.size(),
             -1,
             d_matrix ) != 0 )
        {
            delete d_matrix;

            throw std::runtime_error(
                "Creating XGBoost DMatrix from Matrix failed! Do your "
                "features contain NAN or infinite values?" );
        }

    return std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>(
        d_matrix, &XGBoostPredictor::delete_dmatrix );
}

// -----------------------------------------------------------------------------

std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>
XGBoostPredictor::convert_to_dmatrix_sparse(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    if ( impl().n_encodings() != _X_categorical.size() )
        {
            throw std::invalid_argument(
                "Expected " + std::to_string( impl().n_encodings() ) +
                " categorical columns, got " +
                std::to_string( _X_categorical.size() ) + "." );
        }

    const auto csr_mat = impl().make_csr<float, unsigned int, size_t>(
        _X_categorical, _X_numerical );

    DMatrixHandle *d_matrix = new DMatrixHandle;

    if ( XGDMatrixCreateFromCSREx(
             csr_mat.indptr(),
             csr_mat.indices(),
             csr_mat.data(),
             csr_mat.nrows() + 1,
             csr_mat.size(),
             csr_mat.ncols(),
             d_matrix ) != 0 )
        {
            delete d_matrix;

            throw std::runtime_error(
                "Creating XGBoost DMatrix from CSRMatrix failed! Do your "
                "features contain NAN or infinite values?" );
        }

    return std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>(
        d_matrix, &XGBoostPredictor::delete_dmatrix );
}

// -----------------------------------------------------------------------------

std::vector<Float> XGBoostPredictor::feature_importances(
    const size_t _num_features ) const
{
    // --------------------------------------------------------------------
    // Reload the booster

    auto handle = allocate_booster( NULL, 0 );

    if ( XGBoosterLoadModelFromBuffer( *handle, model(), len() ) != 0 )
        {
            std::runtime_error( "Could not reload booster!" );
        }

    // --------------------------------------------------------------------
    // Generate dump

    bst_ulong out_len = 0;

    const char **out_dump_array = nullptr;

    if ( XGBoosterDumpModel( *handle, "", 1, &out_len, &out_dump_array ) != 0 )
        {
            std::runtime_error( "Generating XGBoost dump failed!" );
        }

    // --------------------------------------------------------------------
    // Parse dump

    std::vector<Float> all_feature_importances( impl().ncols_csr() );

    for ( bst_ulong i = 0; i < out_len; ++i )
        {
            parse_dump( out_dump_array[i], &all_feature_importances );
        }

    // --------------------------------------------------------------------
    // Compress the sparse importances to calculate the importance of the entire
    // categorical feature.

    std::vector<Float> feature_importances( _num_features );

    impl().compress_importances(
        all_feature_importances, &feature_importances );

    // --------------------------------------------------------------------
    // Normalize feature importances

    Float sum_importances = std::accumulate(
        feature_importances.begin(), feature_importances.end(), 0.0 );

    for ( auto &val : feature_importances )
        {
            val = val / sum_importances;

            if ( std::isnan( val ) )
                {
                    val = 0.0;
                }
        }

    // --------------------------------------------------------------------

    return feature_importances;
}

// -----------------------------------------------------------------------------

Poco::JSON::Object::Ptr XGBoostPredictor::fingerprint() const
{
    auto obj = Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

    obj->set( "cmd_", cmd_ );
    obj->set( "dependencies_", JSON::vector_to_array_ptr( dependencies_ ) );
    obj->set( "impl_", impl().to_json_obj() );

    return obj;
}

// -----------------------------------------------------------------------------

std::string XGBoostPredictor::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical,
    const CFloatColumn &_y )
{
    // --------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical, _y );

    // --------------------------------------------------------------------
    // Build DMatrix

    auto d_matrix = convert_to_dmatrix( _X_categorical, _X_numerical );

    // convert_to_dmatrix(...) should make sure of this.
    assert_true( _X_numerical.size() > 0 );

    std::vector<float> y_float( _y->size() );

    std::transform(
        _y->begin(), _y->end(), y_float.begin(), []( const Float val ) {
            return static_cast<float>( val );
        } );

    if ( XGDMatrixSetFloatInfo(
             *d_matrix, "label", y_float.data(), y_float.size() ) != 0 )
        {
            throw std::runtime_error( "Setting XGBoost labels failed!" );
        }

    // --------------------------------------------------------------------
    // Allocate the booster

    auto handle = allocate_booster( d_matrix.get(), 1 );

    // --------------------------------------------------------------------
    // Set the hyperparameters

    XGBoosterSetParam(
        *handle, "alpha", std::to_string( hyperparams_.alpha_ ).c_str() );

    XGBoosterSetParam( *handle, "booster", hyperparams_.booster_.c_str() );

    XGBoosterSetParam(
        *handle,
        "colsample_bytree",
        std::to_string( hyperparams_.colsample_bytree_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "colsample_bylevel",
        std::to_string( hyperparams_.colsample_bylevel_ ).c_str() );

    XGBoosterSetParam(
        *handle, "eta", std::to_string( hyperparams_.eta_ ).c_str() );

    XGBoosterSetParam(
        *handle, "gamma", std::to_string( hyperparams_.gamma_ ).c_str() );

    XGBoosterSetParam(
        *handle, "lambda", std::to_string( hyperparams_.lambda_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "max_delta_step",
        std::to_string( hyperparams_.max_delta_step_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "max_depth",
        std::to_string( hyperparams_.max_depth_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "min_child_weight",
        std::to_string( hyperparams_.min_child_weights_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "num_parallel_tree",
        std::to_string( hyperparams_.num_parallel_tree_ ).c_str() );

    XGBoosterSetParam(
        *handle, "normalize_type", hyperparams_.normalize_type_.c_str() );

    XGBoosterSetParam(
        *handle, "nthread", std::to_string( hyperparams_.nthread_ ).c_str() );

    // XGBoost has deprecated reg::linear, but we will continue to support it.
    // Strangely enough, its exactly the other way around for windows.
#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    if ( hyperparams_.objective_ == "reg:squarederror" )
        {
            XGBoosterSetParam( *handle, "objective", "reg:linear" );
        }
#else
    if ( hyperparams_.objective_ == "reg:linear" )
        {
            XGBoosterSetParam( *handle, "objective", "reg:squarederror" );
        }
#endif
    else
        {
            XGBoosterSetParam(
                *handle, "objective", hyperparams_.objective_.c_str() );
        }

    if ( hyperparams_.one_drop_ )
        {
            XGBoosterSetParam( *handle, "one_drop", "1" );
        }
    else
        {
            XGBoosterSetParam( *handle, "one_drop", "0" );
        }

    XGBoosterSetParam(
        *handle,
        "rate_drop",
        std::to_string( hyperparams_.rate_drop_ ).c_str() );

    XGBoosterSetParam(
        *handle, "sample_type", hyperparams_.sample_type_.c_str() );

    if ( hyperparams_.silent_ )
        {
            XGBoosterSetParam( *handle, "silent", "1" );
        }
    else
        {
            XGBoosterSetParam( *handle, "silent", "0" );
        }

    XGBoosterSetParam(
        *handle,
        "skip_drop",
        std::to_string( hyperparams_.skip_drop_ ).c_str() );

    XGBoosterSetParam(
        *handle,
        "subsample",
        std::to_string( hyperparams_.subsample_ ).c_str() );

    // --------------------------------------------------------------------
    // Do the actual fitting

    const auto n_iter = static_cast<int>( hyperparams_.n_iter_ );

    for ( int i = 0; i < n_iter; ++i )
        {
            if ( XGBoosterUpdateOneIter( *handle, i, *d_matrix ) != 0 )
                {
                    std::runtime_error(
                        "XGBoost: Fitting tree or linear model " +
                        std::to_string( i + 1 ) + " failed!" );
                }

            const auto progress = ( ( i + 1 ) * 100 ) / n_iter;

            const auto progress_str =
                "Progress: " + std::to_string( progress ) + "\%.";

            if ( _logger )
                {
                    if ( hyperparams_.booster_ == "gblinear" )
                        {
                            _logger->log(
                                "XGBoost: Trained linear model " +
                                std::to_string( i + 1 ) + ". " + progress_str );
                        }
                    else
                        {
                            _logger->log(
                                "XGBoost: Trained tree " +
                                std::to_string( i + 1 ) + ". " + progress_str );
                        }
                }
        }

    // ----------------------------------------------------------------
    // Dump booster

    const char *out_dptr;

    bst_ulong len = 0;

    if ( XGBoosterGetModelRaw( *handle, &len, &out_dptr ) != 0 )
        {
            throw std::invalid_argument( "Storing of booster failed!" );
        }

    model_ = std::vector<char>( out_dptr, out_dptr + len );

    // ----------------------------------------------------------------
    // Return message

    std::stringstream msg;

    if ( hyperparams_.booster_ == "gblinear" )
        {
            msg << std::endl
                << "XGBoost: Trained " << hyperparams_.n_iter_
                << " linear models.";
        }
    else
        {
            msg << std::endl
                << "XGBoost: Trained " << hyperparams_.n_iter_ << " trees.";
        }

    return msg.str();
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::load( const std::string &_fname )
{
    // --------------------------------------------------------------------
    // Allocate the booster

    auto handle = allocate_booster( NULL, 0 );

    // --------------------------------------------------------------------
    // Load the booster

    if ( XGBoosterLoadModel( *handle, _fname.c_str() ) != 0 )
        {
            throw std::runtime_error( "Could not load XGBoostPredictor!" );
        }

    // ----------------------------------------------------------------
    // Dump booster

    const char *out_dptr;

    bst_ulong len = 0;

    if ( XGBoosterGetModelRaw( *handle, &len, &out_dptr ) != 0 )
        {
            throw std::invalid_argument( "Storing of booster failed!" );
        }

    model_ = std::vector<char>( out_dptr, out_dptr + len );

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::parse_dump(
    const std::string &_dump,
    std::vector<Float> *_all_feature_importances ) const
{
    // ----------------------------------------------------------------
    // Split _dump

    std::vector<std::string> lines;

    {
        std::stringstream stream( _dump );
        std::string line;
        while ( std::getline( stream, line ) )
            {
                lines.push_back( line );
            }
    }

    // ----------------------------------------------------------------

    if ( hyperparams_.booster_ == "gblinear" )
        {
            assert_true( lines.size() >= _all_feature_importances->size() + 3 );

            for ( size_t i = 0; i < _all_feature_importances->size(); ++i )
                {
                    ( *_all_feature_importances )[i] =
                        std::abs( std::atof( lines[i + 3].c_str() ) );
                }
        }
    else
        {
            // ----------------------------------------------------------------
            // Parse individual lines, extracting the gain

            // A typical node might look like this:
            // 4:[f3<42.5] yes=9,no=10,missing=9,gain=8119.99414,cover=144

            // And a leaf looks like this:
            // 9:leaf=3.354321,cover=80

            for ( auto &line : lines )
                {
                    std::size_t begin = line.find( "[f" ) + 2;

                    std::size_t end = line.find( "<" );

                    if ( end != std::string::npos )
                        {
                            // -----------------------
                            // Identify feature

                            int fnum =
                                std::stoi( line.substr( begin, end - begin ) );

                            assert_true( fnum >= 0 );

                            assert_true(
                                fnum < static_cast<int>(
                                           _all_feature_importances->size() ) );

                            // -----------------------
                            // Extract gain

                            begin = line.find( "gain=" ) + 5;

                            assert_true( begin - 5 != std::string::npos );

                            end = line.find( ",", begin );

                            assert_true( end != std::string::npos );

                            Float gain =
                                std::stod( line.substr( begin, end - begin ) );

                            // -----------------------
                            // Add to feature importances

                            ( *_all_feature_importances )[fnum] += gain;
                        }
                }

            // ----------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

CFloatColumn XGBoostPredictor::predict(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    // --------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical );

    // --------------------------------------------------------------------

    if ( !is_fitted() )
        {
            throw std::runtime_error( "XGBoostPredictor has not been fitted!" );
        }

    // --------------------------------------------------------------------
    // Build DMatrix

    auto d_matrix = convert_to_dmatrix( _X_categorical, _X_numerical );

    // --------------------------------------------------------------------
    // Reload the booster

    auto handle = allocate_booster( d_matrix.get(), 1 );

    if ( XGBoosterLoadModelFromBuffer( *handle, model(), len() ) != 0 )
        {
            std::runtime_error( "Could not reload booster!" );
        }

    // --------------------------------------------------------------------
    // Generate predictions

    auto yhat = std::make_shared<std::vector<Float>>( _X_numerical[0]->size() );

    bst_ulong nrows = 0;

    const float *yhat_float = nullptr;

    if ( XGBoosterPredict( *handle, *d_matrix, 0, 0, &nrows, &yhat_float ) !=
         0 )
        {
            std::runtime_error( "Generating XGBoost predictions failed!" );
        }

    assert_true( static_cast<size_t>( nrows ) == yhat->size() );

    std::transform(
        yhat_float, yhat_float + nrows, yhat->begin(), []( const float val ) {
            return static_cast<Float>( val );
        } );

    // --------------------------------------------------------------------

    return yhat;

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::save( const std::string &_fname ) const
{
    // --------------------------------------------------------------------

    if ( len() == 0 )
        {
            throw std::runtime_error( "XGBoostPredictor has not been fitted!" );
        }

    // --------------------------------------------------------------------
    // Load booster

    auto handle = allocate_booster( NULL, 0 );

    if ( XGBoosterLoadModelFromBuffer( *handle, model(), len() ) != 0 )
        {
            std::runtime_error( "Could not reload booster!" );
        }

    // --------------------------------------------------------------------
    // Save model

    if ( XGBoosterSaveModel( *handle, _fname.c_str() ) != 0 )
        {
            throw std::runtime_error( "Could not save XGBoostPredictor!" );
        }

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

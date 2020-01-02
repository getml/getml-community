#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::unique_ptr<BoosterHandle, LightGBMPredictor::BoosterDestructor>
LightGBMPredictor::allocate_booster(
    const DatasetHandle &_training_set, bst_ulong _len ) const
{
    BoosterHandle *raw_ptr = new BoosterHandle;

    const auto res = LGBM_BoosterCreate( _training_set, "TODO", raw_ptr );

    if ( res != 0 )
        {
            delete raw_ptr;

            throw std::runtime_error(
                std::string( "Could not create LightGBM handle: " ) +
                LastErrorMsg() );
        }

    return std::unique_ptr<BoosterHandle, LightGBMPredictor::BoosterDestructor>(
        raw_ptr, &LightGBMPredictor::delete_booster );
}

// -----------------------------------------------------------------------------

std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dmatrix(
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

std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dmatrix_dense(
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    if ( _X_numerical.size() == 0 )
        {
            throw std::invalid_argument(
                "You must provide at least one column of data!" );
        }

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            assert_true( _X_numerical[j] );

            if ( _X_numerical[j]->size() != _X_numerical[0]->size() )
                {
                    throw std::invalid_argument(
                        "All columns must have the same length!" );
                }
        }

    auto mats = std::vector<Float *>();

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            mats.push_back( _X_numerical[i]->data() );
        }

    DatasetHandle *raw_ptr = new DatasetHandle;

    const auto res = LGBM_DatasetCreateFromMats(
        static_cast<std::int32_t>(
            mats.size() ),    // nmat: Number of dense matrices
        mats.data(),          // data: Pointer to the data space
        C_API_DTYPE_FLOAT64,  // data_type: Type of data pointer
        static_cast<std::int32_t>(
            _X_numerical[0]->size() ),  // nrow: Number of rows
        1,                              // ncol: Number of columns
        0,       // is_row_major: 1 for row-major, 0 for column-major
        "",      // parameters: Additional parameters
        NULL,    // reference: Used to align bin mapper with other dataset,
                 // nullptr means isn’t used
        raw_ptr  // [out] out: Created dataset
    );

    if ( res != 0 )
        {
            delete raw_ptr;

            throw std::runtime_error(
                std::string(
                    "Creating LightGBM Dataset from Matrix failed: " ) +
                LastErrorMsg() );
        }

    return std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>(
        raw_ptr, &LightGBMPredictor::delete_dmatrix );
}

// -----------------------------------------------------------------------------

std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dmatrix_sparse(
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

    DatasetHandle *raw_ptr = new DatasetHandle;

    /*if ( XGDatasetCreateFromCSREx(
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
                "Creating XGBoost Dataset from CSRMatrix failed! Do your "
                "features contain NAN or infinite values?" );
        }*/

    return std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>(
        raw_ptr, &LightGBMPredictor::delete_dmatrix );
}

// -----------------------------------------------------------------------------

std::vector<Float> LightGBMPredictor::feature_importances(
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
    // handle: Handle of booster
    // num_iteration: Number of iterations for which feature importance is
    // calculated, <= 0 means use all
    // importance_type: Method of importance
    // calculation: 0 for split, result contains numbers of times the feature is
    // used in a model; 1 for gain, result contains total gains of splits which
    // use the feature
    // [out] out_results: Result array with feature importance

    // TODO> out

    const auto result = LGBM_BoosterFeatureImportance( handle, -1, 1, out );

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

std::string LightGBMPredictor::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical,
    const CFloatColumn &_y )
{
    // --------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical, _y );

    // --------------------------------------------------------------------

    _logger->log( "LightGBM: Preparing..." );

    // --------------------------------------------------------------------
    // Build Dataset

    auto d_matrix = convert_to_dataset( _X_categorical, _X_numerical );

    // convert_to_dmatrix(...) should make sure of this.
    assert_true( _X_numerical.size() > 0 );

    std::vector<float> y_float( _y->size() );

    std::transform(
        _y->begin(), _y->end(), y_float.begin(), []( const Float val ) {
            return static_cast<float>( val );
        } );

    if ( XGDatasetSetFloatInfo(
             *d_matrix, "label", y_float.data(), y_float.size() ) != 0 )
        {
            std::runtime_error( "Setting XGBoost labels failed!" );
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

    for ( int i = 0; i < static_cast<int>( hyperparams_.n_iter_ ); ++i )
        {
            if ( XGBoosterUpdateOneIter( *handle, i, *d_matrix ) != 0 )
                {
                    std::runtime_error(
                        "XGBoost: Fitting tree or linear model " +
                        std::to_string( i + 1 ) + " failed!" );
                }

            if ( hyperparams_.booster_ == "gblinear" )
                {
                    _logger->log(
                        "XGBoost: Trained linear model " +
                        std::to_string( i + 1 ) + "." );
                }
            else
                {
                    _logger->log(
                        "XGBoost: Trained tree " + std::to_string( i + 1 ) +
                        "." );
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

void LightGBMPredictor::load( const std::string &_fname )
{
    // --------------------------------------------------------------------
    // Allocate the booster

    auto handle = allocate_booster( NULL, 0 );

    // --------------------------------------------------------------------
    // Load the booster

    if ( XGBoosterLoadModel( *handle, _fname.c_str() ) != 0 )
        {
            throw std::runtime_error( "Could not load LightGBMPredictor!" );
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

CFloatColumn LightGBMPredictor::predict(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    // --------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical );

    // --------------------------------------------------------------------

    if ( len() == 0 )
        {
            throw std::runtime_error(
                "LightGBMPredictor has not been fitted!" );
        }

    // --------------------------------------------------------------------
    // Build Dataset

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

void LightGBMPredictor::save( const std::string &_fname ) const
{
    // --------------------------------------------------------------------

    if ( len() == 0 )
        {
            throw std::runtime_error(
                "LightGBMPredictor has not been fitted!" );
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
            throw std::runtime_error( "Could not save LightGBMPredictor!" );
        }

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

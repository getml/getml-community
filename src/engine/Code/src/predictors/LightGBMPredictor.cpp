#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

std::shared_ptr<BoosterHandle> LightGBMPredictor::allocate_booster(
    const DatasetHandle &_training_set ) const
{
    BoosterHandle *raw_ptr = new BoosterHandle;

    const auto res =
        LGBM_BoosterCreate( _training_set, hyperparam_string_.data(), raw_ptr );

    if ( res != 0 )
        {
            delete raw_ptr;

            throw std::runtime_error(
                std::string( "Could not create LightGBM handle: " ) +
                LastErrorMsg() );
        }

    return std::shared_ptr<BoosterHandle>(
        raw_ptr, &LightGBMPredictor::delete_booster );
}

// -----------------------------------------------------------------------------
/*
std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dataset(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    if ( _X_categorical.size() > 0 )
        {
            return convert_to_dataset_sparse( _X_categorical, _X_numerical );
        }
    else
        {
            return convert_to_dataset_dense( _X_numerical );
        }
}*/

// -----------------------------------------------------------------------------

std::vector<float> LightGBMPredictor::convert_to_dense_matrix(
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

    auto mat =
        std::vector<float>( _X_numerical.size() * _X_numerical[0]->size() );

    for ( size_t j = 0; j < _X_numerical.size(); ++j )
        {
            std::transform(
                _X_numerical[j]->begin(),
                _X_numerical[j]->end(),
                mat.begin() + j * _X_numerical[0]->size(),
                []( const Float val ) { return static_cast<float>( val ); } );
        }

    return mat;
}

// -----------------------------------------------------------------------------

std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dataset_dense(
    const std::vector<CFloatColumn> &_X_numerical,
    const std::vector<float> &_mat ) const
{
    DatasetHandle *raw_ptr = new DatasetHandle;

    assert_true( _X_numerical.size() > 0 );

    assert_true( _X_numerical.size() * _X_numerical[0]->size() == _mat.size() );

    const auto nrow = static_cast<std::int32_t>( _X_numerical[0]->size() );

    const auto ncol = static_cast<std::int32_t>( _X_numerical.size() );

    const auto res = LGBM_DatasetCreateFromMat(
        _mat.data(),          // data: Pointer to the data space
        C_API_DTYPE_FLOAT32,  // data_type: Type of data pointer
        nrow,                 // nrow: Number of rows
        ncol,                 // ncol: Number of columns
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
                std::string( "Creating dataset failed: " ) + LastErrorMsg() );
        }

    return std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>(
        raw_ptr, &LightGBMPredictor::delete_dmatrix );
}

// -----------------------------------------------------------------------------

std::unique_ptr<DatasetHandle, LightGBMPredictor::DatasetDestructor>
LightGBMPredictor::convert_to_dataset_sparse(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    assert_true( false && "TODO" );

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

    /*auto handle = allocate_booster( NULL, 0 );

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
        }*/

    // --------------------------------------------------------------------

    // TODO
    auto feature_importances = std::vector<Float>( _num_features, 1.0 );

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

    auto mat = convert_to_dense_matrix( _X_numerical );

    auto dataset = convert_to_dataset_dense( _X_numerical, mat );

    assert_true( dataset );

    assert_true( _X_numerical.size() > 0 );

    std::vector<float> y_float( _y->size() );

    std::transform(
        _y->begin(), _y->end(), y_float.begin(), []( const Float val ) {
            return static_cast<float>( val );
        } );

    auto res = LGBM_DatasetSetField(
        *dataset,  // handle: Handle of dataset
        "label",   // field_name: Field name, can be label, weight, init_score,
                   // group
        y_float.data(),  // field_data: Pointer to data vector
        static_cast<int>(
            y_float.size() ),  // num_elements: Number of elements in field_data
        C_API_DTYPE_FLOAT32    // type: Type of field_data pointer, can be
                               // C_API_DTYPE_INT32, C_API_DTYPE_FLOAT32 or
                               // C_API_DTYPE_FLOAT64
    );

    if ( res != 0 )
        {
            throw std::runtime_error(
                std::string( "Setting LightGBM labels failed: " ) +
                LastErrorMsg() );
        }

    // --------------------------------------------------------------------
    // Allocate the booster

    auto booster = allocate_booster( *dataset );

    assert_true( booster );

    // --------------------------------------------------------------------
    // Do the actual fitting

    for ( int i = 0; i < static_cast<int>( hyperparams_.n_estimators_ ); ++i )
        {
            int is_finished = 0;

            res = LGBM_BoosterUpdateOneIter( *booster, &is_finished );

            if ( res != 0 )
                {
                    std::runtime_error(
                        "LightGBM: Fitting tree " + std::to_string( i + 1 ) +
                        " failed: " + LastErrorMsg() );
                }

            if ( _logger )
                {
                    _logger->log(
                        "LightGBM: Trained tree " + std::to_string( i + 1 ) +
                        "." );
                }
        }

    // ----------------------------------------------------------------
    // Dump booster

    model_ = booster;

    save( "test" );

    std::cout << "Saved." << std::endl;

    // ----------------------------------------------------------------
    // Return message

    std::stringstream msg;

    msg << std::endl
        << "LightGBM: Trained " << hyperparams_.n_estimators_ << " trees.";

    return msg.str();

    // ----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void LightGBMPredictor::load( const std::string &_fname )
{
    assert_true( false && "TODO" );

    /*
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
*/
    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
/*
std::unique_ptr<BoosterHandle, LightGBMPredictor::BoosterDestructor>
LightGBMPredictor::load_booster_from_string() const
{
    if ( model_ )
        {
            throw std::runtime_error(
                "Failed to load LightGBM predictor from string: LightGBM "
                "predictor has not been fitted." );
        }

    int num_iterations = 0;

    auto booster =
        std::unique_ptr<BoosterHandle, LightGBMPredictor::BoosterDestructor>(
            new BoosterHandle, &LightGBMPredictor::delete_booster );

    const auto res = LGBM_BoosterLoadModelFromString(
        model_.data(), &num_iterations, booster.get() );

    if ( res != 0 )
        {
            throw std::runtime_error(
                std::string(
                    "Failed to load LightGBM predictor from string: " ) +
                LastErrorMsg() );
        }

    return booster;
}*/

// -----------------------------------------------------------------------------

std::string LightGBMPredictor::make_hyperparam_string() const
{  // TODO
    return "    ";
}

// -----------------------------------------------------------------------------

CFloatColumn LightGBMPredictor::predict(
    const std::vector<CIntColumn> &_X_categorical,
    const std::vector<CFloatColumn> &_X_numerical ) const
{
    // --------------------------------------------------------------------

    impl().check_plausibility( _X_categorical, _X_numerical );

    // --------------------------------------------------------------------

    if ( !model_ )
        {
            throw std::runtime_error(
                "LightGBMPredictor has not been fitted!" );
        }

    // --------------------------------------------------------------------

    auto booster = model_;

    // --------------------------------------------------------------------
    // Generate predictions

    assert_true( _X_numerical.size() > 0 );

    auto yhat = std::make_shared<std::vector<Float>>( _X_numerical[0]->size() );

    auto out_len = static_cast<std::int64_t>( yhat->size() );

    auto mat = convert_to_dense_matrix( _X_numerical );

    assert_true( _X_numerical.size() * _X_numerical[0]->size() == mat.size() );

    const auto nrow = static_cast<std::int32_t>( _X_numerical[0]->size() );

    const auto ncol = static_cast<std::int32_t>( _X_numerical.size() );

    const auto res = LGBM_BoosterPredictForMat(
        *booster,             // handle: Handle of booster
        mat.data(),           // data: Pointer to the data space
        C_API_DTYPE_FLOAT32,  // data_type: Type of data pointer, can be
                              // C_API_DTYPE_FLOAT32 or C_API_DTYPE_FLOAT64
        nrow,                 // nrow: Number of rows
        ncol,                 // ncol: Number of columns
        0,  // is_row_major: 1 for row-major, 0 for column-major
        C_API_PREDICT_NORMAL,  // predict_type: What should be predicted
        // C_API_PREDICT_NORMAL: normal prediction, with transform (if
        // needed);
        // C_API_PREDICT_RAW_SCORE: raw score;
        // C_API_PREDICT_LEAF_INDEX: leaf index;
        // C_API_PREDICT_CONTRIB: feature contributions (SHAP values)
        0,  // num_iteration: Number of iteration for prediction, <= 0 means no
        // limit,
        "",  // parameter: Other parameters for prediction, e.g. early stopping
             // for prediction
        &out_len,
        yhat->data() );

    if ( res != 0 )
        {
            throw std::runtime_error(
                std::string( "Failed to generate predictions: " ) +
                LastErrorMsg() );
        }

    // --------------------------------------------------------------------

    return yhat;

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------

void LightGBMPredictor::save( const std::string &_fname ) const
{
    // --------------------------------------------------------------------

    if ( !model_ )
        {
            throw std::runtime_error(
                "Could not save LightGBM predictor: LightGBM predictor has not "
                "been fitted!" );
        }

    // --------------------------------------------------------------------

    const auto res = LGBM_BoosterSaveModel(
        *model_,  // handle: Handle of booster
        0,  // start_iteration: Start index of the iteration that should be
            // saved
        0,  // num_iteration: Index of the iteration that should be saved, <= 0
            // means save all
        _fname.c_str()  // filename: The name of the file
    );

    if ( res != 0 )
        {
            throw std::runtime_error( "Saving LightGBM predictor failed!" );
        }

    // --------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

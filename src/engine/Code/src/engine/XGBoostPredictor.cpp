#include "engine/predictors/predictors.hpp"

namespace engine
{
namespace predictors
{
// ------------------------------------------------------------------------

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

// ------------------------------------------------------------------------

std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>
XGBoostPredictor::convert_to_dmatrix(
    const containers::Matrix<ENGINE_FLOAT> &_mat ) const
{
    containers::Matrix<float> mat_float( _mat.nrows(), _mat.ncols() );

    std::transform(
        _mat.begin(),
        _mat.end(),
        mat_float.begin(),
        []( const ENGINE_FLOAT val ) { return static_cast<float>( val ); } );

    DMatrixHandle *d_matrix = new DMatrixHandle;

    if ( XGDMatrixCreateFromMat(
             mat_float.data(),
             mat_float.nrows(),
             mat_float.ncols(),
             -1,
             d_matrix ) != 0 )
        {
            delete d_matrix;

            throw std::runtime_error( "Creating XGBoost DMatrix failed!" );
        }

    return std::unique_ptr<DMatrixHandle, XGBoostPredictor::DMatrixDestructor>(
        d_matrix, &XGBoostPredictor::delete_dmatrix );
}

// ------------------------------------------------------------------------

std::vector<ENGINE_FLOAT> XGBoostPredictor::feature_importances(
    const size_t _num_features ) const
{
    // --------------------------------------------------------------------
    // Reload the booster

    auto handle = allocate_booster( NULL, 0 );

    if ( XGBoosterLoadModelFromBuffer( *handle, model(), len() ) != 0 )
        {
            std::invalid_argument( "Could not reload booster!" );
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

    std::vector<ENGINE_FLOAT> feature_importances( _num_features );

    for ( bst_ulong i = 0; i < out_len; ++i )
        {
            parse_dump( out_dump_array[i], &feature_importances );
        }

    // --------------------------------------------------------------------
    // Normalize feature importances

    ENGINE_FLOAT sum_importances = std::accumulate(
        feature_importances.begin(), feature_importances.end(), 0.0 );

    for ( auto &val : feature_importances )
        {
            val = val / sum_importances;

            if ( val != val )
                {
                    val = 0.0;
                }
        }

    // --------------------------------------------------------------------

    return feature_importances;
}

// ------------------------------------------------------------------------

std::string XGBoostPredictor::fit(
    const std::shared_ptr<const logging::Logger> _logger,
    const containers::Matrix<ENGINE_FLOAT> &_X,
    const containers::Matrix<ENGINE_FLOAT> &_y )
{
    // --------------------------------------------------------------------

    assert( _X.nrows() == _y.nrows() );

    assert( _y.ncols() == 1 );

    _logger->log( "XGBoost: Preparing..." );

    // --------------------------------------------------------------------
    // Build DMatrix

    auto d_matrix = convert_to_dmatrix( _X );

    containers::Matrix<float> y_float( _y.nrows(), 1 );

    std::transform(
        _y.begin(), _y.end(), y_float.begin(), []( const ENGINE_FLOAT val ) {
            return static_cast<float>( val );
        } );

    if ( XGDMatrixSetFloatInfo(
             *d_matrix, "label", y_float.data(), y_float.nrows() ) != 0 )
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

    XGBoosterSetParam( *handle, "objective", hyperparams_.objective_.c_str() );

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

    for ( size_t i = 0; i < hyperparams_.n_iter_; ++i )
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

// ------------------------------------------------------------------------

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

// ------------------------------------------------------------------------

void XGBoostPredictor::parse_dump(
    const std::string &_dump,
    std::vector<ENGINE_FLOAT> *_feature_importances ) const
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
            assert( lines.size() >= _feature_importances->size() + 3 );

            for ( size_t i = 0; i < _feature_importances->size(); ++i )
                {
                    ( *_feature_importances )[i] =
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

                            assert( fnum >= 0 );

                            assert(
                                fnum < static_cast<int>(
                                           _feature_importances->size() ) );

                            // -----------------------
                            // Extract gain

                            begin = line.find( "gain=" ) + 5;

                            assert( begin - 5 != std::string::npos );

                            end = line.find( ",", begin );

                            assert( end != std::string::npos );

                            ENGINE_FLOAT gain =
                                std::stod( line.substr( begin, end - begin ) );

                            // -----------------------
                            // Add to feature importances

                            ( *_feature_importances )[fnum] += gain;
                        }
                }

            // ----------------------------------------------------------------
        }
}

// ------------------------------------------------------------------------

containers::Matrix<ENGINE_FLOAT> XGBoostPredictor::predict(
    const containers::Matrix<ENGINE_FLOAT> &_X ) const
{
    // --------------------------------------------------------------------

    if ( len() == 0 )
        {
            throw std::runtime_error( "XGBoostPredictor has not been fitted!" );
        }

    // --------------------------------------------------------------------
    // Build DMatrix

    auto d_matrix = convert_to_dmatrix( _X );

    // --------------------------------------------------------------------
    // Reload the booster

    auto handle = allocate_booster( d_matrix.get(), 1 );

    if ( XGBoosterLoadModelFromBuffer( *handle, model(), len() ) != 0 )
        {
            std::invalid_argument( "Could not reload booster!" );
        }

    // --------------------------------------------------------------------
    // Generate predictions

    containers::Matrix<ENGINE_FLOAT> yhat( _X.nrows(), 1 );

    bst_ulong nrows = 0;

    const float *yhat_float = nullptr;

    if ( XGBoosterPredict( *handle, *d_matrix, 0, 0, &nrows, &yhat_float ) !=
         0 )
        {
            std::runtime_error( "Generating XGBoost predictions failed!" );
        }

    assert( static_cast<size_t>( nrows ) == yhat.nrows() );

    std::transform(
        yhat_float, yhat_float + nrows, yhat.begin(), []( const float val ) {
            return static_cast<ENGINE_FLOAT>( val );
        } );

    // --------------------------------------------------------------------

    return yhat;
}

// ------------------------------------------------------------------------

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
            std::invalid_argument( "Could not reload booster!" );
        }

    // --------------------------------------------------------------------
    // Save model

    if ( XGBoosterSaveModel( *handle, _fname.c_str() ) != 0 )
        {
            throw std::runtime_error( "Could not save XGBoostPredictor!" );
        }

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace predictors
}  // namespace engine

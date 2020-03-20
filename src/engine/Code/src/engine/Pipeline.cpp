#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

Pipeline::Pipeline(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const Poco::JSON::Object& _obj )
    : impl_( PipelineImpl( _categories, _obj ) )

{
    // This won't to anything - the point it to make sure that it can be
    // parsed correctly.
    init_feature_engineerers( 1 );
    init_predictors( "feature_selectors_", 1 );
    init_predictors( "predictors_", 1 );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _path )
    : impl_( PipelineImpl( _categories ) )
{
    *this = load( _categories, _path );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline( const Pipeline& _other ) : impl_( _other.impl_ )
{
    feature_engineerers_ = clone( _other.feature_engineerers_ );

    feature_selectors_ = clone( _other.feature_selectors_ );

    predictors_ = clone( _other.predictors_ );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline( Pipeline&& _other ) noexcept
    : impl_( std::move( _other.impl_ ) )
{
    feature_engineerers_ = std::move( _other.feature_engineerers_ );

    feature_selectors_ = std::move( _other.feature_selectors_ );

    predictors_ = std::move( _other.predictors_ );
}

// ----------------------------------------------------------------------------

Pipeline::~Pipeline() = default;

// ----------------------------------------------------------------------

void Pipeline::add_population_cols(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    containers::Features* _features ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().numerical_colnames() )
        {
            _features->push_back( population_df.numerical( col ).data_ptr() );
        }
}

// ----------------------------------------------------------------------

void Pipeline::calculate_feature_stats(
    const containers::Features _features,
    const size_t _nrows,
    const size_t _ncols,
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // ------------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto df = utils::Getter::get( population_name, _data_frames );

    // ------------------------------------------------------------------------

    std::vector<const Float*> targets;

    for ( size_t j = 0; j < df.num_targets(); ++j )
        {
            targets.push_back( df.target( j ).data() );
        }

    // ------------------------------------------------------------------------

    const size_t num_bins = 200;

    scores().from_json_obj( metrics::Summarizer::calculate_feature_correlations(
        _features, _nrows, _ncols, targets ) );

    scores().from_json_obj( metrics::Summarizer::calculate_feature_plots(
        _features, _nrows, _ncols, num_bins, targets ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
Pipeline::feature_names() const
{
    std::vector<std::string> autofeatures;

    size_t i = 0;

    for ( const auto& fe : feature_engineerers_ )
        {
            for ( size_t j = 0; j < fe->num_features(); ++j )
                {
                    autofeatures.push_back(
                        "feature_" + std::to_string( ++i ) );
                }
        }

    if ( impl_.predictor_impl_ )
        {
            return std::make_tuple(
                autofeatures,
                predictor_impl().categorical_colnames(),
                predictor_impl().numerical_colnames() );
        }
    else
        {
            return std::make_tuple(
                autofeatures,
                std::vector<std::string>(),
                std::vector<std::string>() );
        }
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::generate_numerical_features(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket ) const
{
    // -------------------------------------------------------------------------
    // Generate the features.

    auto numerical_features = containers::Features();

    for ( const auto& fe : feature_engineerers_ )
        {
            assert_true( fe );

            auto new_features =
                fe->transform( _cmd, _logger, _data_frames, _socket );

            numerical_features.insert(
                numerical_features.end(),
                new_features.begin(),
                new_features.end() );
        }

    // -------------------------------------------------------------------------
    // Add the numerical columns from the population table.

    add_population_cols( _cmd, _data_frames, &numerical_features );

    // -------------------------------------------------------------------------

    return numerical_features;

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::generate_predictions(
    const containers::CategoricalFeatures& _categorical_features,
    const containers::Features& _numerical_features ) const
{
    // ------------------------------------------------------------------------

    size_t nrows = 0;

    if ( _numerical_features.size() > 0 )
        {
            nrows = _numerical_features[0]->size();
        }
    else if ( _categorical_features.size() > 0 )
        {
            nrows = _categorical_features[0]->size();
        }
    else
        {
            assert_true( false && "No features" );
        }

    // ------------------------------------------------------------------------

    assert_true( num_predictors_per_set() > 0 );

    const auto divisor = static_cast<Float>( num_predictors_per_set() );

    auto predictions = containers::Features();

    for ( size_t i = 0; i < num_predictor_sets(); ++i )
        {
            auto mean_prediction =
                std::make_shared<std::vector<Float>>( nrows );

            for ( size_t j = 0; j < num_predictors_per_set(); ++j )
                {
                    const auto new_prediction = predictor( i, j )->predict(
                        _categorical_features, _numerical_features );

                    assert_true( new_prediction );
                    assert_true(
                        new_prediction->size() == mean_prediction->size() );

                    std::transform(
                        mean_prediction->begin(),
                        mean_prediction->end(),
                        new_prediction->begin(),
                        mean_prediction->begin(),
                        std::plus<Float>() );
                }

            for ( auto& val : *mean_prediction )
                {
                    val /= divisor;
                }

            predictions.push_back( mean_prediction );
        }

    // ------------------------------------------------------------------------

    return predictions;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::fit(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------

    num_targets() = infer_num_targets( _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Fit the feature engineering algorithms.

    auto feature_engineerers = init_feature_engineerers( num_targets() );

    for ( auto& fe : feature_engineerers )
        {
            assert_true( fe );

            fe->fit( _cmd, _logger, _data_frames, _socket );
        }

    feature_engineerers_ = std::move( feature_engineerers );

    // -------------------------------------------------------------------------

    make_predictor_impl( _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Select features

    // auto feature_selectors = init_predictors( "feature_selectors_",
    // _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Fit the predictors.

    auto predictors = init_predictors( "predictors_", num_targets() );

    fit_predictors( _cmd, _logger, _data_frames, &predictors, _socket );

    predictors_ = std::move( predictors );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::fit_predictors(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>*
        _predictors,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    auto categorical_features = get_categorical_features( _cmd, _data_frames );

    categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    // --------------------------------------------------------------------

    const auto numerical_features =
        generate_numerical_features( _cmd, _logger, _data_frames, _socket );

    // --------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    assert_true( population_df.num_targets() == _predictors->size() );

    // --------------------------------------------------------------------

    for ( size_t t = 0; t < population_df.num_targets(); ++t )
        {
            const auto target_col = population_df.target( t ).data_ptr();

            for ( auto& p : ( *_predictors )[t] )
                {
                    assert_true( p );

                    p->fit(
                        _logger,
                        categorical_features,
                        numerical_features,
                        target_col );
                }
        }

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures Pipeline::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    auto categorical_features = containers::CategoricalFeatures();

    // TODO
    /*if ( !feature_engineerer().hyperparameters().include_categorical_ )
        {
            return categorical_features;
        }*/

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : predictor_impl().categorical_colnames() )
        {
            categorical_features.push_back(
                population_df.categorical( col ).data_ptr() );
        }

    return categorical_features;
}

// ----------------------------------------------------------------------

std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
Pipeline::init_feature_engineerers( const size_t _num_targets ) const
{
    // ----------------------------------------------------------------------

    const auto population = std::make_shared<Poco::JSON::Object>(
        *JSON::get_object( obj(), "population_" ) );

    const auto peripheral = parse_peripheral();

    const auto arr = JSON::get_array( obj(), "feature_engineerers_" );

    // ----------------------------------------------------------------------

    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers;

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            // --------------------------------------------------------------

            const auto ptr = arr->getObject( i );

            if ( !ptr )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in feature_engineerers_ is not a proper JSON "
                        "object." );
                }

            // --------------------------------------------------------------

            auto new_feature_engineerer =
                featureengineerers::FeatureEngineererParser::parse(
                    *ptr, population, peripheral, categories() );

            // --------------------------------------------------------------

            if ( new_feature_engineerer->supports_multiple_targets() )
                {
                    feature_engineerers.emplace_back(
                        std::move( new_feature_engineerer ) );
                }
            else
                {
                    for ( size_t t = 0; t < _num_targets; ++t )
                        {
                            // TODO: Find elegant way to pass on information
                            // about target_num_

                            feature_engineerers.emplace_back(
                                featureengineerers::FeatureEngineererParser::
                                    parse(
                                        *ptr,
                                        population,
                                        peripheral,
                                        categories() ) );
                        }
                }

            // --------------------------------------------------------------
        }

    // ----------------------------------------------------------------------

    return feature_engineerers;

    // ----------------------------------------------------------------------
}

// ----------------------------------------------------------------------

std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>
Pipeline::init_predictors(
    const std::string& _elem, const size_t _num_targets ) const
{
    // --------------------------------------------------------------------

    const auto arr = JSON::get_array( obj(), _elem );

    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>> predictors;

    for ( size_t t = 0; t < _num_targets; ++t )
        {
            std::vector<std::shared_ptr<predictors::Predictor>>
                predictors_for_target;

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    const auto ptr = arr->getObject( i );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "Element " + std::to_string( i ) + " in " +
                                _elem +
                                " is not a proper JSON "
                                "object." );
                        }

                    auto new_predictor = predictors::PredictorParser::parse(
                        *ptr, impl_.predictor_impl_, categories() );

                    predictors_for_target.emplace_back(
                        std::move( new_predictor ) );
                }

            predictors.emplace_back( std::move( predictors_for_target ) );
        }

    // --------------------------------------------------------------------

    return predictors;

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Pipeline Pipeline::load(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _path ) const
{
    // ------------------------------------------------------------------

    const auto obj = load_json_obj( _path + "obj.json" );

    const auto pipeline_json = load_json_obj( _path + "pipeline.json" );

    const auto scores =
        metrics::Scores( load_json_obj( _path + "scores.json" ) );

    auto predictor_impl = std::shared_ptr<predictors::PredictorImpl>();

    try
        {
            predictor_impl = std::make_shared<predictors::PredictorImpl>(
                load_json_obj( _path + "impl.json" ) );
        }
    catch ( std::exception& e )
        {
            std::cout << "no impl" << std::endl;
        }

    // ------------------------------------------------------------

    const auto num_targets =
        JSON::get_value<size_t>( pipeline_json, "num_targets_" );

    // ------------------------------------------------------------------

    auto pipeline = Pipeline( _categories, obj );

    pipeline.allow_http() =
        JSON::get_value<bool>( pipeline_json, "allow_http_" );

    pipeline.num_targets() = num_targets;

    pipeline.obj() = obj;

    pipeline.impl_.predictor_impl_ = predictor_impl;

    pipeline.scores() = scores;

    // ------------------------------------------------------------
    // Load feature engineerers

    pipeline.feature_engineerers_ =
        pipeline.init_feature_engineerers( num_targets );

    for ( size_t i = 0; i < pipeline.feature_engineerers_.size(); ++i )
        {
            auto& fe = pipeline.feature_engineerers_[i];

            assert_true( fe );

            fe->load(
                _path + "feature-engineerer-" + std::to_string( i ) + ".json" );
        }

    // ------------------------------------------------------------
    // Load feature selectors

    // TODO

    // ------------------------------------------------------------
    // Load predictors

    pipeline.predictors_ =
        pipeline.init_predictors( "predictors_", num_targets );

    assert_true( num_targets == pipeline.predictors_.size() );

    for ( size_t i = 0; i < num_targets; ++i )
        {
            for ( size_t j = 0; j < pipeline.predictors_[i].size(); ++j )
                {
                    auto& p = pipeline.predictors_[i][j];

                    assert_true( p );

                    p->load(
                        _path + "predictor-" + std::to_string( i ) + "-" +
                        std::to_string( j ) );
                }
        }

    // ------------------------------------------------------------

    return pipeline;

    // ------------------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object Pipeline::load_json_obj( const std::string& _fname ) const
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

    const auto ptr = Poco::JSON::Parser()
                         .parse( json.str() )
                         .extract<Poco::JSON::Object::Ptr>();

    if ( !ptr )
        {
            throw std::runtime_error( "JSON file did not contain an object!" );
        }

    return *ptr;
}

// ------------------------------------------------------------------------

void Pipeline::make_predictor_impl(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // --------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    // TODO
    const auto allow_null = false;

    const auto is_null = []( const Float val ) {
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    // --------------------------------------------------------------------

    auto categorical_colnames = std::vector<std::string>();

    // TODO
    /*if ( feature_engineerer().hyperparameters().include_categorical_ )
        {*/
    for ( size_t i = 0; i < population_df.num_categoricals(); ++i )
        {
            if ( population_df.categorical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            categorical_colnames.push_back(
                population_df.categorical( i ).name() );
        }
    //}

    // --------------------------------------------------------------------

    auto numerical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_numericals(); ++i )
        {
            if ( population_df.numerical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( !allow_null )
                {
                    const auto contains_null = std::any_of(
                        population_df.numerical( i ).begin(),
                        population_df.numerical( i ).end(),
                        is_null );

                    if ( contains_null )
                        {
                            continue;
                        }
                }

            numerical_colnames.push_back( population_df.numerical( i ).name() );
        }

    // --------------------------------------------------------------------

    size_t num_autofeatures = 0;

    for ( const auto& fe : feature_engineerers_ )
        {
            assert_true( fe );

            num_autofeatures += fe->num_features();
        }

    // --------------------------------------------------------------------

    const auto local_predictor_impl =
        std::make_shared<predictors::PredictorImpl>(
            categorical_colnames, numerical_colnames, num_autofeatures );

    impl_.predictor_impl_ = local_predictor_impl;

    // --------------------------------------------------------------------

    auto categorical_features = get_categorical_features( _cmd, _data_frames );

    local_predictor_impl->fit_encodings( categorical_features );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Pipeline& Pipeline::operator=( const Pipeline& _other )
{
    Pipeline temp( _other );

    *this = std::move( temp );

    return *this;
}

// ----------------------------------------------------------------------------

Pipeline& Pipeline::operator=( Pipeline&& _other ) noexcept
{
    if ( this == &_other )
        {
            return *this;
        }

    impl_ = std::move( _other.impl_ );

    feature_engineerers_ = std::move( _other.feature_engineerers_ );

    feature_selectors_ = std::move( _other.feature_selectors_ );

    predictors_ = std::move( _other.predictors_ );

    return *this;
}

// ----------------------------------------------------------------------

std::shared_ptr<std::vector<std::string>> Pipeline::parse_peripheral() const
{
    auto peripheral = std::make_shared<std::vector<std::string>>();

    const auto arr = JSON::get_array( obj(), "peripheral_" );

    assert_true( arr );

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            const auto ptr = arr->getObject( i );

            if ( !ptr )
                {
                    throw std::invalid_argument(
                        "Element " + std::to_string( i ) +
                        " in peripheral_ is not a proper JSON "
                        "object." );
                }

            const auto name = JSON::get_value<std::string>( *ptr, "name_" );

            peripheral->push_back( name );
        }

    return peripheral;
}

// ----------------------------------------------------------------------------

void Pipeline::save( const std::string& _path, const std::string& _name ) const
{
    // ------------------------------------------------------------------

    auto tfile = Poco::TemporaryFile();

    tfile.createDirectories();

    // ------------------------------------------------------------------

    for ( size_t i = 0; i < feature_engineerers_.size(); ++i )
        {
            const auto& fe = feature_engineerers_[i];

            if ( !fe )
                {
                    throw std::invalid_argument(
                        "Feature engineering algorithm #" +
                        std::to_string( i ) + " has not been fitted!" );
                }

            fe->save(
                tfile.path() + "/feature-engineerer-" + std::to_string( i ) +
                ".json" );
        }

    // ------------------------------------------------------------------

    Poco::JSON::Object pipeline_json;

    pipeline_json.set( "allow_http_", allow_http() );

    pipeline_json.set( "num_targets_", num_targets() );

    save_json_obj( pipeline_json, tfile.path() + "/pipeline.json" );

    // ------------------------------------------------------------------

    save_json_obj( obj(), tfile.path() + "/obj.json" );

    // ------------------------------------------------------------------

    scores().save( tfile.path() + "/scores.json" );

    // ------------------------------------------------------------------

    predictor_impl().save( tfile.path() + "/impl.json" );

    // ------------------------------------------------------------------

    for ( size_t i = 0; i < predictors_.size(); ++i )
        {
            for ( size_t j = 0; j < predictors_[i].size(); ++j )
                {
                    const auto& p = predictors_[i][j];

                    if ( !p )
                        {
                            throw std::invalid_argument(
                                "Predictor " + std::to_string( i ) + "-" +
                                std::to_string( j ) + " has not been fitted!" );
                        }

                    p->save(
                        tfile.path() + "/predictor-" + std::to_string( i ) +
                        "-" + std::to_string( j ) );
                }
        }

    // ------------------------------------------------------------------

    auto file = Poco::File( _path + _name );

    // Create all parent directories, if necessary.
    file.createDirectories();

    // If the actual folder already exists, delete it.
    file.remove( true );

    tfile.renameTo( file.path() );

    tfile.keep();

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::score(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const containers::Features& _yhat )
{
    // ------------------------------------------------
    // Get population table.

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    containers::Features y;

    for ( size_t i = 0; i < population_df.num_targets(); ++i )
        {
            y.push_back( population_df.target( i ).data_ptr() );
        }

    // ------------------------------------------------
    // Make sure input is plausible

    if ( _yhat.size() != y.size() )
        {
            throw std::invalid_argument(
                "Number of columns in predictions and targets do not "
                "match! "
                "Number of columns in predictions: " +
                std::to_string( _yhat.size() ) +
                ". Number of columns in targets: " +
                std::to_string( y.size() ) + "." );
        }

    for ( size_t i = 0; i < y.size(); ++i )
        {
            assert_true( y[i] );
            assert_true( _yhat[i] );

            if ( _yhat[i]->size() != y[i]->size() )
                {
                    throw std::invalid_argument(
                        "Number of rows in predictions and targets do not "
                        "match! "
                        "Number of rows in predictions: " +
                        std::to_string( _yhat[i]->size() ) +
                        ". Number of rows in targets: " +
                        std::to_string( y[i]->size() ) + "." );
                }
        }

    // ------------------------------------------------
    // Calculate the score

    debug_log( "Calculating score..." );

    auto obj = metrics::Scorer::score( is_classification(), _yhat, y );

    scores().from_json_obj( obj );

    // ------------------------------------------------

    return metrics::Scorer::get_metrics( obj );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::to_monitor( const std::string& _name ) const
{
    auto feature_engineerers =
        Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& fe : feature_engineerers_ )
        {
            auto obj = fe->to_monitor( _name );

            feature_engineerers->add( obj );
        }

    Poco::JSON::Object json_obj;

    json_obj.set( "name_", _name );

    json_obj.set( "allow_http_", allow_http() );

    json_obj.set( "feature_engineerers_", feature_engineerers );

    return json_obj;
}

// ----------------------------------------------------------------------------

std::string Pipeline::to_sql() const
{
    std::string sql;

    size_t offset = 0;

    for ( const auto& fe : feature_engineerers_ )
        {
            sql += fe->to_sql( offset );

            offset += fe->num_features();
        }

    return sql;
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------

    if ( feature_engineerers_.size() == 0 && predictors_.size() == 0 )
        {
            throw std::runtime_error( "Pipeline has not been fitted!" );
        }

    // -------------------------------------------------------------------------
    // Generate the numerical features.

    const auto numerical_features =
        generate_numerical_features( _cmd, _logger, _data_frames, _socket );

    // -------------------------------------------------------------------------
    // If we do not want to score or predict, then we can stop here.

    const bool score =
        _cmd.has( "score_" ) && JSON::get_value<bool>( _cmd, "score_" );

    const bool predict =
        _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" );

    if ( !score && !predict )
        {
            return numerical_features;
        }

    // -------------------------------------------------------------------------
    // Retrieve the categorical features.

    auto categorical_features = get_categorical_features( _cmd, _data_frames );

    categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    // -------------------------------------------------------------------------
    // Calculate the feature statistics, if applicable.

    const auto ncols = numerical_features.size();

    if ( score && ncols > 0 )
        {
            const auto nrows = numerical_features[0]->size();

            calculate_feature_stats(
                numerical_features, nrows, ncols, _cmd, _data_frames );
        }

    //-------------------------------------------------------------------------
    // Generate predictions, if applicable.

    if ( predict && num_predictors_per_set() > 0 )
        {
            return generate_predictions(
                categorical_features, numerical_features );
        }
    else
        {
            return numerical_features;
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

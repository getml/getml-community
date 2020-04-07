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
    init_feature_engineerers( 1, df_fingerprints() );

    init_predictors(
        "feature_selectors_",
        1,
        impl_.feature_selector_impl_,
        fe_fingerprints() );

    init_predictors(
        "predictors_", 1, impl_.predictor_impl_, fe_fingerprints() );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker )
    : impl_( PipelineImpl( _categories ) )
{
    *this = load( _categories, _path, _fe_tracker, _pred_tracker );
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
    const predictors::PredictorImpl& _predictor_impl,
    containers::Features* _features ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    for ( const auto& col : _predictor_impl.numerical_colnames() )
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

    size_t num_bins = 200;

    num_bins = std::min( num_bins, _nrows / 30 );

    num_bins = std::max( num_bins, static_cast<size_t>( 10 ) );

    // ------------------------------------------------------------------------

    scores().from_json_obj( metrics::Summarizer::calculate_feature_correlations(
        _features, _nrows, _ncols, targets ) );

    scores().from_json_obj( metrics::Summarizer::calculate_feature_plots(
        _features, _nrows, _ncols, num_bins, targets ) );

    scores().from_json_obj( feature_names_as_obj() );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_df_fingerprints(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    std::vector<Poco::JSON::Object::Ptr> df_fingerprints;

    const auto population_fingerprint =
        utils::Getter::get( population_name, _data_frames ).fingerprint();

    df_fingerprints.push_back( population_fingerprint );

    for ( const auto& name : peripheral_names )
        {
            const auto fingerprint =
                utils::Getter::get( name, _data_frames ).fingerprint();

            df_fingerprints.push_back( fingerprint );
        }

    return df_fingerprints;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_fe_fingerprints() const
{
    if ( feature_engineerers_.size() == 0 )
        {
            return df_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fe_fingerprints;

    for ( const auto& fe : feature_engineerers_ )
        {
            assert_true( fe );
            fe_fingerprints.push_back( fe->fingerprint() );
        }

    return fe_fingerprints;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_fs_fingerprints() const
{
    if ( feature_selectors_.size() == 0 || feature_selectors_[0].size() == 0 )
        {
            return fe_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fs_fingerprints;

    for ( const auto& vec : feature_selectors_ )
        {
            for ( const auto& fs : vec )
                {
                    assert_true( fs );
                    fs_fingerprints.push_back( fs->fingerprint() );
                }
        }

    return fs_fingerprints;
}

// ----------------------------------------------------------------------

std::pair<Poco::JSON::Object::Ptr, Poco::JSON::Array::Ptr>
Pipeline::extract_schemata(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto peripheral_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    const auto population_schema = population_df.to_schema();

    auto peripheral_schema = Poco::JSON::Array::Ptr( new Poco::JSON::Array );

    for ( const auto& df_name : peripheral_names )
        {
            const auto df = utils::Getter::get( df_name, _data_frames );

            peripheral_schema->add( df.to_schema() );
        }

    return std::make_pair( population_schema, peripheral_schema );
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>> Pipeline::feature_importances(
    const std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>&
        _predictors ) const
{
    // ----------------------------------------------------------------

    assert_true( _predictors.size() == num_targets() );

    // ----------------------------------------------------------------

    const auto n_features = num_features();

    // ----------------------------------------------------------------

    std::vector<std::vector<Float>> fi;

    for ( size_t t = 0; t < _predictors.size(); ++t )
        {
            auto current_fi = std::vector<Float>( n_features );

            if ( _predictors[t].size() == 0 )
                {
                    fi.push_back( current_fi );
                    continue;
                }

            for ( auto& p : _predictors[t] )
                {
                    assert_true( p );

                    const auto fi_for_this_target =
                        p->feature_importances( n_features );

                    assert_true(
                        current_fi.size() == fi_for_this_target.size() );

                    std::transform(
                        current_fi.begin(),
                        current_fi.end(),
                        fi_for_this_target.begin(),
                        current_fi.begin(),
                        std::plus<Float>() );
                }

            const auto n = static_cast<Float>( _predictors[t].size() );

            for ( auto& val : current_fi )
                {
                    val /= n;
                }

            fi.push_back( current_fi );
        }

    // ----------------------------------------------------------------

    assert_true( fi.size() == num_targets() );

    return fi;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::feature_importances_as_obj() const
{
    // ----------------------------------------------------------------

    const auto n_features = num_features();

    // ----------------------------------------------------------------
    // Extract feature importances

    const auto feature_importances_transposed =
        feature_importances( predictors_ );

    assert_true( feature_importances_transposed.size() == num_targets() );

    // ----------------------------------------------------------------
    // Transpose feature importances and transform to arrays.

    if ( feature_importances_transposed.size() == 0 )
        {
            return Poco::JSON::Object();
        }

    Poco::JSON::Array::Ptr feature_importances( new Poco::JSON::Array() );

    for ( std::size_t i = 0; i < n_features; ++i )
        {
            Poco::JSON::Array::Ptr temp( new Poco::JSON::Array() );

            for ( const auto& feat : feature_importances_transposed )
                {
                    assert_true( feat.size() == n_features );
                    temp->add( feat[i] );
                }

            feature_importances->add( temp );
        }

    // ----------------------------------------------------------------
    // Insert array into object and return.

    Poco::JSON::Object obj;

    obj.set( "feature_importances_", feature_importances );

    return obj;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
Pipeline::feature_names() const
{
    std::vector<std::string> autofeatures;

    if ( impl_.predictor_impl_ )
        {
            size_t offset = 0;

            assert_true(
                feature_engineerers_.size() ==
                predictor_impl().autofeatures().size() );

            for ( size_t i = 0; i < feature_engineerers_.size(); ++i )
                {
                    const auto& fe = feature_engineerers_.at( i );

                    const auto& index = predictor_impl().autofeatures().at( i );

                    for ( const auto ix : index )
                        {
                            autofeatures.push_back(
                                "feature_" +
                                std::to_string( offset + ix + 1 ) );
                        }

                    assert_true( fe );

                    offset += fe->num_features();
                }

            return std::make_tuple(
                autofeatures,
                predictor_impl().numerical_colnames(),
                predictor_impl().categorical_colnames() );
        }
    else
        {
            size_t i = 0;

            for ( const auto& fe : feature_engineerers_ )
                {
                    for ( size_t j = 0; j < fe->num_features(); ++j )
                        {
                            autofeatures.push_back(
                                "feature_" + std::to_string( ++i ) );
                        }
                }

            return std::make_tuple(
                autofeatures,
                std::vector<std::string>(),
                std::vector<std::string>() );
        }
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::feature_names_as_obj() const
{
    const auto fn = feature_names();

    Poco::JSON::Array::Ptr all_names( new Poco::JSON::Array() );

    for ( const auto& name : std::get<0>( fn ) )
        {
            all_names->add( name );
        }

    for ( const auto& name : std::get<1>( fn ) )
        {
            all_names->add( name );
        }

    for ( const auto& name : std::get<2>( fn ) )
        {
            all_names->add( name );
        }

    Poco::JSON::Object obj;

    obj.set( "feature_names_", all_names );

    return obj;
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::generate_numerical_features(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const predictors::PredictorImpl& _predictor_impl,
    Poco::Net::StreamSocket* _socket ) const
{
    // -------------------------------------------------------------------------

    assert_true(
        feature_engineerers_.size() == _predictor_impl.autofeatures().size() );

    // -------------------------------------------------------------------------
    // Generate the features.

    auto numerical_features = containers::Features();

    for ( size_t i = 0; i < feature_engineerers_.size(); ++i )
        {
            const auto& fe = feature_engineerers_.at( i );

            const auto& index = _predictor_impl.autofeatures().at( i );

            assert_true( fe );

            auto new_features =
                fe->transform( _cmd, index, _logger, _data_frames, _socket );

            numerical_features.insert(
                numerical_features.end(),
                new_features.begin(),
                new_features.end() );
        }

    // -------------------------------------------------------------------------
    // Add the numerical columns from the population table.

    add_population_cols(
        _cmd, _data_frames, _predictor_impl, &numerical_features );

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
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------

    assert_true( _fe_tracker );
    assert_true( _pred_tracker );

    // -------------------------------------------------------------------------

    targets() = get_targets( _cmd, _data_frames );

    std::tie( population_schema(), peripheral_schema() ) =
        extract_schemata( _cmd, _data_frames );

    df_fingerprints() = extract_df_fingerprints( _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Fit the feature engineering algorithms.

    auto feature_engineerers =
        init_feature_engineerers( num_targets(), df_fingerprints() );

    for ( auto& fe : feature_engineerers )
        {
            assert_true( fe );

            const auto fingerprint = fe->fingerprint();

            const auto retrieved_fe = _fe_tracker->retrieve( fingerprint );

            if ( retrieved_fe )
                {
                    fe = retrieved_fe;
                    continue;
                }

            fe->fit( _cmd, _logger, _data_frames, _socket );

            _fe_tracker->add( fe );
        }

    feature_engineerers_ = std::move( feature_engineerers );

    fe_fingerprints() = extract_fe_fingerprints();

    // -------------------------------------------------------------------------

    make_feature_selector_impl( _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Fit the feature selectors

    auto feature_selectors = init_predictors(
        "feature_selectors_",
        num_targets(),
        impl_.feature_selector_impl_,
        fe_fingerprints() );

    fit_predictors(
        _cmd,
        _logger,
        _data_frames,
        _pred_tracker,
        feature_selector_impl(),
        &feature_selectors,
        _socket );

    feature_selectors_ = std::move( feature_selectors );

    fs_fingerprints() = extract_fs_fingerprints();

    // -------------------------------------------------------------------------
    // Prepare the predictor - this also uses the results from the feature
    // selection.

    make_predictor_impl( _cmd, _data_frames );

    // -------------------------------------------------------------------------
    // Fit the predictors.

    auto predictors = init_predictors(
        "predictors_",
        num_targets(),
        impl_.predictor_impl_,
        fs_fingerprints() );

    fit_predictors(
        _cmd,
        _logger,
        _data_frames,
        _pred_tracker,
        predictor_impl(),
        &predictors,
        _socket );

    predictors_ = std::move( predictors );

    // -------------------------------------------------------------------------
    // Store the feature importances.

    scores().from_json_obj( feature_importances_as_obj() );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::fit_predictors(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const monitoring::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const predictors::PredictorImpl& _predictor_impl,
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>*
        _predictors,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    const auto all_retrieved =
        retrieve_predictors( _pred_tracker, _predictors );

    if ( all_retrieved )
        {
            return;
        }

    // --------------------------------------------------------------------

    auto categorical_features =
        get_categorical_features( _cmd, _data_frames, _predictor_impl );

    categorical_features =
        _predictor_impl.transform_encodings( categorical_features );

    // --------------------------------------------------------------------

    const auto numerical_features = generate_numerical_features(
        _cmd, _logger, _data_frames, _predictor_impl, _socket );

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

                    // If p is already fitted, that is because it has been
                    // retrieved.
                    if ( p->is_fitted() )
                        {
                            continue;
                        }

                    p->fit(
                        _logger,
                        categorical_features,
                        numerical_features,
                        target_col );

                    _pred_tracker->add( p );
                }
        }

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures Pipeline::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const predictors::PredictorImpl& _predictor_impl ) const
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

    for ( const auto& col : _predictor_impl.categorical_colnames() )
        {
            categorical_features.push_back(
                population_df.categorical( col ).data_ptr() );
        }

    return categorical_features;
}

// ----------------------------------------------------------------------

std::vector<std::string> Pipeline::get_targets(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames ) const
{
    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto population_df =
        utils::Getter::get( population_name, _data_frames );

    auto target_names = std::vector<std::string>( population_df.num_targets() );

    for ( size_t i = 0; i < population_df.num_targets(); ++i )
        {
            target_names[i] = population_df.target( i ).name();
        }

    return target_names;
}

// ----------------------------------------------------------------------

std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
Pipeline::init_feature_engineerers(
    const size_t _num_targets,
    const std::vector<Poco::JSON::Object::Ptr>& _df_fingerprints ) const
{
    // ----------------------------------------------------------------------

    const auto population = std::make_shared<Poco::JSON::Object>(
        *JSON::get_object( obj(), "population_" ) );

    const auto peripheral = parse_peripheral();

    const auto obj_vector = JSON::array_to_obj_vector(
        JSON::get_array( obj(), "feature_engineerers_" ) );

    // ----------------------------------------------------------------------

    std::vector<std::shared_ptr<featureengineerers::AbstractFeatureEngineerer>>
        feature_engineerers;

    for ( auto ptr : obj_vector )
        {
            // --------------------------------------------------------------

            assert_true( ptr );

            // --------------------------------------------------------------

            auto new_feature_engineerer =
                featureengineerers::FeatureEngineererParser::parse(
                    *ptr,
                    population,
                    peripheral,
                    categories(),
                    _df_fingerprints );

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
                                        categories(),
                                        _df_fingerprints ) );
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
    const std::string& _elem,
    const size_t _num_targets,
    const std::shared_ptr<const predictors::PredictorImpl>& _predictor_impl,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies ) const
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
                        *ptr, _predictor_impl, categories(), _dependencies );

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
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker ) const
{
    // ------------------------------------------------------------------

    assert_true( _fe_tracker );

    assert_true( _pred_tracker );

    // ------------------------------------------------------------------

    const auto obj = load_json_obj( _path + "obj.json" );

    const auto pipeline_json = load_json_obj( _path + "pipeline.json" );

    const auto scores =
        metrics::Scores( load_json_obj( _path + "scores.json" ) );

    auto feature_selector_impl = std::shared_ptr<predictors::PredictorImpl>();

    auto predictor_impl = std::shared_ptr<predictors::PredictorImpl>();

    feature_selector_impl = std::make_shared<predictors::PredictorImpl>(
        load_json_obj( _path + "feature-selector-impl.json" ) );

    predictor_impl = std::make_shared<predictors::PredictorImpl>(
        load_json_obj( _path + "predictor-impl.json" ) );

    // ------------------------------------------------------------

    const auto df_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( pipeline_json, "df_fingerprints_" ) );

    const auto fe_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( pipeline_json, "fe_fingerprints_" ) );

    const auto fs_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( pipeline_json, "fs_fingerprints_" ) );

    const auto target_names = JSON::array_to_vector<std::string>(
        JSON::get_array( pipeline_json, "targets_" ) );

    // ------------------------------------------------------------------

    auto pipeline = Pipeline( _categories, obj );

    pipeline.allow_http() =
        JSON::get_value<bool>( pipeline_json, "allow_http_" );

    pipeline.df_fingerprints() = df_fingerprints;

    pipeline.fe_fingerprints() = fe_fingerprints;

    pipeline.fs_fingerprints() = fs_fingerprints;

    pipeline.peripheral_schema() =
        JSON::get_array( pipeline_json, "peripheral_schema_" );

    pipeline.population_schema() =
        JSON::get_object( pipeline_json, "population_schema_" );

    pipeline.targets() = target_names;

    pipeline.obj() = obj;

    pipeline.impl_.feature_selector_impl_ = feature_selector_impl;

    pipeline.impl_.predictor_impl_ = predictor_impl;

    pipeline.scores() = scores;

    // ------------------------------------------------------------
    // Load feature engineerers

    assert_true( _fe_tracker );

    pipeline.feature_engineerers_ = pipeline.init_feature_engineerers(
        pipeline.num_targets(), pipeline.df_fingerprints() );

    for ( size_t i = 0; i < pipeline.feature_engineerers_.size(); ++i )
        {
            auto& fe = pipeline.feature_engineerers_[i];

            assert_true( fe );

            fe->load(
                _path + "feature-engineerer-" + std::to_string( i ) + ".json" );

            _fe_tracker->add( fe );
        }

    // ------------------------------------------------------------
    // Load feature selectors

    pipeline.feature_selectors_ = pipeline.init_predictors(
        "feature_selectors_",
        pipeline.num_targets(),
        pipeline.impl_.feature_selector_impl_,
        pipeline.fe_fingerprints() );

    assert_true( pipeline.num_targets() == pipeline.feature_selectors_.size() );

    for ( size_t i = 0; i < pipeline.num_targets(); ++i )
        {
            for ( size_t j = 0; j < pipeline.feature_selectors_[i].size(); ++j )
                {
                    auto& p = pipeline.feature_selectors_[i][j];

                    assert_true( p );

                    p->load(
                        _path + "feature-selector-" + std::to_string( i ) +
                        "-" + std::to_string( j ) );

                    _pred_tracker->add( p );
                }
        }

    // ------------------------------------------------------------
    // Load predictors

    pipeline.predictors_ = pipeline.init_predictors(
        "predictors_",
        pipeline.num_targets(),
        pipeline.impl_.predictor_impl_,
        pipeline.fs_fingerprints() );

    assert_true( pipeline.num_targets() == pipeline.predictors_.size() );

    for ( size_t i = 0; i < pipeline.num_targets(); ++i )
        {
            for ( size_t j = 0; j < pipeline.predictors_[i].size(); ++j )
                {
                    auto& p = pipeline.predictors_[i][j];

                    assert_true( p );

                    p->load(
                        _path + "predictor-" + std::to_string( i ) + "-" +
                        std::to_string( j ) );

                    _pred_tracker->add( p );
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

void Pipeline::make_feature_selector_impl(
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

    std::vector<size_t> num_autofeatures;

    for ( const auto& fe : feature_engineerers_ )
        {
            assert_true( fe );

            num_autofeatures.push_back( fe->num_features() );
        }

    // --------------------------------------------------------------------

    const auto local_fs_impl = std::make_shared<predictors::PredictorImpl>(
        num_autofeatures, categorical_colnames, numerical_colnames );

    impl_.feature_selector_impl_ = local_fs_impl;

    // --------------------------------------------------------------------

    auto categorical_features =
        get_categorical_features( _cmd, _data_frames, *local_fs_impl );

    local_fs_impl->fit_encodings( categorical_features );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void Pipeline::make_predictor_impl(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames )
{
    // --------------------------------------------------------------------

    const auto local_predictor_impl =
        std::make_shared<predictors::PredictorImpl>( feature_selector_impl() );

    impl_.predictor_impl_ = local_predictor_impl;

    // --------------------------------------------------------------------

    if ( feature_selectors_.size() == 0 || feature_selectors_[0].size() == 0 )
        {
            return;
        }

    const auto share_selected_features =
        JSON::get_value<Float>( obj(), "share_selected_features_" );

    if ( share_selected_features <= 0.0 )
        {
            return;
        }

    // --------------------------------------------------------------------
    // Get importances

    const auto importances = feature_importances( feature_selectors_ );

    assert_true( importances.size() == feature_selectors_.size() );

    auto sum_importances = importances[0];

    for ( size_t i = 1; i < importances.size(); ++i )
        {
            assert_true( sum_importances.size() == importances[i].size() );

            std::transform(
                importances.at( i ).begin(),
                importances.at( i ).end(),
                sum_importances.begin(),
                sum_importances.begin(),
                std::plus<Float>() );
        }

    // --------------------------------------------------------------------
    // Make index

    auto pairs = std::vector<std::pair<size_t, Float>>();

    for ( size_t i = 0; i < sum_importances.size(); ++i )
        {
            pairs.emplace_back( std::make_pair( i, sum_importances[i] ) );
        }

    std::sort(
        pairs.begin(),
        pairs.end(),
        []( const std::pair<size_t, Float>& p1,
            const std::pair<size_t, Float>& p2 ) {
            return p1.second > p2.second;
        } );

    auto index = std::vector<size_t>( pairs.size() );

    for ( size_t i = 0; i < index.size(); ++i )
        {
            index.at( i ) = pairs.at( i ).first;
        }

    // --------------------------------------------------------------------

    const auto n_selected = std::max(
        static_cast<size_t>( 1 ),
        static_cast<size_t>( index.size() * share_selected_features ) );

    local_predictor_impl->select_features( n_selected, index );

    // --------------------------------------------------------------------

    auto categorical_features =
        get_categorical_features( _cmd, _data_frames, *local_predictor_impl );

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

bool Pipeline::retrieve_predictors(
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>*
        _predictors ) const
{
    bool all_retrieved = true;

    for ( auto& vec : *_predictors )
        {
            for ( auto& p : vec )
                {
                    assert_true( p );

                    const auto ptr =
                        _pred_tracker->retrieve( p->fingerprint() );

                    if ( ptr )
                        {
                            p = ptr;
                        }
                    else
                        {
                            all_retrieved = false;
                        }
                }
        }

    return all_retrieved;
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

    pipeline_json.set(
        "df_fingerprints_", JSON::vector_to_array( df_fingerprints() ) );

    pipeline_json.set(
        "fe_fingerprints_", JSON::vector_to_array( fe_fingerprints() ) );

    pipeline_json.set(
        "fs_fingerprints_", JSON::vector_to_array( fs_fingerprints() ) );

    pipeline_json.set( "targets_", JSON::vector_to_array( targets() ) );

    pipeline_json.set( "peripheral_schema_", peripheral_schema() );

    pipeline_json.set( "population_schema_", population_schema() );

    save_json_obj( pipeline_json, tfile.path() + "/pipeline.json" );

    // ------------------------------------------------------------------

    save_json_obj( obj(), tfile.path() + "/obj.json" );

    // ------------------------------------------------------------------

    scores().save( tfile.path() + "/scores.json" );

    // ------------------------------------------------------------------

    feature_selector_impl().save(
        tfile.path() + "/feature-selector-impl.json" );

    predictor_impl().save( tfile.path() + "/predictor-impl.json" );

    // ------------------------------------------------------------------

    for ( size_t i = 0; i < feature_selectors_.size(); ++i )
        {
            for ( size_t j = 0; j < feature_selectors_[i].size(); ++j )
                {
                    const auto& p = feature_selectors_[i][j];

                    if ( !p )
                        {
                            throw std::invalid_argument(
                                "Feature selector " + std::to_string( i ) +
                                "-" + std::to_string( j ) +
                                " has not been fitted!" );
                        }

                    p->save(
                        tfile.path() + "/feature-selector-" +
                        std::to_string( i ) + "-" + std::to_string( j ) );
                }
        }

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

    Poco::JSON::Object json_obj;

    json_obj.set( "name_", _name );

    json_obj.set( "allow_http_", allow_http() );

    json_obj.set(
        "feature_engineerers_",
        JSON::get_array( obj(), "feature_engineerers_" ) );

    json_obj.set(
        "feature_selectors_", JSON::get_array( obj(), "feature_selectors_" ) );

    json_obj.set( "num_features_", num_features() );

    json_obj.set( "peripheral_schema_", peripheral_schema() );

    json_obj.set( "population_schema_", population_schema() );

    json_obj.set( "population_", JSON::get_object( obj(), "population_" ) );

    json_obj.set( "predictors_", JSON::get_array( obj(), "predictors_" ) );

    json_obj.set( "scores_", scores().to_json_obj() );

    json_obj.set( "sql_", to_sql_arr() );

    json_obj.set( "targets_", JSON::vector_to_array( targets() ) );

    return json_obj;
}

// ----------------------------------------------------------------------------

std::string Pipeline::to_sql() const
{
    std::string sql;

    size_t offset = 0;

    for ( const auto& fe : feature_engineerers_ )
        {
            const auto vec = fe->to_sql( offset, true );

            for ( const auto& str : vec )
                {
                    sql += str;
                }

            offset += fe->num_features();
        }

    return sql;
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Pipeline::to_sql_arr() const
{
    auto sql = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    size_t offset = 0;

    assert_true(
        feature_engineerers_.size() == predictor_impl().autofeatures().size() );

    for ( size_t i = 0; i < feature_engineerers_.size(); ++i )
        {
            const auto& fe = feature_engineerers_.at( i );

            const auto& index = predictor_impl().autofeatures().at( i );

            const auto vec = fe->to_sql( offset, false );

            for ( const auto ix : index )
                {
                    assert_true( ix < vec.size() );
                    sql->add( vec.at( ix ) );
                }

            offset += fe->num_features();
        }

    const auto population = JSON::get_object( obj(), "population_" );

    const auto tname = JSON::get_value<std::string>( *population, "name_" );

    const auto [autofeature, numerical, categorical] = feature_names();

    for ( const auto& cname : numerical )
        {
            sql->add( "SELECT " + cname + " FROM " + tname + ";" );
        }

    for ( const auto& cname : categorical )
        {
            sql->add( "SELECT " + cname + " FROM " + tname + ";" );
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

    const auto numerical_features = generate_numerical_features(
        _cmd, _logger, _data_frames, predictor_impl(), _socket );

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

    auto categorical_features =
        get_categorical_features( _cmd, _data_frames, predictor_impl() );

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

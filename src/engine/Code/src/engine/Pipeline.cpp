#include "engine/pipelines/pipelines.hpp"

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

Pipeline::Pipeline( const Poco::JSON::Object& _obj )
    : impl_( PipelineImpl( _obj ) )

{
    // This won't to anything - the point it to make sure that it can be
    // parsed correctly.
    init_preprocessors( df_fingerprints() );

    init_feature_learners( 1, preprocessor_fingerprints() );

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
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker )
    : impl_( PipelineImpl() )
{
    *this = load( _path, _fe_tracker, _pred_tracker );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline( const Pipeline& _other ) : impl_( _other.impl_ )
{
    feature_learners_ = clone( _other.feature_learners_ );

    feature_selectors_ = clone( _other.feature_selectors_ );

    predictors_ = clone( _other.predictors_ );

    preprocessors_ = clone( _other.preprocessors_ );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline( Pipeline&& _other ) noexcept
    : impl_( std::move( _other.impl_ ) )
{
    feature_learners_ = std::move( _other.feature_learners_ );

    feature_selectors_ = std::move( _other.feature_selectors_ );

    predictors_ = std::move( _other.predictors_ );

    preprocessors_ = std::move( _other.preprocessors_ );
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

// ----------------------------------------------------------------------------

std::map<std::string, containers::DataFrame> Pipeline::apply_preprocessors(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<const containers::Encoding>& _categories,
    Poco::Net::StreamSocket* _socket ) const
{
    auto data_frames = _data_frames;

    for ( const auto& p : preprocessors_ )
        {
            assert_true( p );

            p->transform( _cmd, _categories, &data_frames );
        }

    return data_frames;
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

    size_t num_bins = 30;

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

// ------------------------------------------------------------------------

std::vector<size_t> Pipeline::calculate_importance_index() const
{
    const auto sum_importances = calculate_sum_importances();

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

    return index;
}

// ------------------------------------------------------------------------

std::vector<Float> Pipeline::calculate_sum_importances() const
{
    const auto importances = feature_importances( feature_selectors_ );

    assert_true( importances.size() == feature_selectors_.size() );

    auto sum_importances = importances.at( 0 );

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

    return sum_importances;
}

// ----------------------------------------------------------------------------

void Pipeline::check(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    Poco::Net::StreamSocket* _socket ) const
{
    // ----------------------------------------------------------------------

    const auto population_placeholder = std::make_shared<Poco::JSON::Object>(
        *JSON::get_object( obj(), "population_" ) );

    const auto peripheral_names = parse_peripheral();

    // -------------------------------------------------------------------------

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto peripheral_df_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "peripheral_names_" ) );

    const auto population = utils::Getter::get( population_name, _data_frames );

    auto peripheral = std::vector<containers::DataFrame>();

    for ( const auto& df_name : peripheral_df_names )
        {
            const auto df = utils::Getter::get( df_name, _data_frames );

            peripheral.push_back( df );
        }

    // -------------------------------------------------------------------------

    const auto [feature_learners, target_nums] =
        init_feature_learners( 1, df_fingerprints() );

    // -------------------------------------------------------------------------

    preprocessors::DataModelChecker::check(
        population_placeholder,
        peripheral_names,
        population,
        peripheral,
        feature_learners,
        _logger,
        _socket );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<
    std::vector<helpers::ColumnDescription>,
    std::vector<std::vector<Float>>>
Pipeline::column_importances() const
{
    auto c_desc = std::vector<helpers::ColumnDescription>();

    auto c_importances = std::vector<std::vector<Float>>();

    if ( predictors_.size() == 0 )
        {
            return std::make_pair( c_desc, c_importances );
        }

    const auto f_importances = feature_importances( predictors_ );

    auto importance_makers =
        std::vector<helpers::ImportanceMaker>( f_importances.size() );

    column_importances_auto( f_importances, &importance_makers );

    column_importances_manual( f_importances, &importance_makers );

    for ( const auto& i_maker : importance_makers )
        {
            extract_coldesc( i_maker.importances(), &c_desc );
            extract_importance_values( i_maker.importances(), &c_importances );
        }

    return std::make_pair( c_desc, c_importances );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::column_importances_as_obj() const
{
    // ----------------------------------------------------------------

    const auto [c_desc, c_importances] = column_importances();

    // ----------------------------------------------------------------

    if ( c_importances.size() == 0 )
        {
            return Poco::JSON::Object();
        }

    // ----------------------------------------------------------------

    auto column_descriptions =
        Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    for ( const auto& desc : c_desc )
        {
            column_descriptions->add( desc.to_json_obj() );
        }

    // ----------------------------------------------------------------

    const auto column_importances = transpose( c_importances );

    // ----------------------------------------------------------------

    Poco::JSON::Object obj;

    obj.set( "column_descriptions_", column_descriptions );

    obj.set( "column_importances_", column_importances );

    return obj;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::column_importances_auto(
    const std::vector<std::vector<Float>>& _f_importances,
    std::vector<helpers::ImportanceMaker>* _importance_makers ) const
{
    assert_true( _f_importances.size() == _importance_makers->size() );

    const auto autofeatures = predictor_impl().autofeatures();

    assert_true( autofeatures.size() == feature_learners_.size() );

    for ( size_t i = 0; i < _f_importances.size(); ++i )
        {
            const auto& f_imp_for_target = _f_importances.at( i );

            size_t ix_begin = 0;

            for ( size_t j = 0; j < feature_learners_.size(); ++j )
                {
                    const auto& fl = feature_learners_.at( j );

                    assert_true( fl );

                    const auto ix_end = ix_begin + autofeatures.at( j ).size();

                    const auto importance_factors = make_importance_factors(
                        fl->num_features(),
                        autofeatures.at( j ),
                        f_imp_for_target.begin() + ix_begin,
                        f_imp_for_target.begin() + ix_end );

                    ix_begin = ix_end;

                    const auto c_imp_for_target =
                        fl->column_importances( importance_factors );

                    _importance_makers->at( i ).merge( c_imp_for_target );
                }
        }
}

// ----------------------------------------------------------------------------

void Pipeline::column_importances_manual(
    const std::vector<std::vector<Float>>& _f_importances,
    std::vector<helpers::ImportanceMaker>* _importance_makers ) const
{
    assert_true( _f_importances.size() == _importance_makers->size() );

    for ( size_t i = 0; i < _f_importances.size(); ++i )
        {
            const auto& f_imp_for_target = _f_importances.at( i );

            auto j = predictor_impl().num_autofeatures();

            assert_true(
                j + predictor_impl().num_manual_features() ==
                f_imp_for_target.size() );

            const auto population_name = parse_population();

            assert_true( population_name );

            for ( const auto& colname : predictor_impl().numerical_colnames() )
                {
                    const auto desc = helpers::ColumnDescription(
                        _importance_makers->at( i ).population(),
                        *population_name,
                        colname );

                    const auto value = f_imp_for_target.at( j++ );

                    _importance_makers->at( i ).add_to_importances(
                        desc, value );
                }

            for ( const auto& colname :
                  predictor_impl().categorical_colnames() )
                {
                    const auto desc = helpers::ColumnDescription(
                        _importance_makers->at( i ).population(),
                        *population_name,
                        colname );

                    const auto value = f_imp_for_target.at( j++ );

                    _importance_makers->at( i ).add_to_importances(
                        desc, value );
                }
        }
}

// ----------------------------------------------------------------------------

void Pipeline::extract_coldesc(
    const std::map<helpers::ColumnDescription, Float>& _column_importances,
    std::vector<helpers::ColumnDescription>* _coldesc ) const
{
    if ( _coldesc->size() == 0 )
        {
            for ( const auto& [key, _] : _column_importances )
                {
                    _coldesc->push_back( key );
                }
        }
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
    if ( feature_learners_.size() == 0 )
        {
            return preprocessor_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fe_fingerprints;

    for ( const auto& fe : feature_learners_ )
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

// ----------------------------------------------------------------------------

void Pipeline::extract_importance_values(
    const std::map<helpers::ColumnDescription, Float>& _column_importances,
    std::vector<std::vector<Float>>* _all_column_importances ) const
{
    auto importance_values = std::vector<Float>();

    for ( const auto& [_, value] : _column_importances )
        {
            importance_values.push_back( value );
        }

    _all_column_importances->push_back( importance_values );
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr>
Pipeline::extract_preprocessor_fingerprints() const
{
    if ( preprocessors_.size() == 0 )
        {
            return df_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fingerprints;

    for ( const auto& p : preprocessors_ )
        {
            assert_true( p );
            fingerprints.push_back( p->fingerprint() );
        }

    return fingerprints;
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

    const auto feature_importances_transposed =
        feature_importances( predictors_ );

    assert_true( feature_importances_transposed.size() == num_targets() );

    // ----------------------------------------------------------------

    if ( feature_importances_transposed.size() == 0 )
        {
            return Poco::JSON::Object();
        }

    // ----------------------------------------------------------------

    const auto feature_importances =
        transpose( feature_importances_transposed );

    // ----------------------------------------------------------------

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
            assert_true(
                feature_learners_.size() ==
                predictor_impl().autofeatures().size() );

            for ( size_t i = 0; i < feature_learners_.size(); ++i )
                {
                    const auto& fe = feature_learners_.at( i );

                    const auto& index = predictor_impl().autofeatures().at( i );

                    for ( const auto ix : index )
                        {
                            autofeatures.push_back(
                                "feature_" + std::to_string( i + 1 ) + "_" +
                                std::to_string( ix + 1 ) );
                        }

                    assert_true( fe );
                }

            return std::make_tuple(
                autofeatures,
                predictor_impl().numerical_colnames(),
                predictor_impl().categorical_colnames() );
        }
    else
        {
            size_t i = 0;

            for ( const auto& fe : feature_learners_ )
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

containers::Features Pipeline::generate_autofeatures(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const predictors::PredictorImpl& _predictor_impl,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    assert_true(
        feature_learners_.size() == _predictor_impl.autofeatures().size() );

    // -------------------------------------------------------------------------

    auto autofeatures = containers::Features();

    for ( size_t i = 0; i < feature_learners_.size(); ++i )
        {
            const auto& fe = feature_learners_.at( i );

            const auto socket_logger =
                std::make_shared<const communication::SocketLogger>(
                    _logger, fe->silent(), _socket );

            const auto& index = _predictor_impl.autofeatures().at( i );

            assert_true( fe );

            auto new_features =
                fe->transform( _cmd, index, socket_logger, _data_frames );

            autofeatures.insert(
                autofeatures.end(), new_features.begin(), new_features.end() );
        }

    // -------------------------------------------------------------------------

    return autofeatures;

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
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<containers::Encoding>& _categories,
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

    const auto data_frames =
        fit_preprocessors( _cmd, _data_frames, _categories, _socket );

    // -------------------------------------------------------------------------

    fit_feature_learners( _cmd, _logger, data_frames, _fe_tracker, _socket );

    // -------------------------------------------------------------------------

    make_feature_selector_impl( _cmd, data_frames );

    // -------------------------------------------------------------------------

    containers::Features autofeatures;

    // -------------------------------------------------------------------------

    auto feature_selectors = init_predictors(
        "feature_selectors_",
        num_targets(),
        impl_.feature_selector_impl_,
        fe_fingerprints() );

    fit_predictors(
        _cmd,
        _logger,
        data_frames,
        _pred_tracker,
        feature_selector_impl(),
        "feature selector",
        &autofeatures,
        &feature_selectors,
        _socket );

    feature_selectors_ = std::move( feature_selectors );

    fs_fingerprints() = extract_fs_fingerprints();

    // -------------------------------------------------------------------------
    // Prepare the predictor impl - this also uses the results from the feature
    // selection.

    make_predictor_impl( _cmd, data_frames );

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
        data_frames,
        _pred_tracker,
        predictor_impl(),
        "predictor",
        &autofeatures,
        &predictors,
        _socket );

    predictors_ = std::move( predictors );

    // -------------------------------------------------------------------------

    scores().from_json_obj( column_importances_as_obj() );

    scores().from_json_obj( feature_importances_as_obj() );

    scores().from_json_obj( feature_names_as_obj() );

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::fit_feature_learners(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    Poco::Net::StreamSocket* _socket )
{
    auto [feature_learners, target_nums] =
        init_feature_learners( num_targets(), preprocessor_fingerprints() );

    assert_true( feature_learners.size() == target_nums.size() );

    for ( size_t i = 0; i < feature_learners.size(); ++i )
        {
            auto& fe = feature_learners.at( i );

            const auto socket_logger =
                std::make_shared<const communication::SocketLogger>(
                    _logger, fe->silent(), _socket );

            assert_true( fe );

            const auto fingerprint = fe->fingerprint();

            const auto retrieved_fe = _fe_tracker->retrieve( fingerprint );

            if ( retrieved_fe )
                {
                    socket_logger->log(
                        "Retrieving features (because a similar feature "
                        "learner has already been fitted)..." );

                    socket_logger->log( "Progress: 100%." );

                    fe = retrieved_fe;

                    continue;
                }

            fe->fit( _cmd, socket_logger, _data_frames, target_nums.at( i ) );

            _fe_tracker->add( fe );
        }

    feature_learners_ = std::move( feature_learners );

    fe_fingerprints() = extract_fe_fingerprints();
}

// ----------------------------------------------------------------------------

void Pipeline::fit_predictors(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const predictors::PredictorImpl& _predictor_impl,
    const std::string& _purpose,
    containers::Features* _autofeatures,
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

    if ( _autofeatures->size() == 0 )
        {
            *_autofeatures = generate_autofeatures(
                _cmd, _logger, _data_frames, _predictor_impl, _socket );
        }
    else
        {
            *_autofeatures =
                select_autofeatures( *_autofeatures, _predictor_impl );
        }

    const auto numerical_features = get_numerical_features(
        *_autofeatures, _cmd, _data_frames, _predictor_impl );

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

                    const auto socket_logger =
                        std::make_shared<const communication::SocketLogger>(
                            _logger, p->silent(), _socket );

                    // If p is already fitted, that is because it has been
                    // retrieved.
                    if ( p->is_fitted() )
                        {
                            socket_logger->log( "Retrieving predictor..." );

                            socket_logger->log( "Progress: 100%." );

                            continue;
                        }

                    socket_logger->log(
                        p->type() + ": Training as " + _purpose + "..." );

                    p->fit(
                        socket_logger,
                        categorical_features,
                        numerical_features,
                        target_col );

                    _pred_tracker->add( p );
                }
        }

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::map<std::string, containers::DataFrame> Pipeline::fit_preprocessors(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<containers::Encoding>& _categories,
    Poco::Net::StreamSocket* _socket )
{
    auto data_frames = _data_frames;

    auto preprocessors = init_preprocessors( df_fingerprints() );

    for ( auto& p : preprocessors )
        {
            assert_true( p );

            p->fit_transform( _cmd, _categories, &data_frames );
        }

    preprocessors_ = preprocessors;

    preprocessor_fingerprints() = extract_preprocessor_fingerprints();

    return data_frames;
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures Pipeline::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const predictors::PredictorImpl& _predictor_impl ) const
{
    auto categorical_features = containers::CategoricalFeatures();

    if ( !impl_.include_categorical_ )
        {
            return categorical_features;
        }

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

// ----------------------------------------------------------------------------

containers::Features Pipeline::get_numerical_features(
    const containers::Features& _autofeatures,
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const predictors::PredictorImpl& _predictor_impl ) const
{
    // --------------------------------------------------------------------

    auto numerical_features = _autofeatures;

    // -------------------------------------------------------------------------

    add_population_cols(
        _cmd, _data_frames, _predictor_impl, &numerical_features );

    // -------------------------------------------------------------------------

    return numerical_features;

    // -------------------------------------------------------------------------
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

std::pair<
    std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>,
    std::vector<Int>>
Pipeline::init_feature_learners(
    const size_t _num_targets,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies ) const
{
    // ----------------------------------------------------------------------

    if ( _num_targets == 0 )
        {
            throw std::invalid_argument(
                "You must provide at least one target." );
        }

    // ----------------------------------------------------------------------

    const auto population = std::make_shared<Poco::JSON::Object>(
        *JSON::get_object( obj(), "population_" ) );

    const auto peripheral = parse_peripheral();

    const auto obj_vector = JSON::array_to_obj_vector(
        JSON::get_array( obj(), "feature_learners_" ) );

    // ----------------------------------------------------------------------

    std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
        feature_learners;

    std::vector<Int> target_nums;

    for ( auto ptr : obj_vector )
        {
            // --------------------------------------------------------------

            assert_true( ptr );

            // --------------------------------------------------------------

            auto new_feature_learner =
                featurelearners::FeatureLearnerParser::parse(
                    *ptr, population, peripheral, _dependencies );

            // --------------------------------------------------------------

            if ( new_feature_learner->supports_multiple_targets() )
                {
                    feature_learners.emplace_back(
                        std::move( new_feature_learner ) );

                    target_nums.push_back(
                        featurelearners::AbstractFeatureLearner::
                            USE_ALL_TARGETS );
                }
            else
                {
                    for ( size_t t = 0; t < _num_targets; ++t )
                        {
                            auto obj = Poco::JSON::Object::Ptr(
                                new Poco::JSON::Object() );

                            obj->set( "target_num_", t );

                            auto dependencies = _dependencies;

                            dependencies.push_back( obj );

                            feature_learners.emplace_back(
                                featurelearners::FeatureLearnerParser::parse(
                                    *ptr,
                                    population,
                                    peripheral,
                                    dependencies ) );

                            target_nums.push_back( static_cast<Int>( t ) );
                        }
                }

            // --------------------------------------------------------------
        }

    // ----------------------------------------------------------------------

    return std::make_pair( feature_learners, target_nums );

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
                        *ptr, _predictor_impl, _dependencies );

                    predictors_for_target.emplace_back(
                        std::move( new_predictor ) );
                }

            predictors.emplace_back( std::move( predictors_for_target ) );
        }

    // --------------------------------------------------------------------

    return predictors;

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------

std::vector<std::shared_ptr<preprocessors::Preprocessor>>
Pipeline::init_preprocessors(
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies ) const
{
    auto vec = std::vector<std::shared_ptr<preprocessors::Preprocessor>>();

    if ( !obj().has( "preprocessors_" ) )
        {
            return vec;
        }

    const auto arr =
        jsonutils::JSON::get_object_array( obj(), "preprocessors_" );

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            const auto ptr = arr->getObject( i );

            assert_true( ptr );

            vec.push_back( preprocessors::PreprocessorParser::parse(
                *ptr, df_fingerprints() ) );
        }

    return vec;
}

// ----------------------------------------------------------------------------

bool Pipeline::is_classification() const
{
    // -----------------------------------------------------------------------

    const auto fl_is_cl =
        []( const std::shared_ptr<featurelearners::AbstractFeatureLearner>&
                fe ) {
            assert_true( fe );
            return fe->is_classification();
        };

    const auto pred_is_cl =
        []( const std::shared_ptr<predictors::Predictor>& pred ) {
            assert_true( pred );
            return pred->is_classification();
        };

    // -----------------------------------------------------------------------

    bool all_classifiers = std::all_of(
        feature_learners_.begin(), feature_learners_.end(), fl_is_cl );

    for ( const auto& fs : feature_selectors_ )
        {
            all_classifiers = all_classifiers &&
                              std::all_of( fs.begin(), fs.end(), pred_is_cl );
        }

    for ( const auto& p : predictors_ )
        {
            all_classifiers = all_classifiers &&
                              std::all_of( p.begin(), p.end(), pred_is_cl );
        }

    // -----------------------------------------------------------------------

    bool all_regressors = std::none_of(
        feature_learners_.begin(), feature_learners_.end(), fl_is_cl );

    for ( const auto& fs : feature_selectors_ )
        {
            all_regressors = all_regressors &&
                             std::none_of( fs.begin(), fs.end(), pred_is_cl );
        }

    for ( const auto& p : predictors_ )
        {
            all_regressors = all_regressors &&
                             std::none_of( p.begin(), p.end(), pred_is_cl );
        }

    // -----------------------------------------------------------------------

    if ( !all_classifiers && !all_regressors )
        {
            throw std::invalid_argument(
                "You are mixing classification and regression algorithms. "
                "Please make sure that all of your feature learners, feature "
                "selectors and predictors are either all regression algorithms "
                "or all classifications algorithms." );
        }

    if ( all_classifiers == all_regressors )
        {
            throw std::invalid_argument(
                "The pipelines needs at least one feature learner, feature "
                "selector or predictor." );
        }

    // -----------------------------------------------------------------------

    return all_classifiers;

    // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Pipeline Pipeline::load(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker ) const
{
    // ------------------------------------------------------------------

    assert_true( _fe_tracker );

    assert_true( _pred_tracker );

    // ------------------------------------------------------------------

    const auto obj = load_json_obj( _path + "obj.json" );

    const auto scores =
        metrics::Scores( load_json_obj( _path + "scores.json" ) );

    // ------------------------------------------------------------------

    auto pipeline = Pipeline( obj );

    pipeline.obj() = obj;

    pipeline.scores() = scores;

    // ------------------------------------------------------------

    load_pipeline_json( _path, &pipeline );

    load_impls( _path, &pipeline );

    load_preprocessors( _path, &pipeline );

    load_feature_learners( _path, _fe_tracker, &pipeline );

    load_feature_selectors( _path, _pred_tracker, &pipeline );

    load_predictors( _path, _pred_tracker, &pipeline );

    // ------------------------------------------------------------

    return pipeline;

    // ------------------------------------------------------------
}

// ------------------------------------------------------------------------

void Pipeline::load_feature_learners(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    Pipeline* _pipeline ) const
{
    assert_true( _fe_tracker );

    _pipeline->feature_learners_ =
        std::get<0>( _pipeline->init_feature_learners(
            _pipeline->num_targets(),
            _pipeline->preprocessor_fingerprints() ) );

    for ( size_t i = 0; i < _pipeline->feature_learners_.size(); ++i )
        {
            auto& fe = _pipeline->feature_learners_.at( i );

            assert_true( fe );

            fe->load(
                _path + "feature-learner-" + std::to_string( i ) + ".json" );

            _fe_tracker->add( fe );
        }
}

// ------------------------------------------------------------------------

void Pipeline::load_feature_selectors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    Pipeline* _pipeline ) const
{
    _pipeline->feature_selectors_ = _pipeline->init_predictors(
        "feature_selectors_",
        _pipeline->num_targets(),
        _pipeline->impl_.feature_selector_impl_,
        _pipeline->fe_fingerprints() );

    assert_true(
        _pipeline->num_targets() == _pipeline->feature_selectors_.size() );

    for ( size_t i = 0; i < _pipeline->num_targets(); ++i )
        {
            for ( size_t j = 0;
                  j < _pipeline->feature_selectors_.at( i ).size();
                  ++j )
                {
                    auto& p = _pipeline->feature_selectors_.at( i ).at( j );

                    assert_true( p );

                    p->load(
                        _path + "feature-selector-" + std::to_string( i ) +
                        "-" + std::to_string( j ) );

                    _pred_tracker->add( p );
                }
        }
}

// ------------------------------------------------------------------------

void Pipeline::load_fingerprints(
    const Poco::JSON::Object& _pipeline_json, Pipeline* _pipeline ) const
{
    const auto df_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "df_fingerprints_" ) );

    const auto preprocessor_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "preprocessor_fingerprints_" ) );

    const auto fe_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "fe_fingerprints_" ) );

    const auto fs_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "fs_fingerprints_" ) );

    _pipeline->df_fingerprints() = df_fingerprints;

    _pipeline->preprocessor_fingerprints() = preprocessor_fingerprints;

    _pipeline->fe_fingerprints() = fe_fingerprints;

    _pipeline->fs_fingerprints() = fs_fingerprints;
}

// ------------------------------------------------------------------------

void Pipeline::load_impls( const std::string& _path, Pipeline* _pipeline ) const
{
    const auto feature_selector_impl =
        std::make_shared<predictors::PredictorImpl>(
            load_json_obj( _path + "feature-selector-impl.json" ) );

    const auto predictor_impl = std::make_shared<predictors::PredictorImpl>(
        load_json_obj( _path + "predictor-impl.json" ) );

    _pipeline->impl_.feature_selector_impl_ = feature_selector_impl;

    _pipeline->impl_.predictor_impl_ = predictor_impl;
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

void Pipeline::load_pipeline_json(
    const std::string& _path, Pipeline* _pipeline ) const
{
    const auto pipeline_json = load_json_obj( _path + "pipeline.json" );

    _pipeline->allow_http() =
        JSON::get_value<bool>( pipeline_json, "allow_http_" );

    _pipeline->creation_time() =
        JSON::get_value<std::string>( pipeline_json, "creation_time_" );

    _pipeline->peripheral_schema() =
        JSON::get_array( pipeline_json, "peripheral_schema_" );

    _pipeline->population_schema() =
        JSON::get_object( pipeline_json, "population_schema_" );

    _pipeline->targets() = JSON::array_to_vector<std::string>(
        JSON::get_array( pipeline_json, "targets_" ) );

    load_fingerprints( pipeline_json, _pipeline );
}

// ------------------------------------------------------------------------

void Pipeline::load_predictors(
    const std::string& _path,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    Pipeline* _pipeline ) const
{
    _pipeline->predictors_ = _pipeline->init_predictors(
        "predictors_",
        _pipeline->num_targets(),
        _pipeline->impl_.predictor_impl_,
        _pipeline->fs_fingerprints() );

    assert_true( _pipeline->num_targets() == _pipeline->predictors_.size() );

    for ( size_t i = 0; i < _pipeline->num_targets(); ++i )
        {
            for ( size_t j = 0; j < _pipeline->predictors_.at( i ).size(); ++j )
                {
                    auto& p = _pipeline->predictors_.at( i ).at( j );

                    assert_true( p );

                    p->load(
                        _path + "predictor-" + std::to_string( i ) + "-" +
                        std::to_string( j ) );

                    _pred_tracker->add( p );
                }
        }
}

// ------------------------------------------------------------------------

void Pipeline::load_preprocessors(
    const std::string& _path, Pipeline* _pipeline ) const
{
    _pipeline->preprocessors_ =
        init_preprocessors( _pipeline->df_fingerprints() );

    for ( size_t i = 0; i < _pipeline->preprocessors_.size(); ++i )
        {
            const auto json_obj = load_json_obj(
                _path + "preprocessor-" + std::to_string( i ) + ".json" );

            auto& p = _pipeline->preprocessors_.at( i );

            p = preprocessors::PreprocessorParser::parse(
                json_obj, _pipeline->df_fingerprints() );
        }
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

    const auto is_null = []( const Float val ) {
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    // --------------------------------------------------------------------

    auto categorical_colnames = std::vector<std::string>();

    if ( impl_.include_categorical_ )
        {
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
        }

    // --------------------------------------------------------------------

    auto numerical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < population_df.num_numericals(); ++i )
        {
            if ( population_df.numerical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            const auto contains_null = std::any_of(
                population_df.numerical( i ).begin(),
                population_df.numerical( i ).end(),
                is_null );

            if ( contains_null )
                {
                    continue;
                }

            numerical_colnames.push_back( population_df.numerical( i ).name() );
        }

    // --------------------------------------------------------------------

    std::vector<size_t> num_autofeatures;

    for ( const auto& fe : feature_learners_ )
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

// ----------------------------------------------------------------------------

std::vector<Float> Pipeline::make_importance_factors(
    const size_t _num_features,
    const std::vector<size_t>& _autofeatures,
    const std::vector<Float>::const_iterator _begin,
    const std::vector<Float>::const_iterator _end ) const
{
    auto importance_factors = std::vector<Float>( _num_features );

    assert_true( _end >= _begin );

    assert_true(
        _autofeatures.size() ==
        static_cast<size_t>( std::distance( _begin, _end ) ) );

    for ( size_t i = 0; i < _autofeatures.size(); ++i )
        {
            const auto ix = _autofeatures.at( i );

            assert_true( ix < importance_factors.size() );

            importance_factors.at( ix ) = *( _begin + i );
        }

    return importance_factors;
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

    const auto index = calculate_importance_index();

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

void Pipeline::move_tfile(
    const std::string& _path,
    const std::string& _name,
    Poco::TemporaryFile* _tfile ) const
{
    auto file = Poco::File( _path + _name );

    // Create all parent directories, if necessary.
    file.createDirectories();

    // If the actual folder already exists, delete it.
    file.remove( true );

    _tfile->renameTo( file.path() );

    _tfile->keep();
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

    feature_learners_ = std::move( _other.feature_learners_ );

    feature_selectors_ = std::move( _other.feature_selectors_ );

    predictors_ = std::move( _other.predictors_ );

    preprocessors_ = std::move( _other.preprocessors_ );

    return *this;
}

// ----------------------------------------------------------------------

std::shared_ptr<std::string> Pipeline::parse_population() const
{
    const auto ptr = JSON::get_object( obj(), "population_" );

    if ( !ptr )
        {
            throw std::invalid_argument(
                "'population_' is not a proper JSON object!" );
        }

    const auto name = JSON::get_value<std::string>( *ptr, "name_" );

    return std::make_shared<std::string>( name );
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

    auto tfile = Poco::TemporaryFile( engine::temp_dir );

    tfile.createDirectories();

    // ------------------------------------------------------------------

    save_preprocessors( tfile );

    save_feature_learners( tfile );

    // ------------------------------------------------------------------

    save_pipeline_json( tfile );

    // ------------------------------------------------------------------

    save_json_obj( obj(), tfile.path() + "/obj.json" );

    // ------------------------------------------------------------------

    scores().save( tfile.path() + "/scores.json" );

    // ------------------------------------------------------------------

    feature_selector_impl().save(
        tfile.path() + "/feature-selector-impl.json" );

    predictor_impl().save( tfile.path() + "/predictor-impl.json" );

    // ------------------------------------------------------------------

    save_feature_selectors( tfile );

    save_predictors( tfile );

    // ------------------------------------------------------------------

    move_tfile( _path, _name, &tfile );

    // ------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::save_feature_learners( const Poco::TemporaryFile& _tfile ) const
{
    for ( size_t i = 0; i < feature_learners_.size(); ++i )
        {
            const auto& fe = feature_learners_.at( i );

            if ( !fe )
                {
                    throw std::invalid_argument(
                        "Feature learning algorithm #" + std::to_string( i ) +
                        " has not been fitted!" );
                }

            fe->save(
                _tfile.path() + "/feature-learner-" + std::to_string( i ) +
                ".json" );
        }
}

// ----------------------------------------------------------------------------

void Pipeline::save_feature_selectors( const Poco::TemporaryFile& _tfile ) const
{
    for ( size_t i = 0; i < feature_selectors_.size(); ++i )
        {
            for ( size_t j = 0; j < feature_selectors_.at( i ).size(); ++j )
                {
                    const auto& p = feature_selectors_.at( i ).at( j );

                    if ( !p )
                        {
                            throw std::invalid_argument(
                                "Feature selector " + std::to_string( i ) +
                                "-" + std::to_string( j ) +
                                " has not been fitted!" );
                        }

                    p->save(
                        _tfile.path() + "/feature-selector-" +
                        std::to_string( i ) + "-" + std::to_string( j ) );
                }
        }
}

// ----------------------------------------------------------------------------

void Pipeline::save_pipeline_json( const Poco::TemporaryFile& _tfile ) const
{
    Poco::JSON::Object pipeline_json;

    pipeline_json.set( "allow_http_", allow_http() );

    pipeline_json.set( "creation_time_", creation_time() );

    pipeline_json.set(
        "df_fingerprints_", JSON::vector_to_array( df_fingerprints() ) );

    pipeline_json.set(
        "fe_fingerprints_", JSON::vector_to_array( fe_fingerprints() ) );

    pipeline_json.set(
        "fs_fingerprints_", JSON::vector_to_array( fs_fingerprints() ) );

    pipeline_json.set(
        "preprocessor_fingerprints_",
        JSON::vector_to_array( preprocessor_fingerprints() ) );

    pipeline_json.set( "targets_", JSON::vector_to_array( targets() ) );

    pipeline_json.set( "peripheral_schema_", peripheral_schema() );

    pipeline_json.set( "population_schema_", population_schema() );

    save_json_obj( pipeline_json, _tfile.path() + "/pipeline.json" );
}

// ----------------------------------------------------------------------------

void Pipeline::save_predictors( const Poco::TemporaryFile& _tfile ) const
{
    for ( size_t i = 0; i < predictors_.size(); ++i )
        {
            for ( size_t j = 0; j < predictors_.at( i ).size(); ++j )
                {
                    const auto& p = predictors_.at( i ).at( j );

                    if ( !p )
                        {
                            throw std::invalid_argument(
                                "Predictor " + std::to_string( i ) + "-" +
                                std::to_string( j ) + " has not been fitted!" );
                        }

                    p->save(
                        _tfile.path() + "/predictor-" + std::to_string( i ) +
                        "-" + std::to_string( j ) );
                }
        }
}

// ----------------------------------------------------------------------------

void Pipeline::save_preprocessors( const Poco::TemporaryFile& _tfile ) const
{
    for ( size_t i = 0; i < preprocessors_.size(); ++i )
        {
            const auto& p = preprocessors_.at( i );

            if ( !p )
                {
                    throw std::invalid_argument(
                        "Preprocessor #" + std::to_string( i ) +
                        " has not been fitted!" );
                }

            const auto ptr = p->to_json_obj();

            assert_true( ptr );

            save_json_obj(
                *ptr,
                _tfile.path() + "/preprocessor-" + std::to_string( i ) +
                    ".json" );
        }
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

    obj.set( "set_used_", population_name );

    scores().from_json_obj( obj );

    // ------------------------------------------------

    return metrics::Scorer::get_metrics( obj );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::select_autofeatures(
    const containers::Features& _autofeatures,
    const predictors::PredictorImpl& _predictor_impl ) const
{
    // ------------------------------------------------

    assert_true(
        feature_learners_.size() == _predictor_impl.autofeatures().size() );

    // ------------------------------------------------

    containers::Features selected;

    // ------------------------------------------------

    size_t offset = 0;

    for ( size_t i = 0; i < feature_learners_.size(); ++i )
        {
            for ( const size_t ix : _predictor_impl.autofeatures().at( i ) )
                {
                    assert_true( offset + ix < _autofeatures.size() );

                    selected.push_back( _autofeatures.at( offset + ix ) );
                }

            assert_true( feature_learners_.at( i ) );

            offset += feature_learners_.at( i )->num_features();
        }

    // ------------------------------------------------

    assert_true( offset == _autofeatures.size() );

    // ------------------------------------------------

    return selected;

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Pipeline::to_monitor(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _name ) const
{
    auto feature_learners = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    Poco::JSON::Object json_obj;

    json_obj.set( "name_", _name );

    json_obj.set( "allow_http_", allow_http() );

    json_obj.set( "creation_time_", creation_time() );

    json_obj.set(
        "feature_learners_", JSON::get_array( obj(), "feature_learners_" ) );

    json_obj.set(
        "feature_selectors_", JSON::get_array( obj(), "feature_selectors_" ) );

    json_obj.set( "num_features_", num_features() );

    json_obj.set( "peripheral_schema_", peripheral_schema() );

    json_obj.set( "population_schema_", population_schema() );

    json_obj.set( "population_", JSON::get_object( obj(), "population_" ) );

    json_obj.set( "predictors_", JSON::get_array( obj(), "predictors_" ) );

    json_obj.set( "scores_", scores().to_json_obj() );

    json_obj.set( "sql_", to_sql_arr( _categories ) );

    json_obj.set( "tags_", JSON::get_array( obj(), "tags_" ) );

    json_obj.set( "targets_", JSON::vector_to_array( targets() ) );

    return json_obj;
}

// ----------------------------------------------------------------------------

std::string Pipeline::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
    const
{
    std::string sql;

    size_t offset = 0;

    for ( size_t i = 0; i < feature_learners_.size(); ++i )
        {
            const auto& fe = feature_learners_.at( i );

            const auto vec =
                fe->to_sql( _categories, std::to_string( i + 1 ) + "_", true );

            for ( const auto& str : vec )
                {
                    sql += str;
                }

            offset += fe->num_features();
        }

    return sql;
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Pipeline::to_sql_arr(
    const std::shared_ptr<const std::vector<strings::String>>& _categories )
    const
{
    assert_true( _categories );

    auto sql = Poco::JSON::Array::Ptr( new Poco::JSON::Array() );

    size_t offset = 0;

    assert_true(
        feature_learners_.size() == predictor_impl().autofeatures().size() );

    for ( size_t i = 0; i < feature_learners_.size(); ++i )
        {
            const auto& fe = feature_learners_.at( i );

            const auto& index = predictor_impl().autofeatures().at( i );

            const auto vec =
                fe->to_sql( _categories, std::to_string( i + 1 ) + "_", false );

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
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const std::shared_ptr<const containers::Encoding> _categories,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------

    if ( feature_learners_.size() == 0 && predictors_.size() == 0 )
        {
            throw std::runtime_error( "Pipeline has not been fitted!" );
        }

    // -------------------------------------------------------------------------

    const auto data_frames =
        apply_preprocessors( _cmd, _data_frames, _categories, _socket );

    const auto autofeatures = generate_autofeatures(
        _cmd, _logger, data_frames, predictor_impl(), _socket );

    const auto numerical_features = get_numerical_features(
        autofeatures, _cmd, data_frames, predictor_impl() );

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
        get_categorical_features( _cmd, data_frames, predictor_impl() );

    categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    // -------------------------------------------------------------------------
    // Calculate the feature statistics, if applicable.

    const auto ncols = numerical_features.size();

    if ( score && ncols > 0 )
        {
            const auto nrows = numerical_features[0]->size();

            calculate_feature_stats(
                numerical_features, nrows, ncols, _cmd, data_frames );
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

Poco::JSON::Array::Ptr Pipeline::transpose(
    const std::vector<std::vector<Float>>& _original ) const
{
    assert_true( _original.size() > 0 );

    const auto n = _original.at( 0 ).size();

    Poco::JSON::Array::Ptr transposed( new Poco::JSON::Array() );

    for ( std::size_t i = 0; i < n; ++i )
        {
            Poco::JSON::Array::Ptr temp( new Poco::JSON::Array() );

            for ( const auto& vec : _original )
                {
                    assert_true( vec.size() == n );
                    temp->add( vec.at( i ) );
                }

            transposed->add( temp );
        }

    return transposed;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

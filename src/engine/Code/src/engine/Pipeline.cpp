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
        fl_fingerprints() );

    init_predictors(
        "predictors_", 1, impl_.predictor_impl_, fl_fingerprints() );
}

// ----------------------------------------------------------------------------

Pipeline::Pipeline(
    const std::string& _path,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const std::shared_ptr<dependency::PreprocessorTracker>
        _preprocessor_tracker )
    : impl_( PipelineImpl() )
{
    *this = load( _path, _fe_tracker, _pred_tracker, _preprocessor_tracker );
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
    const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl,
    containers::Features* _features ) const
{
    for ( const auto& col : _predictor_impl.numerical_colnames() )
        {
            _features->push_back( _population_df.numerical( col ).data_ptr() );
        }
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Pipeline::apply_preprocessors(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<const containers::Encoding>& _categories,
    Poco::Net::StreamSocket* _socket ) const
{
    if ( preprocessors_.size() == 0 )
        {
            return std::make_pair( _population_df, _peripheral_dfs );
        }

    auto population_df = _population_df;

    auto peripheral_dfs = _peripheral_dfs;

    const auto [placeholder, peripheral_names] = make_placeholder();

    assert_true( placeholder );

    assert_true( peripheral_names );

    assert_true( _logger );

    const auto socket_logger =
        std::make_shared<communication::SocketLogger>( _logger, true, _socket );

    socket_logger->log( "Preprocessing..." );

    for ( size_t i = 0; i < preprocessors_.size(); ++i )
        {
            const auto progress = ( i * 100 ) / preprocessors_.size();

            socket_logger->log(
                "Progress: " + std::to_string( progress ) + "%." );

            auto& p = preprocessors_.at( i );

            assert_true( p );

            const auto params = preprocessors::TransformParams{
                .cmd_ = _cmd,
                .categories_ = _categories,
                .logger_ = socket_logger,
                .logging_begin_ = ( i * 100 ) / preprocessors_.size(),
                .logging_end_ = ( ( i + 1 ) * 100 ) / preprocessors_.size(),
                .peripheral_dfs_ = peripheral_dfs,
                .peripheral_names_ = *peripheral_names,
                .placeholder_ = *placeholder,
                .population_df_ = population_df };

            std::tie( population_df, peripheral_dfs ) = p->transform( params );
        }

    socket_logger->log( "Progress: 100%." );

    return std::make_pair( population_df, peripheral_dfs );
}

// ----------------------------------------------------------------------------

std::vector<std::string> Pipeline::autofeature_names() const
{
    if ( !impl_.predictor_impl_ )
        {
            throw std::invalid_argument( "Pipeline has not been fitted!" );
        }

    std::vector<std::string> autofeatures;

    for ( size_t i = 0; i < predictor_impl().autofeatures().size(); ++i )
        {
            const auto& index = predictor_impl().autofeatures().at( i );

            for ( const auto ix : index )
                {
                    autofeatures.push_back(
                        "feature_" + std::to_string( i + 1 ) + "_" +
                        std::to_string( ix + 1 ) );
                }
        }

    return autofeatures;
}

// ----------------------------------------------------------------------

void Pipeline::calculate_feature_stats(
    const containers::Features _features,
    const size_t _nrows,
    const size_t _ncols,
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df )
{
    // ------------------------------------------------------------------------

    std::vector<const Float*> targets;

    for ( size_t j = 0; j < _population_df.num_targets(); ++j )
        {
            targets.push_back( _population_df.target( j ).data() );
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
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<dependency::PreprocessorTracker>&
        _preprocessor_tracker,
    const std::shared_ptr<dependency::WarningTracker>& _warning_tracker,
    Poco::Net::StreamSocket* _socket ) const
{
    // -------------------------------------------------------------------------

    auto [population_df, peripheral_dfs] =
        modify_data_frames( _population_df, _peripheral_dfs, _logger, _socket );

    const auto df_fingerprints =
        extract_df_fingerprints( _population_df, _peripheral_dfs );

    // -------------------------------------------------------------------------

    const auto preprocessors = init_preprocessors( df_fingerprints );

    const auto preprocessor_fingerprints =
        preprocessors.size() > 0
            ? extract_preprocessor_fingerprints( preprocessors )
            : df_fingerprints;

    const auto feature_learners =
        init_feature_learners( 1, preprocessor_fingerprints );

    const auto fl_fingerprints =
        feature_learners.size() > 0
            ? extract_fl_fingerprints( feature_learners )
            : preprocessor_fingerprints;

    // -------------------------------------------------------------------------

    const auto warning_fingerprint =
        make_warning_fingerprint( fl_fingerprints );

    assert_true( _warning_tracker );

    const auto retrieved = _warning_tracker->retrieve( warning_fingerprint );

    if ( retrieved )
        {
            communication::Sender::send_string( "Success!", _socket );
            retrieved->send( _socket );
            return;
        }

    // -------------------------------------------------------------------------

    fit_transform_preprocessors(
        _cmd,
        _logger,
        _categories,
        _preprocessor_tracker,
        df_fingerprints,
        &population_df,
        &peripheral_dfs,
        _socket );

    // -------------------------------------------------------------------------

    const auto [placeholder, peripheral_names] = make_placeholder();

    const auto socket_logger =
        _logger ? std::make_shared<const communication::SocketLogger>(
                      _logger, true, _socket )
                : std::shared_ptr<const communication::SocketLogger>();

    const auto warner = preprocessors::DataModelChecker::check(
        placeholder,
        peripheral_names,
        population_df,
        peripheral_dfs,
        feature_learners,
        socket_logger );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------------------------

    const auto warnings = warner.to_warnings_obj( warning_fingerprint );

    warnings->send( _socket );

    _warning_tracker->add( warnings );

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

    for ( auto& i_maker : importance_makers )
        {
            i_maker = helpers::Macros::modify_column_importances( i_maker );
        }

    fill_zeros( &importance_makers );

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

// ----------------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_df_fingerprints(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    const auto placeholder = JSON::get_object( obj(), "data_model_" );

    std::vector<Poco::JSON::Object::Ptr> df_fingerprints = {
        placeholder, _population_df.fingerprint() };

    for ( const auto& df : _peripheral_dfs )
        {
            df_fingerprints.push_back( df.fingerprint() );
        }

    return df_fingerprints;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_fingerprints(
    const std::vector<std::vector<std::shared_ptr<predictors::Predictor>>>&
        _predictors ) const
{
    std::vector<Poco::JSON::Object::Ptr> fingerprints;

    for ( const auto& vec : _predictors )
        {
            for ( const auto& p : vec )
                {
                    assert_true( p );
                    fingerprints.push_back( p->fingerprint() );
                }
        }

    return fingerprints;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_fl_fingerprints(
    const std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>&
        _feature_learners ) const
{
    if ( _feature_learners.size() == 0 )
        {
            return preprocessor_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fl_fingerprints;

    for ( const auto& fl : _feature_learners )
        {
            assert_true( fl );
            fl_fingerprints.push_back( fl->fingerprint() );
        }

    return fl_fingerprints;
}

// ----------------------------------------------------------------------

std::vector<Poco::JSON::Object::Ptr> Pipeline::extract_fs_fingerprints() const
{
    if ( feature_selectors_.size() == 0 ||
         feature_selectors_.at( 0 ).size() == 0 )
        {
            return fl_fingerprints();
        }

    return extract_fingerprints( feature_selectors_ );
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
Pipeline::extract_preprocessor_fingerprints(
    const std::vector<std::shared_ptr<preprocessors::Preprocessor>>&
        _preprocessors ) const
{
    if ( _preprocessors.size() == 0 )
        {
            return df_fingerprints();
        }

    std::vector<Poco::JSON::Object::Ptr> fingerprints;

    for ( const auto& p : _preprocessors )
        {
            assert_true( p );
            fingerprints.push_back( p->fingerprint() );
        }

    return fingerprints;
}

// ----------------------------------------------------------------------

std::pair<
    std::shared_ptr<const helpers::Schema>,
    std::shared_ptr<const std::vector<helpers::Schema>>>
Pipeline::extract_modified_schemata(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    const auto extract_schema =
        []( const containers::DataFrame& _df ) -> helpers::Schema {
        return _df.to_schema( true );
    };

    const auto population_schema = std::make_shared<const helpers::Schema>(
        extract_schema( _population_df ) );

    const auto peripheral_schema =
        std::make_shared<const std::vector<helpers::Schema>>(
            stl::collect::vector<helpers::Schema>(
                _peripheral_dfs | VIEWS::transform( extract_schema ) ) );

    return std::make_pair( population_schema, peripheral_schema );
}

// ----------------------------------------------------------------------------

std::pair<
    std::shared_ptr<const helpers::Schema>,
    std::shared_ptr<const std::vector<helpers::Schema>>>
Pipeline::extract_schemata(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs ) const
{
    const auto extract_schema =
        []( const containers::DataFrame& _df ) -> helpers::Schema {
        return _df.to_schema( false );
    };

    const auto population_schema = std::make_shared<const helpers::Schema>(
        extract_schema( _population_df ) );

    const auto peripheral_schema =
        std::make_shared<const std::vector<helpers::Schema>>(
            stl::collect::vector<helpers::Schema>(
                _peripheral_dfs | VIEWS::transform( extract_schema ) ) );

    return std::make_pair( population_schema, peripheral_schema );
}

// ----------------------------------------------------------------------

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

std::vector<std::string> Pipeline::feature_learners_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const bool _targets,
    const bool _subfeatures,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator ) const
{
    const auto to_sql =
        [this, _categories, _targets, _subfeatures, _sql_dialect_generator](
            const size_t _i ) -> std::vector<std::string> {
        const auto& fl = feature_learners_.at( _i );

        assert_true( fl );

        const auto all = fl->to_sql(
            _categories,
            _targets,
            _subfeatures,
            _sql_dialect_generator,
            std::to_string( _i + 1 ) + "_" );

        assert_true( all.size() >= fl->num_features() );

        const auto num_subfeatures = all.size() - fl->num_features();

        const auto subfeatures = stl::collect::vector<std::string>(
            all | VIEWS::take( num_subfeatures ) );

        const auto get_feature = [num_subfeatures,
                                  all]( const size_t _ix ) -> std::string {
            assert_true( _ix < all.size() );
            return all.at( num_subfeatures + _ix );
        };

        assert_true( _i < predictor_impl().autofeatures().size() );

        const auto& autofeatures = predictor_impl().autofeatures().at( _i );

        const auto features = stl::collect::vector<std::string>(
            autofeatures | VIEWS::transform( get_feature ) );

        return stl::join::vector<std::string>( { subfeatures, features } );
    };

    const auto iota = stl::iota<size_t>( 0, feature_learners_.size() );

    return stl::join::vector<std::string>( iota | VIEWS::transform( to_sql ) );
}

// ----------------------------------------------------------------------------

std::tuple<
    std::vector<std::string>,
    std::vector<std::string>,
    std::vector<std::string>>
Pipeline::feature_names() const
{
    if ( !impl_.predictor_impl_ )
        {
            throw std::invalid_argument( "Pipeline has not been fitted!" );
        }

    assert_true(
        feature_learners_.size() == predictor_impl().autofeatures().size() );

    const auto autofeatures = autofeature_names();

    const auto numerical = helpers::Macros::modify_colnames(
        predictor_impl().numerical_colnames() );

    const auto categorical = helpers::Macros::modify_colnames(
        predictor_impl().categorical_colnames() );

    return std::make_tuple( autofeatures, numerical, categorical );
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

void Pipeline::fill_zeros(
    std::vector<helpers::ImportanceMaker>* _f_importances ) const
{
    if ( _f_importances->size() == 0 )
        {
            return;
        }

    const auto fill_all = []( const helpers::ImportanceMaker& _f1,
                              helpers::ImportanceMaker* _f2 ) {
        for ( const auto& [desc, _] : _f1.importances() )
            {
                _f2->add_to_importances( desc, 0.0 );
            }
    };

    for ( size_t i = 1; i < _f_importances->size(); ++i )
        {
            fill_all( _f_importances->at( i ), &_f_importances->at( 0 ) );
        }

    for ( size_t i = 1; i < _f_importances->size(); ++i )
        {
            fill_all( _f_importances->at( 0 ), &_f_importances->at( i ) );
        }
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::generate_autofeatures(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
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

            auto new_features = fe->transform(
                _cmd,
                index,
                std::to_string( i + 1 ) + "_",
                socket_logger,
                _population_df,
                _peripheral_dfs );

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

void Pipeline::fit( const FitParams& _params )
{
    // -------------------------------------------------------------------------

    assert_true( _params.fe_tracker_ );
    assert_true( _params.pred_tracker_ );

    // -------------------------------------------------------------------------

    targets() = get_targets( _params.population_df_ );

    std::tie( population_schema(), peripheral_schema() ) =
        extract_schemata( _params.population_df_, _params.peripheral_dfs_ );

    df_fingerprints() = extract_df_fingerprints(
        _params.population_df_, _params.peripheral_dfs_ );

    // -------------------------------------------------------------------------

    auto [population_df, peripheral_dfs] = modify_data_frames(
        _params.population_df_,
        _params.peripheral_dfs_,
        _params.logger_,
        _params.socket_ );

    // -------------------------------------------------------------------------

    preprocessors_ = fit_transform_preprocessors(
        _params.cmd_,
        _params.logger_,
        _params.categories_,
        _params.preprocessor_tracker_,
        df_fingerprints(),
        &population_df,
        &peripheral_dfs,
        _params.socket_ );

    preprocessor_fingerprints() =
        extract_preprocessor_fingerprints( preprocessors_ );

    // -------------------------------------------------------------------------

    fit_feature_learners(
        _params.cmd_,
        _params.logger_,
        population_df,
        peripheral_dfs,
        _params.fe_tracker_,
        _params.socket_ );

    // -------------------------------------------------------------------------

    make_feature_selector_impl( _params.cmd_, population_df );

    // -------------------------------------------------------------------------

    containers::Features autofeatures;

    // -------------------------------------------------------------------------

    auto feature_selectors = init_predictors(
        "feature_selectors_",
        num_targets(),
        impl_.feature_selector_impl_,
        fl_fingerprints() );

    const auto feature_selection_params = TransformParams{
        .categories_ = _params.categories_,
        .cmd_ = _params.cmd_,
        .data_frames_ = _params.data_frames_,
        .data_frame_tracker_ = _params.data_frame_tracker_,
        .dependencies_ = fl_fingerprints(),
        .logger_ = _params.logger_,
        .original_peripheral_dfs_ = _params.peripheral_dfs_,
        .original_population_df_ = _params.population_df_,
        .peripheral_dfs_ = peripheral_dfs,
        .population_df_ = population_df,
        .predictor_impl_ = feature_selector_impl(),
        .pred_tracker_ = _params.pred_tracker_,
        .purpose_ = TransformParams::FEATURE_SELECTOR,
        .autofeatures_ = &autofeatures,
        .predictors_ = &feature_selectors,
        .socket_ = _params.socket_ };

    fit_predictors( feature_selection_params );

    feature_selectors_ = std::move( feature_selectors );

    fs_fingerprints() = extract_fs_fingerprints();

    // -------------------------------------------------------------------------

    make_predictor_impl( _params.cmd_, population_df );

    // -------------------------------------------------------------------------

    const auto validation_fingerprint =
        _params.validation_df_ ? std::vector<Poco::JSON::Object::Ptr>(
                                     { _params.validation_df_->fingerprint() } )
                               : std::vector<Poco::JSON::Object::Ptr>();

    const auto dependencies = stl::join::vector<Poco::JSON::Object::Ptr>(
        { fs_fingerprints(), validation_fingerprint } );

    auto predictors = init_predictors(
        "predictors_", num_targets(), impl_.predictor_impl_, dependencies );

    const auto prediction_params = TransformParams{
        .categories_ = _params.categories_,
        .cmd_ = _params.cmd_,
        .data_frames_ = _params.data_frames_,
        .data_frame_tracker_ = _params.data_frame_tracker_,
        .dependencies_ = fs_fingerprints(),
        .logger_ = _params.logger_,
        .original_peripheral_dfs_ = _params.peripheral_dfs_,
        .original_population_df_ = _params.population_df_,
        .peripheral_dfs_ = peripheral_dfs,
        .population_df_ = population_df,
        .predictor_impl_ = predictor_impl(),
        .pred_tracker_ = _params.pred_tracker_,
        .purpose_ = TransformParams::PREDICTOR,
        .validation_df_ = _params.validation_df_,
        .autofeatures_ = &autofeatures,
        .predictors_ = &predictors,
        .socket_ = _params.socket_ };

    fit_predictors( prediction_params );

    predictors_ = std::move( predictors );

    // -------------------------------------------------------------------------

    scores().from_json_obj( column_importances_as_obj() );

    scores().from_json_obj( feature_importances_as_obj() );

    scores().from_json_obj( feature_names_as_obj() );

    // -------------------------------------------------------------------------

    const bool score = predictors_.size() > 0 && predictors_.at( 0 ).size() > 0;

    if ( score )
        {
            const auto score_params = TransformParams{
                .categories_ = _params.categories_,
                .cmd_ = _params.cmd_,
                .data_frames_ = _params.data_frames_,
                .data_frame_tracker_ = _params.data_frame_tracker_,
                .dependencies_ = fs_fingerprints(),
                .logger_ = _params.logger_,
                .original_peripheral_dfs_ = _params.peripheral_dfs_,
                .original_population_df_ = _params.population_df_,
                .peripheral_dfs_ = peripheral_dfs,
                .population_df_ = population_df,
                .predictor_impl_ = predictor_impl(),
                .pred_tracker_ = _params.pred_tracker_,
                .autofeatures_ = &autofeatures,
                .predictors_ = nullptr,
                .socket_ = _params.socket_ };

            score_after_fitting( score_params );
        }

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::fit_feature_learners(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<dependency::FETracker> _fe_tracker,
    Poco::Net::StreamSocket* _socket )
{
    std::tie(
        impl_.modified_population_schema_, impl_.modified_peripheral_schema_ ) =
        extract_modified_schemata( _population_df, _peripheral_dfs );

    auto feature_learners =
        init_feature_learners( num_targets(), preprocessor_fingerprints() );

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

            fe->fit(
                _cmd,
                std::to_string( i + 1 ) + "_",
                socket_logger,
                _population_df,
                _peripheral_dfs );

            _fe_tracker->add( fe );
        }

    feature_learners_ = std::move( feature_learners );

    fl_fingerprints() = extract_fl_fingerprints( feature_learners_ );
}

// ----------------------------------------------------------------------------

void Pipeline::fit_predictors( const TransformParams& _params )
{
    // --------------------------------------------------------------------

    const auto all_retrieved =
        retrieve_predictors( _params.pred_tracker_, _params.predictors_ );

    if ( all_retrieved )
        {
            return;
        }

    // --------------------------------------------------------------------

    auto [numerical_features, categorical_features, autofeatures] =
        make_features( _params );

    *_params.autofeatures_ = autofeatures;

    categorical_features =
        _params.predictor_impl_.transform_encodings( categorical_features );

    // --------------------------------------------------------------------

    auto [numerical_features_valid, categorical_features_valid] =
        make_features_validation( _params );

    if ( categorical_features_valid )
        {
            *categorical_features_valid =
                _params.predictor_impl_.transform_encodings(
                    *categorical_features_valid );
        }

    // --------------------------------------------------------------------

    assert_true(
        _params.population_df_.num_targets() == _params.predictors_->size() );

    // --------------------------------------------------------------------

    for ( size_t t = 0; t < _params.population_df_.num_targets(); ++t )
        {
            const auto target_col =
                _params.population_df_.target( t ).data_ptr();

            const auto target_col_valid =
                numerical_features_valid
                    ? std::make_optional<decltype( target_col )>(
                          _params.validation_df_.value()
                              .target( t )
                              .data_ptr() )
                    : std::optional<decltype( target_col )>();

            for ( auto& p : _params.predictors_->at( t ) )
                {
                    assert_true( p );

                    const auto socket_logger =
                        std::make_shared<const communication::SocketLogger>(
                            _params.logger_, p->silent(), _params.socket_ );

                    // If p is already fitted, that is because it has been
                    // retrieved.
                    if ( p->is_fitted() )
                        {
                            socket_logger->log( "Retrieving predictor..." );

                            socket_logger->log( "Progress: 100%." );

                            continue;
                        }

                    socket_logger->log(
                        p->type() + ": Training as " + _params.purpose_ +
                        "..." );

                    p->fit(
                        socket_logger,
                        categorical_features,
                        numerical_features,
                        target_col,
                        categorical_features_valid,
                        numerical_features_valid,
                        target_col_valid );

                    _params.pred_tracker_->add( p );
                }
        }

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<preprocessors::Preprocessor>>
Pipeline::fit_transform_preprocessors(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<dependency::PreprocessorTracker>&
        _preprocessor_tracker,
    const std::vector<Poco::JSON::Object::Ptr>& _dependencies,
    containers::DataFrame* _population_df,
    std::vector<containers::DataFrame>* _peripheral_dfs,
    Poco::Net::StreamSocket* _socket ) const
{
    auto preprocessors = init_preprocessors( _dependencies );

    if ( preprocessors.size() == 0 )
        {
            return preprocessors;
        }

    const auto [placeholder, peripheral_names] = make_placeholder();

    assert_true( placeholder );

    assert_true( peripheral_names );

    assert_true( _preprocessor_tracker );

    const auto socket_logger =
        _logger ? std::make_shared<const communication::SocketLogger>(
                      _logger, true, _socket )
                : std::shared_ptr<const communication::SocketLogger>();

    if ( socket_logger )
        {
            socket_logger->log( "Preprocessing..." );
        }

    for ( size_t i = 0; i < preprocessors.size(); ++i )
        {
            if ( socket_logger )
                {
                    const auto progress = ( i * 100 ) / preprocessors.size();
                    socket_logger->log(
                        "Progress: " + std::to_string( progress ) + "%." );
                }

            auto& p = preprocessors.at( i );

            assert_true( p );

            const auto fingerprint = p->fingerprint();

            const auto retrieved_preprocessor =
                _preprocessor_tracker->retrieve( fingerprint );

            if ( retrieved_preprocessor )
                {
                    const auto params = preprocessors::TransformParams{
                        .cmd_ = _cmd,
                        .categories_ = _categories,
                        .logger_ = socket_logger,
                        .logging_begin_ = ( i * 100 ) / preprocessors.size(),
                        .logging_end_ =
                            ( ( i + 1 ) * 100 ) / preprocessors.size(),
                        .peripheral_dfs_ = *_peripheral_dfs,
                        .peripheral_names_ = *peripheral_names,
                        .placeholder_ = *placeholder,
                        .population_df_ = *_population_df };

                    p = retrieved_preprocessor;

                    std::tie( *_population_df, *_peripheral_dfs ) =
                        p->transform( params );

                    continue;
                }

            const auto params = preprocessors::FitParams{
                .cmd_ = _cmd,
                .categories_ = _categories,
                .logger_ = socket_logger,
                .logging_begin_ = ( i * 100 ) / preprocessors.size(),
                .logging_end_ = ( ( i + 1 ) * 100 ) / preprocessors.size(),
                .peripheral_dfs_ = *_peripheral_dfs,
                .peripheral_names_ = *peripheral_names,
                .placeholder_ = *placeholder,
                .population_df_ = *_population_df };

            std::tie( *_population_df, *_peripheral_dfs ) =
                p->fit_transform( params );

            _preprocessor_tracker->add( p );
        }

    if ( socket_logger )
        {
            socket_logger->log( "Progress: 100%." );
        }

    return preprocessors;
}

// ----------------------------------------------------------------------------

containers::CategoricalFeatures Pipeline::get_categorical_features(
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl ) const
{
    auto categorical_features = containers::CategoricalFeatures();

    if ( !impl_.include_categorical_ )
        {
            return categorical_features;
        }

    for ( const auto& col : _predictor_impl.categorical_colnames() )
        {
            categorical_features.push_back(
                _population_df.categorical( col ).data_ptr() );
        }

    return categorical_features;
}

// ----------------------------------------------------------------------------

containers::Features Pipeline::get_numerical_features(
    const containers::Features& _autofeatures,
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df,
    const predictors::PredictorImpl& _predictor_impl ) const
{
    // --------------------------------------------------------------------

    auto numerical_features = _autofeatures;

    // -------------------------------------------------------------------------

    add_population_cols(
        _cmd, _population_df, _predictor_impl, &numerical_features );

    // -------------------------------------------------------------------------

    return numerical_features;

    // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------

std::vector<std::string> Pipeline::get_targets(
    const containers::DataFrame& _population_df ) const
{
    auto target_names =
        std::vector<std::string>( _population_df.num_targets() );

    for ( size_t i = 0; i < _population_df.num_targets(); ++i )
        {
            target_names[i] = _population_df.target( i ).name();
        }

    return target_names;
}

// ----------------------------------------------------------------------

std::vector<std::shared_ptr<featurelearners::AbstractFeatureLearner>>
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

    const auto make_fl_for_one_target =
        []( const featurelearners::FeatureLearnerParams& _params,
            const Int _target_num ) {
            const auto new_params = featurelearners::FeatureLearnerParams{
                .cmd_ = _params.cmd_,
                .dependencies_ = _params.dependencies_,
                .peripheral_ = _params.peripheral_,
                .peripheral_schema_ = _params.peripheral_schema_,
                .placeholder_ = _params.placeholder_,
                .population_schema_ = _params.population_schema_,
                .target_num_ = _target_num };

            return featurelearners::FeatureLearnerParser::parse( new_params );
        };

    // ----------------------------------------------------------------------

    const auto make_fl_for_all_targets =
        [_num_targets, make_fl_for_one_target](
            const featurelearners::FeatureLearnerParams& _params ) {
            const auto make = std::bind(
                make_fl_for_one_target, _params, std::placeholders::_1 );

            const auto iota = stl::iota<Int>( 0, _num_targets );

            const auto range = iota | VIEWS::transform( make );

            return stl::collect::vector<
                std::shared_ptr<featurelearners::AbstractFeatureLearner>>(
                range );
        };

    // ----------------------------------------------------------------------

    const auto [placeholder, peripheral] = make_placeholder();

    const auto to_fl = [this,
                        &_dependencies,
                        placeholder = placeholder,
                        peripheral = peripheral,
                        make_fl_for_all_targets]( Poco::JSON::Object::Ptr _cmd )
        -> std::vector<
            std::shared_ptr<featurelearners::AbstractFeatureLearner>> {
        assert_true( _cmd );

        const auto params = featurelearners::FeatureLearnerParams{
            .cmd_ = *_cmd,
            .dependencies_ = _dependencies,
            .peripheral_ = peripheral,
            .peripheral_schema_ = impl_.modified_peripheral_schema_,
            .placeholder_ = placeholder,
            .population_schema_ = impl_.modified_population_schema_,
            .target_num_ =
                featurelearners::AbstractFeatureLearner::USE_ALL_TARGETS };

        const auto new_feature_learner =
            featurelearners::FeatureLearnerParser::parse( params );

        if ( new_feature_learner->supports_multiple_targets() )
            {
                return { new_feature_learner };
            }

        return make_fl_for_all_targets( params );
    };

    // ----------------------------------------------------------------------

    const auto obj_vector = JSON::array_to_obj_vector(
        JSON::get_array( obj(), "feature_learners_" ) );

    return stl::join::vector<
        std::shared_ptr<featurelearners::AbstractFeatureLearner>>(
        obj_vector | VIEWS::transform( to_fl ) );

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

            auto target_num =
                Poco::JSON::Object::Ptr( new Poco::JSON::Object() );

            target_num->set( "target_num_", t );

            auto dependencies = _dependencies;

            dependencies.push_back( target_num );

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
                        *ptr, _predictor_impl, dependencies );

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
    if ( !obj().has( "preprocessors_" ) )
        {
            return std::vector<std::shared_ptr<preprocessors::Preprocessor>>();
        }

    const auto arr =
        jsonutils::JSON::get_object_array( obj(), "preprocessors_" );

    const auto parse = [&arr, &_dependencies]( const size_t _i ) {
        const auto ptr = arr->getObject( _i );
        assert_true( ptr );
        return preprocessors::PreprocessorParser::parse( *ptr, _dependencies );
    };

    const auto iota = stl::iota<size_t>( 0, arr->size() );

    const auto range = iota | VIEWS::transform( parse );

    auto vec =
        stl::collect::vector<std::shared_ptr<preprocessors::Preprocessor>>(
            range );

    const auto mapping_to_end =
        []( const std::shared_ptr<preprocessors::Preprocessor>& _ptr ) -> bool {
        assert_true( _ptr );
        return _ptr->type() != preprocessors::Preprocessor::MAPPING;
    };

    std::stable_partition( vec.begin(), vec.end(), mapping_to_end );

    // We need to take into consideration that preprocessors can also depend on
    // each other.
    auto dependencies = _dependencies;

    for ( auto& p : vec )
        {
            assert_true( p );
            const auto copy = p->clone( dependencies );
            dependencies.push_back( p->fingerprint() );
            p = copy;
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
    const std::shared_ptr<dependency::PredTracker> _pred_tracker,
    const std::shared_ptr<dependency::PreprocessorTracker>
        _preprocessor_tracker ) const
{
    // ------------------------------------------------------------------

    assert_true( _fe_tracker );

    assert_true( _pred_tracker );

    assert_true( _preprocessor_tracker );

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

    load_preprocessors( _path, _preprocessor_tracker, &pipeline );

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

    _pipeline->feature_learners_ = _pipeline->init_feature_learners(
        _pipeline->num_targets(), _pipeline->preprocessor_fingerprints() );

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
        _pipeline->fl_fingerprints() );

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

    const auto fl_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "fl_fingerprints_" ) );

    const auto fs_fingerprints = JSON::array_to_obj_vector(
        JSON::get_array( _pipeline_json, "fs_fingerprints_" ) );

    _pipeline->df_fingerprints() = df_fingerprints;

    _pipeline->preprocessor_fingerprints() = preprocessor_fingerprints;

    _pipeline->fl_fingerprints() = fl_fingerprints;

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
    const auto to_schema =
        []( const Poco::JSON::Object::Ptr& _obj ) -> helpers::Schema {
        assert_true( _obj );
        return helpers::Schema::from_json( *_obj );
    };

    const auto pipeline_json = load_json_obj( _path + "pipeline.json" );

    const auto get_peripheral_schema = [&pipeline_json,
                                        to_schema]( const std::string& _name ) {
        auto arr = JSON::get_array( pipeline_json, _name );
        const auto vec = JSON::array_to_obj_vector( arr );
        return std::make_shared<const std::vector<helpers::Schema>>(
            stl::collect::vector<helpers::Schema>(
                vec | VIEWS::transform( to_schema ) ) );
    };

    _pipeline->allow_http() =
        JSON::get_value<bool>( pipeline_json, "allow_http_" );

    _pipeline->creation_time() =
        JSON::get_value<std::string>( pipeline_json, "creation_time_" );

    _pipeline->modified_peripheral_schema() =
        get_peripheral_schema( "modified_peripheral_schema_" );

    _pipeline->modified_population_schema() =
        std::make_shared<const helpers::Schema>(
            helpers::Schema::from_json( *JSON::get_object(
                pipeline_json, "modified_population_schema_" ) ) );

    _pipeline->peripheral_schema() =
        get_peripheral_schema( "peripheral_schema_" );

    _pipeline->population_schema() =
        std::make_shared<const helpers::Schema>( helpers::Schema::from_json(
            *JSON::get_object( pipeline_json, "population_schema_" ) ) );

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
    const std::string& _path,
    const std::shared_ptr<dependency::PreprocessorTracker>
        _preprocessor_tracker,
    Pipeline* _pipeline ) const
{
    assert_true( _preprocessor_tracker );

    if ( !_pipeline->obj().has( "preprocessors_" ) )
        {
            return;
        }

    auto preprocessors =
        std::vector<std::shared_ptr<preprocessors::Preprocessor>>();

    const auto arr =
        jsonutils::JSON::get_object_array( _pipeline->obj(), "preprocessors_" );

    for ( size_t i = 0; i < arr->size(); ++i )
        {
            const auto json_obj = load_json_obj(
                _path + "preprocessor-" + std::to_string( i ) + ".json" );

            const auto p = preprocessors::PreprocessorParser::parse(
                json_obj, _pipeline->df_fingerprints() );

            preprocessors.push_back( p );

            _preprocessor_tracker->add( p );
        }

    _pipeline->preprocessors_ = preprocessors;
}

// ----------------------------------------------------------------------------

std::vector<std::string> Pipeline::make_feature_names() const
{
    const auto to_names =
        [this]( const size_t _i ) -> std::vector<std::string> {
        const auto make_name = [_i]( const size_t _ix ) -> std::string {
            return "feature_" + std::to_string( _i + 1 ) + "_" +
                   std::to_string( _ix + 1 );
        };

        assert_true( _i < predictor_impl().autofeatures().size() );

        const auto& autofeatures = predictor_impl().autofeatures().at( _i );

        return stl::collect::vector<std::string>(
            autofeatures | VIEWS::transform( make_name ) );
    };

    const auto iota = stl::iota<size_t>( 0, feature_learners_.size() );

    return stl::join::vector<std::string>(
        iota | VIEWS::transform( to_names ) );
}

// ----------------------------------------------------------------------------

std::tuple<
    containers::Features,
    containers::CategoricalFeatures,
    containers::Features>
Pipeline::make_features( const TransformParams& _params ) const
{
    // --------------------------------------------------------------------

    assert_true( _params.original_population_df_ );

    assert_true( _params.original_peripheral_dfs_ );

    const auto df = _params.data_frame_tracker_.retrieve(
        dependencies(),
        _params.original_population_df_.value(),
        _params.original_peripheral_dfs_.value() );

    if ( df )
        {
            return retrieve_features( *df );
        }

    // --------------------------------------------------------------------

    containers::Features autofeatures;

    if ( _params.autofeatures_ &&
         _params.autofeatures_->size() ==
             _params.predictor_impl_.num_autofeatures() )
        {
            autofeatures = *_params.autofeatures_;
        }
    else if ( _params.autofeatures_ && _params.autofeatures_->size() != 0 )
        {
            autofeatures = select_autofeatures(
                *_params.autofeatures_, _params.predictor_impl_ );
        }
    else
        {
            autofeatures = generate_autofeatures(
                _params.cmd_,
                _params.logger_,
                _params.population_df_,
                _params.peripheral_dfs_,
                _params.predictor_impl_,
                _params.socket_ );
        }

    const auto numerical_features = get_numerical_features(
        autofeatures,
        _params.cmd_,
        _params.population_df_,
        _params.predictor_impl_ );

    // --------------------------------------------------------------------

    const auto categorical_features = get_categorical_features(
        _params.cmd_, _params.population_df_, _params.predictor_impl_ );

    // --------------------------------------------------------------------

    return std::make_tuple(
        numerical_features, categorical_features, autofeatures );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<
    std::optional<containers::Features>,
    std::optional<containers::CategoricalFeatures>>
Pipeline::make_features_validation( const TransformParams& _params )
{
    if ( !_params.validation_df_ ||
         _params.purpose_ != TransformParams::PREDICTOR )
        {
            return std::make_pair(
                std::optional<containers::Features>(),
                std::optional<containers::CategoricalFeatures>() );
        }

    assert_true( _params.original_peripheral_dfs_ );

    const auto [numerical_features, categorical_features] = transform(
        _params.cmd_,
        _params.logger_,
        _params.data_frames_,
        _params.validation_df_.value(),
        _params.original_peripheral_dfs_.value(),
        _params.data_frame_tracker_,
        _params.categories_,
        _params.socket_ );

    return std::make_pair(
        std::make_optional<containers::Features>( numerical_features ),
        std::make_optional<containers::CategoricalFeatures>(
            categorical_features ) );
}

// ------------------------------------------------------------------------

void Pipeline::make_feature_selector_impl(
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df )
{
    // --------------------------------------------------------------------

    const auto is_null = []( const Float val ) {
        return ( std::isnan( val ) || std::isinf( val ) );
    };

    // --------------------------------------------------------------------

    const auto blacklist = std::vector<helpers::Subrole>(
        { helpers::Subrole::exclude_predictors,
          helpers::Subrole::email_only,
          helpers::Subrole::substring_only } );

    // --------------------------------------------------------------------

    auto categorical_colnames = std::vector<std::string>();

    if ( impl_.include_categorical_ )
        {
            for ( size_t i = 0; i < _population_df.num_categoricals(); ++i )
                {
                    if ( _population_df.categorical( i ).unit().find(
                             "comparison only" ) != std::string::npos )
                        {
                            continue;
                        }

                    if ( helpers::SubroleParser::contains_any(
                             _population_df.categorical( i ).subroles(),
                             blacklist ) )
                        {
                            continue;
                        }

                    categorical_colnames.push_back(
                        _population_df.categorical( i ).name() );
                }
        }

    // --------------------------------------------------------------------

    auto numerical_colnames = std::vector<std::string>();

    for ( size_t i = 0; i < _population_df.num_numericals(); ++i )
        {
            if ( _population_df.numerical( i ).unit().find(
                     "comparison only" ) != std::string::npos )
                {
                    continue;
                }

            if ( helpers::SubroleParser::contains_any(
                     _population_df.numerical( i ).subroles(), blacklist ) )
                {
                    continue;
                }

            const auto contains_null = std::any_of(
                _population_df.numerical( i ).begin(),
                _population_df.numerical( i ).end(),
                is_null );

            if ( contains_null )
                {
                    continue;
                }

            numerical_colnames.push_back(
                _population_df.numerical( i ).name() );
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
        get_categorical_features( _cmd, _population_df, *local_fs_impl );

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

std::pair<
    std::shared_ptr<const helpers::Placeholder>,
    std::shared_ptr<const std::vector<std::string>>>
Pipeline::make_placeholder() const
{
    const auto data_model = *JSON::get_object( obj(), "data_model_" );

    const auto placeholder = std::make_shared<const helpers::Placeholder>(
        PlaceholderMaker::make_placeholder( data_model, "t1" ) );

    const auto peripheral_names =
        std::make_shared<const std::vector<std::string>>(
            PlaceholderMaker::make_peripheral( *placeholder ) );

    return std::make_pair( placeholder, peripheral_names );
}

// ------------------------------------------------------------------------

void Pipeline::make_predictor_impl(
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df )
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
        get_categorical_features( _cmd, _population_df, *local_predictor_impl );

    local_predictor_impl->fit_encodings( categorical_features );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::pair<containers::Schema, std::vector<containers::Schema>>
Pipeline::make_staging_schemata() const
{
    const auto has_text_field_marker =
        []( const std::string& _colname ) -> bool {
        const auto pos = _colname.find( helpers::Macros::text_field() );
        return pos != std::string::npos;
    };

    const auto remove_text_field_marker =
        []( const std::string& _colname ) -> std::string {
        const auto pos = _colname.find( helpers::Macros::text_field() );
        assert_true( pos != std::string::npos );
        return _colname.substr( 0, pos );
    };

    const auto add_text_fields =
        [has_text_field_marker, remove_text_field_marker](
            const containers::Schema& _schema ) -> containers::Schema {
        const auto text_fields = stl::collect::vector<std::string>(
            _schema.unused_strings_ | VIEWS::filter( has_text_field_marker ) |
            VIEWS::transform( remove_text_field_marker ) );

        return containers::Schema{
            .categoricals_ = _schema.categoricals_,
            .discretes_ = _schema.discretes_,
            .join_keys_ = _schema.join_keys_,
            .name_ = _schema.name_,
            .numericals_ = _schema.numericals_,
            .targets_ = _schema.targets_,
            .text_ = stl::join::vector<std::string>(
                { _schema.text_, text_fields } ),
            .time_stamps_ = _schema.time_stamps_,
            .unused_floats_ = _schema.unused_floats_,
            .unused_strings_ = _schema.unused_strings_ };
    };

    const auto is_not_text_field =
        []( const containers::Schema& _schema ) -> bool {
        return _schema.name_.find( helpers::Macros::text_field() ) ==
               std::string::npos;
    };

    const auto staging_schema_population =
        add_text_fields( *modified_population_schema() );

    const auto staging_schema_peripheral =
        stl::collect::vector<containers::Schema>(
            *modified_peripheral_schema() | VIEWS::filter( is_not_text_field ) |
            VIEWS::transform( add_text_fields ) );

    return std::make_pair(
        staging_schema_population, staging_schema_peripheral );
}

// ----------------------------------------------------------------------------

std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
Pipeline::modify_data_frames(
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const std::shared_ptr<const communication::Logger>& _logger,
    Poco::Net::StreamSocket* _socket ) const
{
    // ----------------------------------------------------------------------

    const auto socket_logger =
        std::make_shared<communication::SocketLogger>( _logger, true, _socket );

    socket_logger->log( "Staging..." );

    // ----------------------------------------------------------------------

    const auto population = *JSON::get_object( obj(), "data_model_" );

    const auto peripheral_names = parse_peripheral();

    assert_true( peripheral_names );

    // ----------------------------------------------------------------------

    auto population_df = _population_df;

    auto peripheral_dfs = _peripheral_dfs;

    // ----------------------------------------------------------------------

    DataFrameModifier::add_time_stamps(
        population, *peripheral_names, &population_df, &peripheral_dfs );

    // ----------------------------------------------------------------------

    DataFrameModifier::add_join_keys(
        population, *peripheral_names, &population_df, &peripheral_dfs );

    // ----------------------------------------------------------------------

    const auto placeholder =
        PlaceholderMaker::make_placeholder( population, "t1" );

    const auto joined_peripheral_names =
        PlaceholderMaker::make_peripheral( placeholder );

    Staging::join_tables(
        *peripheral_names,
        placeholder.name_,
        joined_peripheral_names,
        &population_df,
        &peripheral_dfs );

    // ----------------------------------------------------------------------

    socket_logger->log( "Progress: 100%." );

    // ----------------------------------------------------------------------

    return std::make_pair( population_df, peripheral_dfs );

    // ----------------------------------------------------------------------
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
    const auto ptr = JSON::get_object( obj(), "data_model_" );

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

std::tuple<
    containers::Features,
    containers::CategoricalFeatures,
    containers::Features>
Pipeline::retrieve_features( const containers::DataFrame& _df ) const
{
    containers::Features autofeatures;

    containers::Features numerical_features;

    for ( size_t i = 0; i < _df.num_numericals(); ++i )
        {
            const auto col = _df.numerical( i );

            numerical_features.push_back( col.data_ptr() );

            if ( col.name().substr( 0, 8 ) == "feature_" )
                {
                    autofeatures.push_back( numerical_features.back() );
                }
        }

    containers::CategoricalFeatures categorical_features;

    for ( size_t i = 0; i < _df.num_categoricals(); ++i )
        {
            const auto col = _df.categorical( i );

            categorical_features.push_back( col.data_ptr() );
        }

    return std::make_tuple(
        numerical_features, categorical_features, autofeatures );
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

void Pipeline::save(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::string& _temp_dir,
    const std::string& _path,
    const std::string& _name ) const
{
    // ------------------------------------------------------------------

    auto tfile = Poco::TemporaryFile( _temp_dir );

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

    const auto sql_code =
        to_sql( _categories, true, true, helpers::SQLDialectParser::SQLITE3 );

    utils::SQLDependencyTracker( tfile.path() + "/SQL/" )
        .save_dependencies( sql_code );

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
    const auto to_obj = []( const helpers::Schema& s ) {
        return s.to_json_obj();
    };

    Poco::JSON::Object pipeline_json;

    pipeline_json.set( "allow_http_", allow_http() );

    pipeline_json.set( "creation_time_", creation_time() );

    pipeline_json.set(
        "df_fingerprints_", JSON::vector_to_array( df_fingerprints() ) );

    pipeline_json.set(
        "fl_fingerprints_", JSON::vector_to_array( fl_fingerprints() ) );

    pipeline_json.set(
        "fs_fingerprints_", JSON::vector_to_array( fs_fingerprints() ) );

    pipeline_json.set(
        "modified_peripheral_schema_",
        stl::collect::array(
            *modified_peripheral_schema() | VIEWS::transform( to_obj ) ) );

    pipeline_json.set(
        "modified_population_schema_",
        modified_population_schema()->to_json_obj() );

    pipeline_json.set(
        "preprocessor_fingerprints_",
        JSON::vector_to_array( preprocessor_fingerprints() ) );

    pipeline_json.set( "targets_", JSON::vector_to_array( targets() ) );

    pipeline_json.set(
        "peripheral_schema_",
        stl::collect::array(
            *peripheral_schema() | VIEWS::transform( to_obj ) ) );

    pipeline_json.set(
        "population_schema_", population_schema()->to_json_obj() );

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
    const containers::DataFrame& _population_df,
    const std::string& _population_name,
    const containers::Features& _yhat )
{
    // ------------------------------------------------

    containers::Features y;

    for ( size_t i = 0; i < _population_df.num_targets(); ++i )
        {
            y.push_back( _population_df.target( i ).data_ptr() );
        }

    // ------------------------------------------------

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

    debug_log( "Calculating score..." );

    auto obj = metrics::Scorer::score( is_classification(), _yhat, y );

    obj.set( "set_used_", _population_name );

    scores().from_json_obj( obj );

    scores().to_history();

    // ------------------------------------------------

    return metrics::Scorer::get_metrics( obj );

    // ------------------------------------------------
}

// ----------------------------------------------------------------------------

void Pipeline::score_after_fitting( const TransformParams& _params )
{
    auto [numerical_features, categorical_features, _] =
        make_features( _params );

    categorical_features =
        _params.predictor_impl_.transform_encodings( categorical_features );

    const auto yhat =
        generate_predictions( categorical_features, numerical_features );

    const auto population_json =
        *JSON::get_object( _params.cmd_, "population_df_" );

    const auto name = JSON::get_value<std::string>( population_json, "name_" );

    score( _params.population_df_, name, yhat );
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
    const auto to_json_obj = []( const containers::Schema& _schema ) {
        return _schema.to_json_obj();
    };

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

    json_obj.set(
        "peripheral_schema_",
        stl::collect::array(
            *peripheral_schema() | VIEWS::transform( to_json_obj ) ) );

    json_obj.set( "population_schema_", population_schema()->to_json_obj() );

    json_obj.set( "data_model_", JSON::get_object( obj(), "data_model_" ) );

    json_obj.set( "predictors_", JSON::get_array( obj(), "predictors_" ) );

    json_obj.set(
        "preprocessors_", JSON::get_array( obj(), "preprocessors_" ) );

    json_obj.set( "tags_", JSON::get_array( obj(), "tags_" ) );

    json_obj.set( "targets_", JSON::vector_to_array( targets() ) );

    // -------------------------------------------------

    auto scores_obj = scores().to_json_obj();

    const auto modified_names =
        helpers::Macros::modify_colnames( scores().feature_names() );

    const auto feature_names = JSON::vector_to_array( modified_names );

    scores_obj.set( "feature_names_", feature_names );

    // -------------------------------------------------

    json_obj.set( "scores_", scores_obj );

    // -------------------------------------------------

    return json_obj;
}

// ----------------------------------------------------------------------------

std::vector<std::string> Pipeline::preprocessors_to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator ) const
{
    const auto to_sql =
        [_categories, _sql_dialect_generator](
            const std::shared_ptr<const preprocessors::Preprocessor>& _p )
        -> std::vector<std::string> {
        assert_true( _p );
        return _p->to_sql( _categories, _sql_dialect_generator );
    };

    return stl::join::vector<std::string>(
        preprocessors_ | VIEWS::transform( to_sql ) );
}

// ----------------------------------------------------------------------------

std::vector<std::string> Pipeline::staging_to_sql(
    const bool _targets,
    const std::shared_ptr<const helpers::SQLDialectGenerator>&
        _sql_dialect_generator ) const
{
    const auto needs_targets =
        []( const std::shared_ptr<
            const featurelearners::AbstractFeatureLearner>& _f ) -> bool {
        assert_true( _f );
        return _f->population_needs_targets();
    };

    const auto population_needs_targets =
        _targets |
        std::any_of(
            feature_learners_.begin(), feature_learners_.end(), needs_targets );

    const auto [placeholder, peripheral_names] = make_placeholder();

    assert_true( placeholder );

    assert_true( peripheral_names );

    const auto peripheral_needs_targets =
        placeholder->infer_needs_targets( *peripheral_names );

    const auto [staging_schema_population, staging_schema_peripheral] =
        make_staging_schemata();

    assert_true( _sql_dialect_generator );

    return _sql_dialect_generator->make_staging_tables(
        population_needs_targets,
        peripheral_needs_targets,
        staging_schema_population,
        staging_schema_peripheral );
}

// ----------------------------------------------------------------------------

std::string Pipeline::to_sql(
    const std::shared_ptr<const std::vector<strings::String>>& _categories,
    const bool _targets,
    const bool _full_pipeline,
    const std::string& _dialect ) const
{
    assert_true(
        feature_learners_.size() == predictor_impl().autofeatures().size() );

    const auto sql_dialect_generator =
        helpers::SQLDialectParser::parse( _dialect );

    const auto staging = _full_pipeline
                             ? staging_to_sql( _targets, sql_dialect_generator )
                             : std::vector<std::string>();

    const auto preprocessing =
        _full_pipeline
            ? preprocessors_to_sql( _categories, sql_dialect_generator )
            : std::vector<std::string>();

    const auto features = feature_learners_to_sql(
        _categories, _targets, _full_pipeline, sql_dialect_generator );

    const auto sql =
        stl::join::vector<std::string>( { staging, preprocessing, features } );

    const auto autofeatures = make_feature_names();

    const auto target_names = _targets ? targets() : std::vector<std::string>();

    return sql_dialect_generator->make_sql(
        modified_population_schema()->name_,
        autofeatures,
        sql,
        target_names,
        predictor_impl().categorical_colnames(),
        predictor_impl().numerical_colnames() );
}

// ----------------------------------------------------------------------------

std::pair<containers::Features, containers::CategoricalFeatures>
Pipeline::transform(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const communication::Logger>& _logger,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs,
    const dependency::DataFrameTracker& _data_frame_tracker,
    const std::shared_ptr<const containers::Encoding> _categories,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------------------------

    if ( feature_learners_.size() == 0 && predictors_.size() == 0 )
        {
            throw std::runtime_error( "Pipeline has not been fitted!" );
        }

    // -------------------------------------------------------------------------

    auto [population_df, peripheral_dfs] =
        modify_data_frames( _population_df, _peripheral_dfs, _logger, _socket );

    // -------------------------------------------------------------------------

    std::tie( population_df, peripheral_dfs ) = apply_preprocessors(
        _cmd, _logger, population_df, peripheral_dfs, _categories, _socket );

    // -------------------------------------------------------------------------

    const auto params = TransformParams{
        .cmd_ = _cmd,
        .data_frames_ = _data_frames,
        .data_frame_tracker_ = _data_frame_tracker,
        .dependencies_ = dependencies(),
        .logger_ = _logger,
        .original_peripheral_dfs_ = _peripheral_dfs,
        .original_population_df_ = _population_df,
        .peripheral_dfs_ = peripheral_dfs,
        .population_df_ = population_df,
        .predictor_impl_ = predictor_impl(),
        .pred_tracker_ = nullptr,
        .purpose_ = "",
        .autofeatures_ = nullptr,
        .predictors_ = nullptr,
        .socket_ = _socket };

    const auto [numerical_features, categorical_features, _] =
        make_features( params );

    // -------------------------------------------------------------------------
    // If we do not want to score or predict, then we can stop here.

    const bool score =
        _cmd.has( "score_" ) && JSON::get_value<bool>( _cmd, "score_" );

    const bool predict =
        _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" );

    if ( !score && !predict )
        {
            return std::make_pair( numerical_features, categorical_features );
        }

    // -------------------------------------------------------------------------

    if ( num_predictors_per_set() == 0 )
        {
            throw std::invalid_argument(
                "You cannot call .predict(...) or .score(...) on a pipeline "
                "that doesn't have any predictors." );
        }

    // -------------------------------------------------------------------------

    const auto ncols = numerical_features.size();

    if ( score && ncols > 0 )
        {
            const auto nrows = numerical_features[0]->size();

            calculate_feature_stats(
                numerical_features, nrows, ncols, _cmd, population_df );
        }

    //-------------------------------------------------------------------------

    const auto transformed_categorical_features =
        predictor_impl().transform_encodings( categorical_features );

    const auto predictions = generate_predictions(
        transformed_categorical_features, numerical_features );

    return std::make_pair( predictions, containers::CategoricalFeatures() );

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
                    assert_msg(
                        vec.size() == n,
                        "vec.size(): " + std::to_string( vec.size() ) +
                            ", n: " + std::to_string( n ) );

                    temp->add( vec.at( i ) );
                }

            transposed->add( temp );
        }

    return transposed;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

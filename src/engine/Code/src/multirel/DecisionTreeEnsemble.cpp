#include "multirel/ensemble/ensemble.hpp"

namespace multirel
{
namespace ensemble
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const descriptors::Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const containers::Placeholder> &_placeholder,
    const std::shared_ptr<const std::vector<containers::Placeholder>>
        &_peripheral_schema,
    const std::shared_ptr<const containers::Placeholder> &_population_schema )
    : impl_(
          _hyperparameters,
          *_peripheral,
          *_placeholder,
          _peripheral_schema,
          _population_schema )
{
    placeholder().check_data_model( peripheral(), true );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const Poco::JSON::Object &_json_obj )
    : impl_(
          std::make_shared<const descriptors::Hyperparameters>(
              *JSON::get_object( _json_obj, "hyperparameters_" ) ),
          JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "peripheral_" ) ),
          containers::Placeholder(
              *JSON::get_object( _json_obj, "placeholder_" ) ),
          nullptr,
          nullptr )
{
    *this = from_json_obj( _json_obj );
    placeholder().check_data_model( peripheral(), true );
}

// ----------------------------------------------------------------------------

std::list<decisiontrees::DecisionTree> DecisionTreeEnsemble::build_candidates(
    const size_t _ix_feature,
    const std::vector<descriptors::SameUnits> &_same_units,
    const decisiontrees::TableHolder &_table_holder )
{
    assert_true( random_number_generator() );

    return CandidateTreeBuilder::build_candidates(
        _table_holder,
        _same_units,
        _ix_feature,
        hyperparameters(),
        &aggregation_impl(),
        random_number_generator().get(),
        comm() );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility_of_targets(
    const containers::DataFrame &_population_table )
{
    if ( _population_table.num_targets() < 1 )
        {
            throw std::invalid_argument(
                "The population table must have at least one target "
                "column!" );
        }

    for ( size_t j = 0; j < _population_table.num_targets(); ++j )
        {
            for ( size_t i = 0; i < _population_table.nrows(); ++i )
                {
                    if ( std::isnan( _population_table.target( i, j ) ) ||
                         std::isinf( _population_table.target( i, j ) ) )
                        {
                            throw std::invalid_argument(
                                "Target values can not be NULL or "
                                "infinite!" );
                        }
                }
        }

    if ( is_classification() )
        {
            for ( size_t j = 0; j < _population_table.num_targets(); ++j )
                {
                    for ( size_t i = 0; i < _population_table.nrows(); ++i )
                        {
                            if ( _population_table.target( i, j ) != 0.0 &&
                                 _population_table.target( i, j ) != 1.0 )
                                {
                                    throw std::invalid_argument(
                                        "Target values for a "
                                        "classification "
                                        "problem have to be 0.0 or 1.0!" );
                                }
                        }
                }
        }
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importances(
    const std::vector<Float> &_importance_factors ) const

{
    auto importance_maker = utils::ImportanceMaker();

    assert_true( _importance_factors.size() == trees().size() );

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            const auto importances = column_importance_for_tree(
                _importance_factors.at( i ), trees().at( i ) );

            importance_maker.merge( importances );
        }

    if ( impl().population_schema_ )
        {
            importance_maker.fill_zeros(
                population_schema(), placeholder().name(), true );

            assert_true( peripheral_schema().size() == peripheral().size() );

            for ( size_t i = 0; i < peripheral().size(); ++i )
                {
                    importance_maker.fill_zeros(
                        peripheral_schema().at( i ),
                        peripheral().at( i ),
                        false );
                }
        }

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importance_for_tree(
    const Float _importance_factor,
    const decisiontrees::DecisionTree &_tree ) const
{
    if ( _importance_factor == 0.0 )
        {
            return std::map<helpers::ColumnDescription, Float>();
        }

    const auto p = _tree.ix_perip_used();

    assert_true( subensembles_avg_.size() == subensembles_sum_.size() );

    assert_true( p < subensembles_avg_.size() );

    const auto &sub_avg = subensembles_avg_.at( p );

    const auto &sub_sum = subensembles_sum_.at( p );

    assert_true( ( sub_avg && true ) == ( sub_sum && true ) );

    assert_true(
        !sub_avg || ( sub_avg->num_features() == sub_sum->num_features() ) );

    const size_t num_subfeatures = sub_avg ? sub_avg->num_features() : 0;

    auto importance_maker = utils::ImportanceMaker( num_subfeatures );

    _tree.column_importances( &importance_maker );

    if ( sub_avg )
        {
            const auto importances_avg = sub_avg->column_importances(
                importance_maker.importance_factors_avg() );

            importance_maker.merge( importances_avg );

            const auto importances_sum = sub_sum->column_importances(
                importance_maker.importance_factors_sum() );

            importance_maker.merge( importances_sum );
        }

    importance_maker.normalize();

    importance_maker.multiply( _importance_factor );

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::extract_schemas(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    impl().population_schema_ = std::make_shared<const containers::Placeholder>(
        _population.to_schema() );

    auto peripheral_schema =
        std::make_shared<std::vector<containers::Placeholder>>();

    for ( auto &df : _peripheral )
        {
            peripheral_schema->push_back( df.to_schema() );
        }

    impl().peripheral_schema_ = peripheral_schema;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    // ------------------------------------------------------
    // Some plausibility checks.

    if ( num_features() != 0 )
        {
            throw std::runtime_error(
                "Multirel model has already been fitted!" );
        }

    if ( _population.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    check_plausibility_of_targets( _population );

    // ------------------------------------------------------
    // We need to store the schemas for future reference.

    extract_schemas( _population, _peripheral );

    // ------------------------------------------------------

    debug_log( "Building communicator..." );

    auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    // ------------------------------------------------------
    // Build thread_nums

    debug_log( "Building the thread nums..." );

    const auto [thread_nums, n_unique] =
        utils::DataFrameScatterer::build_thread_nums(
            _population.join_keys(), num_threads );

    // ------------------------------------------------------
    // Take care of an edge case.

    if ( num_threads > n_unique )
        {
            num_threads = n_unique;
        }

    // ------------------------------------------------------

    multithreading::Communicator comm( num_threads );

    // ------------------------------------------------------
    // Create deep copies of this ensemble.

    debug_log( "Building deep copies..." );

    std::vector<ensemble::DecisionTreeEnsemble> ensembles;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            ensembles.push_back( *this );
        }

    // ------------------------------------------------------
    // Spawn threads.

    assert_true( impl().hyperparameters_ );

    debug_log( "Spawning threads..." );

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::fit_ensemble,
                i + 1,
                thread_nums,
                impl().hyperparameters_,
                _population,
                _peripheral,
                placeholder(),
                peripheral(),
                std::shared_ptr<const logging::AbstractLogger>(),
                &comm,
                &ensembles[i] ) );
        }

    // ------------------------------------------------------
    // Train ensemble in main thread.

    debug_log( "Training in main thread..." );

    try
        {
            Threadutils::fit_ensemble(
                0,
                thread_nums,
                impl().hyperparameters_,
                _population,
                _peripheral,
                placeholder(),
                peripheral(),
                _logger,
                &comm,
                this );
        }
    catch ( std::exception &e )
        {
            for ( auto &thr : threads )
                {
                    thr.join();
                }

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------
    // Join all other threads

    debug_log( "Joining threads..." );

    for ( auto &thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const size_t _num_features,
    optimizationcriteria::OptimizationCriterion *_opt,
    multithreading::Communicator *_comm )
{
    // ----------------------------------------------------------------

    set_comm( _comm );

    // ----------------------------------------------------------------

    debug_log( "fit: Beginning to fit features..." );

    // ----------------------------------------------------------------

    if ( _table_holder->main_tables_.size() == 0 )
        {
            throw std::invalid_argument(
                "You need to provide a population table!" );
        }

    if ( _table_holder->main_tables_[0].nrows() == 0 )
        {
            throw std::invalid_argument(
                "Your population table needs to contain at least one "
                "row!" );
        }

    // ----------------------------------------------------------------
    // Store the column numbers, so we can make sure that the user
    // passes properly formatted data to predict(...)

    debug_log( "fit: Storing column numbers..." );

    assert_true( _table_holder->main_tables_.size() > 0 );

    // ----------------------------------------------------------------
    // Make sure that the data passed by the user is plausible

    debug_log( "fit: Checking plausibility of input..." );

    assert_true( _table_holder->main_tables_.size() > 0 );

    // ----------------------------------------------------------------
    // Store names of the targets

    if ( _table_holder->main_tables_[0].num_targets() > 0 )
        {
            targets() = { _table_holder->main_tables_[0].target_name( 0 ) };
        }

    // ----------------------------------------------------------------
    // If there are any subfeatures, fit them.

    SubtreeHelper::fit_subensembles(
        _table_holder,
        _logger,
        *this,
        _opt,
        _comm,
        &subensembles_avg_,
        &subensembles_sum_ );

    // ----------------------------------------------------------------
    // If there are any subfeatures, create them.

    const auto subpredictions = SubtreeHelper::make_predictions(
        *_table_holder, subensembles_avg_, subensembles_sum_, _logger, _comm );

    const auto subfeatures =
        SubtreeHelper::make_subfeatures( *_table_holder, subpredictions );

    // ----------------------------------------------------------------
    // aggregations::AggregationImpl stores most of the data for the
    // aggregations. We do not want to reallocate the data all the time.

    assert_true( _table_holder->main_tables_.size() > 0 );

    aggregation_impl().reset( new aggregations::AggregationImpl(
        _table_holder->main_tables_[0].nrows() ) );

    // ----------------------------------------------------------------
    // Columns that share the same units are candidates for direct
    // comparison

    debug_log( "fit: Identifying same units..." );

    assert_true( _table_holder->main_tables_.size() > 0 );
    assert_true(
        _table_holder->main_tables_.size() ==
        _table_holder->peripheral_tables_.size() );

    auto same_units = SameUnitIdentifier::identify_same_units(
        _table_holder->peripheral_tables_,
        _table_holder->main_tables_[0].df() );

    // ----------------------------------------------------------------
    // containers::Match weights are needed for the random-forest-like
    // functionality

    debug_log( "fit: Setting up sampling..." );

    if ( hyperparameters().seed_ < 0 )
        {
            throw std::invalid_argument( "Seed must be positive!" );
        }

    assert_true( _table_holder->main_tables_.size() > 0 );

    const auto nrows = _table_holder->main_tables_[0].nrows();

    auto sample_weights = std::make_shared<std::vector<Float>>( nrows, 1.0 );

    random_number_generator().reset( new std::mt19937(
        static_cast<unsigned int>( hyperparameters().seed_ ) ) );

    // ----------------------------------------------------------------
    // containers::Match containers are pointers to simple structs, which
    // represent a match between a key in the peripheral table and a key in
    // the population table.

    debug_log( "fit: Creating matches..." );

    const auto num_peripheral = _table_holder->peripheral_tables_.size();

    assert_true( _table_holder->main_tables_.size() == num_peripheral );

    std::vector<containers::Matches> matches( num_peripheral );

    std::vector<containers::MatchPtrs> match_ptrs( num_peripheral );

    if ( hyperparameters().sampling_factor_ <= 0.0 )
        {
            for ( size_t i = 0; i < num_peripheral; ++i )
                {
                    matches[i] = utils::Matchmaker::make_matches(
                        _table_holder->main_tables_[i],
                        _table_holder->peripheral_tables_[i],
                        sample_weights,
                        hyperparameters().use_timestamps_ );

                    match_ptrs[i] =
                        utils::Matchmaker::make_pointers( &matches[i] );
                }
        }

    // ----------------------------------------------------------------

    if ( !is_subensemble() )
        {
            utils::Logger::log(
                "MultirelModel: Training features...", _logger, _comm );
        }
    else
        {
            utils::Logger::log(
                "MultirelModel: Training subfeatures...", _logger, _comm );
        }

    // ----------------------------------------------------------------

    for ( size_t ix_feature = 0; ix_feature < _num_features; ++ix_feature )
        {
            // ----------------------------------------------------------------
            // containers::Match for a random-forest-like algorithm - can be
            // turned off by setting _sampling_rate to 0.0

            debug_log( "fit: Sampling from population..." );

            if ( hyperparameters().sampling_factor_ > 0.0 )
                {
                    sample_weights = _opt->make_sample_weights();

                    assert_true( _table_holder->main_tables_.size() > 0 );

                    assert_true(
                        sample_weights->size() ==
                        _table_holder->main_tables_[0].nrows() );

                    for ( size_t i = 0; i < num_peripheral; ++i )
                        {
                            matches[i] = utils::Matchmaker::make_matches(
                                _table_holder->main_tables_[i],
                                _table_holder->peripheral_tables_[i],
                                sample_weights,
                                hyperparameters().use_timestamps_ );

                            match_ptrs[i] =
                                utils::Matchmaker::make_pointers( &matches[i] );
                        }
                }

            // ----------------------------------------------------------------
            // Reset the optimization criterion based on the residuals
            // generated
            // from the last prediction

            debug_log( "fit: Preparing optimization criterion..." );

            _opt->init( *sample_weights );

            // ----------------------------------------------------------------

            debug_log( "fit: Building candidates..." );

            auto candidate_trees =
                build_candidates( ix_feature, same_units, *_table_holder );

            // ----------------------------------------------------------------
            // Fit the trees

            debug_log( "fit: Fitting features..." );

            TreeFitter tree_fitter(
                impl().hyperparameters_,
                random_number_generator().get(),
                comm() );

            tree_fitter.fit(
                *_table_holder,
                subfeatures,
                &matches,
                &match_ptrs,
                _opt,
                &candidate_trees,
                &trees() );

            // -------------------------------------------------------------
            // Recalculate residuals, which is needed for the gradient
            // boosting algorithm.

            debug_log( "fit: Recalculating residuals..." );

            if ( hyperparameters().shrinkage_ > 0.0 )
                {
                    const auto ix = last_tree()->ix_perip_used();

                    const auto agg =
                        last_tree()->make_aggregation( enums::Mode::transform );

                    agg->set_aggregation_impl( &aggregation_impl() );

                    std::vector<Float> new_feature = last_tree()->transform(
                        _table_holder->main_tables_[ix],
                        _table_holder->peripheral_tables_[ix],
                        subfeatures[ix],
                        hyperparameters().use_timestamps_,
                        agg.get() );

                    _opt->update_yhat_old( *sample_weights, new_feature );

                    _opt->calc_residuals();
                }

            // -------------------------------------------------------------

            debug_log(
                "Trained FEATURE_" + std::to_string( ix_feature + 1 ) + "." );

            const auto progress = ( ( ix_feature + 1 ) * 100 ) / _num_features;

            utils::Logger::log(
                "Trained FEATURE_" + std::to_string( ix_feature + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );

            // -------------------------------------------------------------
        }

    // ----------------------------------------------------------------
    // Clean up

    aggregation_impl().reset();

    // ----------------------------------------------------------------
    // Return message

    debug_log( "fit: Done..." );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble DecisionTreeEnsemble::from_json_obj(
    const Poco::JSON::Object &_json_obj ) const
{
    // ----------------------------------------

    DecisionTreeEnsemble model( *this );

    // ----------------------------------------
    // Extract hyperparameters

    model.impl().hyperparameters_.reset( new descriptors::Hyperparameters(
        *JSON::get_object( _json_obj, "hyperparameters_" ) ) );

    // ----------------------------------------
    // Extract placeholders.

    model.impl().peripheral_ =
        std::vector<std::string>( JSON::array_to_vector<std::string>(
            JSON::get_array( _json_obj, "peripheral_" ) ) );

    model.impl().placeholder_.reset( new containers::Placeholder(
        *JSON::get_object( _json_obj, "placeholder_" ) ) );

    // ----------------------------------------

    // TODO: For backwards compatability.
    model.allow_http() = _json_obj.has( "allow_http_" )
                             ? JSON::get_value<bool>( _json_obj, "allow_http_" )
                             : false;

    // ----------------------------------------
    // Extract features.

    auto features = JSON::get_array( _json_obj, "features_" );

    for ( size_t i = 0; i < features->size(); ++i )
        {
            auto obj = features->getObject( static_cast<unsigned int>( i ) );

            model.trees().push_back( decisiontrees::DecisionTree(
                model.hyperparameters().tree_hyperparameters_, *obj ) );
        }

    // ----------------------------------------
    // Extract targets

    model.targets() = JSON::array_to_vector<std::string>(
        JSON::get_array( _json_obj, "targets_" ) );

    // ----------------------------------------
    // Extract subensembles_avg_

    auto features_avg = JSON::get_array( _json_obj, "subfeatures1_" );

    model.subensembles_avg_ =
        std::vector<containers::Optional<DecisionTreeEnsemble>>(
            features_avg->size() );

    for ( size_t i = 0; i < features_avg->size(); ++i )
        {
            auto obj =
                features_avg->getObject( static_cast<unsigned int>( i ) );

            if ( obj )
                {
                    model.subensembles_avg_[i].reset(
                        new DecisionTreeEnsemble( *obj ) );
                }
        }

    // ----------------------------------------
    // Extract subensembles_sum_

    auto features_sum = JSON::get_array( _json_obj, "subfeatures2_" );

    model.subensembles_sum_ =
        std::vector<containers::Optional<DecisionTreeEnsemble>>(
            features_sum->size() );

    for ( size_t i = 0; i < features_sum->size(); ++i )
        {
            auto obj =
                features_sum->getObject( static_cast<unsigned int>( i ) );

            if ( obj )
                {
                    model.subensembles_sum_[i].reset(
                        new DecisionTreeEnsemble( *obj ) );
                }
        }

    // ------------------------------------------------------------------------

    // Subensembles in a snowflake model do not have schemata.
    if ( _json_obj.has( "population_schema_" ) )
        {
            model.impl().population_schema_ =
                std::make_shared<const containers::Placeholder>(
                    *JSON::get_object( _json_obj, "population_schema_" ) );

            std::vector<containers::Placeholder> peripheral_schema;

            const auto peripheral_arr =
                *JSON::get_array( _json_obj, "peripheral_schema_" );

            for ( size_t i = 0; i < peripheral_arr.size(); ++i )
                {
                    const auto ptr = peripheral_arr.getObject(
                        static_cast<unsigned int>( i ) );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "peripheral_schema_, element " +
                                std::to_string( i ) + " is not an Object!" );
                        }

                    peripheral_schema.push_back(
                        containers::Placeholder( *ptr ) );
                }

            model.impl().peripheral_schema_ =
                std::make_shared<const std::vector<containers::Placeholder>>(
                    peripheral_schema );
        }

    // ----------------------------------------

    return model;

    // -------------------------------------------
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_fname ) const
{
    std::ofstream output( _fname );

    output << to_json();

    output.close();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::select_features( const std::vector<size_t> &_index )
{
    assert_true( _index.size() >= trees().size() );

    const auto num_selected_features =
        ( hyperparameters().num_selected_features() > 0 &&
          hyperparameters().num_selected_features() < _index.size() )
            ? ( hyperparameters().num_selected_features() )
            : ( _index.size() );

    std::vector<decisiontrees::DecisionTree> selected_trees;

    for ( size_t i = 0; i < num_selected_features; ++i )
        {
            const auto ix = _index[i];

            if ( ix < trees().size() )
                {
                    selected_trees.push_back( trees()[ix] );
                }
        }

    trees() = selected_trees;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj(
    const bool _schema_only ) const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "type_", "MultirelModel" );

    // ----------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    // ----------------------------------------

    obj.set( "peripheral_", JSON::vector_to_array( peripheral() ) );

    obj.set( "placeholder_", placeholder().to_json_obj() );

    // ----------------------------------------

    if ( impl().population_schema_ )
        {
            Poco::JSON::Array peripheral_schema_arr;

            for ( auto &p : peripheral_schema() )
                {
                    peripheral_schema_arr.add( p.to_json_obj() );
                }

            obj.set( "peripheral_schema_", peripheral_schema_arr );

            obj.set( "population_schema_", population_schema().to_json_obj() );
        }

    // ----------------------------------------

    if ( _schema_only )
        {
            return obj;
        }

    // ----------------------------------------

    obj.set( "allow_http_", allow_http() );

    // ----------------------------------------
    // Extract features

    Poco::JSON::Array features;

    for ( auto &tree : trees() )
        {
            features.add( tree.to_json_obj() );
        }

    obj.set( "features_", features );

    // ----------------------------------------
    // Extract targets

    obj.set( "targets_", JSON::vector_to_array<std::string>( targets() ) );

    // ----------------------------------------
    // Extract subensembles_avg_

    Poco::JSON::Array features_avg;

    for ( const auto &sub : subensembles_avg_ )
        {
            if ( sub )
                {
                    features_avg.add( sub->to_json_obj() );
                }
            else
                {
                    features_avg.add( Poco::Dynamic::Var() );
                }
        }

    obj.set( "subfeatures1_", features_avg );

    // ----------------------------------------
    // Extract subensembles_sum_

    Poco::JSON::Array features_sum;

    for ( const auto &sub : subensembles_sum_ )
        {
            if ( sub )
                {
                    features_sum.add( sub->to_json_obj() );
                }
            else
                {
                    features_sum.add( Poco::Dynamic::Var() );
                }
        }

    obj.set( "subfeatures2_", features_sum );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> DecisionTreeEnsemble::to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const std::string &_feature_prefix,
    const size_t _offset,
    const bool _subfeatures ) const
{
    assert_true( _categories );

    std::vector<std::string> sql;

    if ( _subfeatures )
        {
            assert_true( subensembles_avg_.size() == subensembles_sum_.size() );

            for ( size_t i = 0; i < subensembles_avg_.size(); ++i )
                {
                    if ( subensembles_avg_.at( i ) )
                        {
                            const auto sub_avg =
                                subensembles_avg_.at( i )->to_sql(
                                    _categories,
                                    _feature_prefix + std::to_string( i + 1 ) +
                                        "_",
                                    0,
                                    true );

                            sql.insert(
                                sql.end(), sub_avg.begin(), sub_avg.end() );

                            assert_true( subensembles_sum_.at( i ) );

                            const auto sub_sum =
                                subensembles_sum_.at( i )->to_sql(
                                    _categories,
                                    _feature_prefix + std::to_string( i + 1 ) +
                                        "_",
                                    subensembles_avg_.at( i )->num_features(),
                                    true );

                            sql.insert(
                                sql.end(), sub_sum.begin(), sub_sum.end() );
                        }
                }
        }

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            sql.push_back( trees().at( i ).to_sql(
                *_categories,
                _feature_prefix,
                std::to_string( _offset + i + 1 ),
                hyperparameters().use_timestamps_ ) );
        }

    return sql;
}

// ----------------------------------------------------------------------------

containers::Features DecisionTreeEnsemble::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::optional<std::vector<size_t>> &_index,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    // ------------------------------------------------------
    // Check plausibility.

    if ( _population.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    // ------------------------------------------------------
    // If no index is passed, take all features.

    std::vector<size_t> index;

    if ( _index )
        {
            index = *_index;
        }
    else
        {
            index = std::vector<size_t>( num_features() );

            for ( size_t i = 0; i < index.size(); ++i )
                {
                    index[i] = i;
                }
        }

    // ------------------------------------------------------
    // thread_nums signify the thread number that a particular row belongs
    // to. The idea is to separate the join keys as clearly as possible.

    auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    // ------------------------------------------------------
    // Build thread_nums

    debug_log( "Building the thread nums..." );

    const auto [thread_nums, n_unique] =
        utils::DataFrameScatterer::build_thread_nums(
            _population.join_keys(), num_threads );

    // ------------------------------------------------------
    // Take care of an edge case.

    if ( num_threads > n_unique )
        {
            num_threads = n_unique;
        }

    // -------------------------------------------------------

    multithreading::Communicator comm( num_threads );

    // -------------------------------------------------------
    // Launch threads and generate predictions on the subviews.

    auto features = containers::Features( index.size() );

    for ( auto &f : features )
        {
            f = std::make_shared<std::vector<Float>>( _population.nrows() );
        }

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::transform_ensemble,
                i + 1,
                thread_nums,
                impl().hyperparameters_,
                _population,
                _peripheral,
                index,
                std::shared_ptr<const logging::AbstractLogger>(),
                *this,
                &comm,
                &features ) );
        }

    // ------------------------------------------------------
    // Transform in main thread.

    try
        {
            Threadutils::transform_ensemble(
                0,
                thread_nums,
                impl().hyperparameters_,
                _population,
                _peripheral,
                index,
                _logger,
                *this,
                &comm,
                &features );
        }
    catch ( std::exception &e )
        {
            for ( auto &thr : threads )
                {
                    thr.join();
                }

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------

    for ( auto &thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------

    return features;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Predictions DecisionTreeEnsemble::transform(
    const decisiontrees::TableHolder &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator *_comm,
    containers::Optional<aggregations::AggregationImpl> *_impl ) const
{
    // ----------------------------------------------------------------

    assert_true( _table_holder.main_tables_.size() > 0 );

    // ----------------------------------------------------------------
    // If there are any subfeatures, create them.

    const auto subpredictions = SubtreeHelper::make_predictions(
        _table_holder, subensembles_avg_, subensembles_sum_, _logger, _comm );

    const auto subfeatures =
        SubtreeHelper::make_subfeatures( _table_holder, subpredictions );

    // ----------------------------------------------------------------
    // Generate the actual predictions.

    utils::Logger::log(
        "MultirelModel: Building subfeatures...", _logger, _comm );

    containers::Predictions predictions;

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            const auto new_prediction =
                transform( _table_holder, subfeatures, i, _impl );

            const auto progress = ( ( i + 1 ) * 100 ) / trees().size();

            utils::Logger::log(
                "Built FEATURE_" + std::to_string( i + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );

            predictions.emplace_back( std::move( new_prediction ) );
        }

    // ----------------------------------------------------------------

    return predictions;

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeEnsemble::transform(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const size_t _num_feature,
    containers::Optional<aggregations::AggregationImpl> *_impl ) const
{
    assert_true( _num_feature < trees().size() );

    assert_true(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert_true(
        _table_holder.main_tables_.size() == _table_holder.subtables_.size() );

    assert_true( _table_holder.main_tables_.size() == _subfeatures.size() );

    const auto ix = trees()[_num_feature].ix_perip_used();

    assert_true( ix < _table_holder.main_tables_.size() );

    auto aggregation =
        trees()[_num_feature].make_aggregation( enums::Mode::transform );

    aggregation->set_aggregation_impl( _impl );

    auto new_feature = trees()[_num_feature].transform(
        _table_holder.main_tables_[ix],
        _table_holder.peripheral_tables_[ix],
        _subfeatures[ix],
        hyperparameters().use_timestamps_,
        aggregation.get() );

    aggregation->reset();

    return new_feature;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel

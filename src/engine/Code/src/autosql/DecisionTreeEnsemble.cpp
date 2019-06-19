#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const std::vector<std::string>> &_categories,
    const std::shared_ptr<const descriptors::Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const decisiontrees::Placeholder> &_placeholder )
    : impl_( _categories, _hyperparameters, *_peripheral, *_placeholder )
{
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const std::vector<std::string>> &_categories,
    const Poco::JSON::Object &_json_obj )
    : impl_(
          _categories,
          std::make_shared<const descriptors::Hyperparameters>(
              *JSON::get_object( _json_obj, "hyperparameters_" ) ),
          JSON::array_to_vector<std::string>(
              JSON::get_array( _json_obj, "peripheral_" ) ),
          decisiontrees::Placeholder(
              *JSON::get_object( _json_obj, "population_" ) ) )
{
    *this = from_json_obj( _json_obj );
}

// ----------------------------------------------------------------------------

std::list<decisiontrees::DecisionTree> DecisionTreeEnsemble::build_candidates(
    const AUTOSQL_INT _ix_feature,
    const std::vector<descriptors::SameUnits> &_same_units,
    const decisiontrees::TableHolder &_table_holder )
{
    assert( random_number_generator() );

    return CandidateTreeBuilder::build_candidates(
        _table_holder,
        categories(),
        _same_units,
        _ix_feature,
        hyperparameters(),
        &aggregation_impl(),
        random_number_generator().get(),
        comm() );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrameView &_population_table )
{
    // -----------------------------------------------------
    // Sizes of arrays must match

    /*   auto expected_size = _population_table.num_join_keys();

       if ( expected_size == 0 )
           {
               throw std::invalid_argument(
                   "You must provide join keys for the population table!" );
           }

       assert( expected_size == _peripheral_tables.size() );

       assert( expected_size == num_columns_peripheral_categorical().size() );

       // -----------------------------------------------------
       // Number of columns must match

       if ( _population_table.num_categoricals() !=
            num_columns_population_categorical() )
           {
               throw std::invalid_argument(
                   "Wrong number of categorical columns in population table! "
                   "Expected " +
                   std::to_string( num_columns_population_categorical() ) +
                   ", got " +
                   std::to_string( _population_table.num_categoricals() ) + "!"
       );
           }

       if ( _population_table.num_discretes() !=
            num_columns_population_discrete() )
           {
               throw std::invalid_argument(
                   "Wrong number of discrete columns in population table! "
                   "Expected " +
                   std::to_string( num_columns_population_discrete() ) + ", got
       " + std::to_string( _population_table.num_discretes() ) + "!" );
           }

       if ( _population_table.num_numericals() !=
            num_columns_population_numerical() )
           {
               throw std::invalid_argument(
                   "Wrong number of numerical columns in population table! "
                   "Expected " +
                   std::to_string( num_columns_population_numerical() ) +
                   ", got " +
                   std::to_string( _population_table.num_numericals() ) + "!" );
           }

       for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
           {
               if ( _peripheral_tables[i].num_categoricals() !=
                    num_columns_peripheral_categorical()[i] )
                   {
                       throw std::invalid_argument(
                           "Wrong number of categorical columns in peripheral "
                           "table " +
                           std::to_string( i ) + "! Expected " +
                           std::to_string(
                               num_columns_peripheral_categorical()[i] ) +
                           ", got " +
                           std::to_string(
                               _peripheral_tables[i].num_categoricals() ) +
                           "!" );
                   }
           }

       for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
           {
               if ( _peripheral_tables[i].num_discretes() !=
                    num_columns_peripheral_discrete()[i] )
                   {
                       throw std::invalid_argument(
                           "Wrong number of discrete columns in peripheral "
                           "table " +
                           std::to_string( i ) + "! Expected " +
                           std::to_string( num_columns_peripheral_discrete()[i]
       ) +
                           ", got " +
                           std::to_string(
                               _peripheral_tables[i].num_discretes() ) +
                           "!" );
                   }
           }

       for ( size_t i = 0; i < _peripheral_tables.size(); ++i )
           {
               if ( _peripheral_tables[i].num_numericals() !=
                    num_columns_peripheral_numerical()[i] )
                   {
                       throw std::invalid_argument(
                           "Wrong number of numerical columns in peripheral "
                           "table " +
                           std::to_string( i ) + "! Expected " +
                           std::to_string(
                               num_columns_peripheral_numerical()[i] ) +
                           ", got " +
                           std::to_string(
                               _peripheral_tables[i].num_numericals() ) +
                           "!" );
                   }
           }*/

    // -----------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility_of_targets(
    const containers::DataFrameView &_population_table )
{
    if ( _population_table.num_targets() < 1 )
        {
            throw std::invalid_argument(
                "Targets must have at least one column!" );
        }

    for ( size_t j = 0; j < _population_table.num_targets(); ++j )
        {
            for ( size_t i = 0; i < _population_table.nrows(); ++i )
                {
                    if ( std::isnan( _population_table.target( i, j ) ) ||
                         std::isinf( _population_table.target( i, j ) ) )
                        {
                            throw std::invalid_argument(
                                "Target values can not be NULL or infinite!" );
                        }
                }
        }

    if ( hyperparameters().share_aggregations_ < 0.0 ||
         hyperparameters().share_aggregations_ > 1.0 )
        {
            throw std::invalid_argument(
                "share_aggregations must be between 0.0 and 1.0!" );
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    // ------------------------------------------------------

    debug_log( "Building communicator..." );

    const auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    multithreading::Communicator comm( num_threads );

    // ------------------------------------------------------
    // Build thread_nums

    debug_log( "Building the thread nums..." );

    const auto thread_nums = utils::DataFrameScatterer::build_thread_nums(
        _population.join_keys(), num_threads );

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

    assert( impl().hyperparameters_ );

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
                peripheral_names(),
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
                peripheral_names(),
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
                "Your population table needs to contain at least one row!" );
        }

    // ----------------------------------------------------------------
    // Store the column numbers, so we can make sure that the user
    // passes properly formatted data to predict(...)

    debug_log( "fit: Storing column numbers..." );

    assert( _table_holder->main_tables_.size() > 0 );

    // ----------------------------------------------------------------
    // Make sure that the data passed by the user is plausible

    debug_log( "fit: Checking plausibility of input..." );

    assert( _table_holder->main_tables_.size() > 0 );

    check_plausibility(
        _table_holder->peripheral_tables_, _table_holder->main_tables_[0] );

    check_plausibility_of_targets( _table_holder->main_tables_[0] );

    // ----------------------------------------------------------------
    // Store names of the targets

    targets() = {_table_holder->main_tables_[0].target_name( 0 )};

    // ----------------------------------------------------------------
    // If there are any subfeatures, fit them.

    fit_subfeatures( _table_holder, _logger, _opt, _comm );

    // ----------------------------------------------------------------
    // aggregations::AggregationImpl stores most of the data for the
    // aggregations. We do not want to reallocate the data all the time.

    assert( _table_holder->main_tables_.size() > 0 );

    aggregation_impl().reset( new aggregations::AggregationImpl(
        _table_holder->main_tables_[0].nrows() ) );

    // ----------------------------------------------------------------
    // Columns that share the same units are candidates for direct
    // comparison

    debug_log( "fit: Identifying same units..." );

    assert( _table_holder->main_tables_.size() > 0 );
    assert(
        _table_holder->main_tables_.size() ==
        _table_holder->peripheral_tables_.size() );

    auto same_units = SameUnitIdentifier::identify_same_units(
        _table_holder->peripheral_tables_,
        _table_holder->main_tables_[0].df() );

    // ----------------------------------------------------------------
    // Sample weights are needed for the random-forest-like functionality

    debug_log( "fit: Setting up sampling..." );

    if ( hyperparameters().seed_ < 0 )
        {
            throw std::invalid_argument( "Seed must be positive!" );
        }

    assert( _table_holder->main_tables_.size() > 0 );

    const auto nrows = _table_holder->main_tables_[0].nrows();

    auto sample_weights =
        std::make_shared<std::vector<AUTOSQL_FLOAT>>( nrows, 1.0 );

    random_number_generator().reset(
        new std::mt19937( static_cast<size_t>( hyperparameters().seed_ ) ) );

    // ----------------------------------------------------------------
    // Sample containers are pointers to simple structs, which represent a match
    // between a key in the peripheral table and a key in the population table.

    debug_log( "fit: Creating samples..." );

    const auto num_peripheral = _table_holder->peripheral_tables_.size();

    assert( _table_holder->main_tables_.size() == num_peripheral );

    std::vector<AUTOSQL_SAMPLES> samples( num_peripheral );

    std::vector<AUTOSQL_SAMPLE_CONTAINER> sample_containers( num_peripheral );

    if ( hyperparameters().sampling_factor_ <= 0.0 )
        {
            for ( size_t i = 0; i < num_peripheral; ++i )
                {
                    samples.push_back( utils::Matchmaker::make_matches(
                        _table_holder->main_tables_[i],
                        _table_holder->peripheral_tables_[i],
                        sample_weights,
                        hyperparameters().use_timestamps_ ) );

                    sample_containers.push_back(
                        utils::Matchmaker::make_pointers( &samples.back() ) );
                }
        }

    // ----------------------------------------------------------------

    for ( size_t ix_feature = 0; ix_feature < _num_features; ++ix_feature )
        {
            // ----------------------------------------------------------------
            // Sample for a random-forest-like algorithm - can be turned off
            // by setting _sampling_rate to 0.0

            debug_log( "fit: Sampling from population..." );

            if ( hyperparameters().sampling_factor_ > 0.0 )
                {
                    sample_weights = _opt->make_sample_weights();

                    assert( _table_holder->main_tables_.size() > 0 );

                    assert(
                        sample_weights->size() ==
                        _table_holder->main_tables_[0].nrows() );

                    for ( size_t i = 0; i < num_peripheral; ++i )
                        {
                            samples[i] = utils::Matchmaker::make_matches(
                                _table_holder->main_tables_[i],
                                _table_holder->peripheral_tables_[i],
                                sample_weights,
                                hyperparameters().use_timestamps_ );

                            sample_containers[i] =
                                utils::Matchmaker::make_pointers(
                                    &samples.back() );
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
                categories(),
                impl().hyperparameters_,
                random_number_generator().get(),
                comm() );

            tree_fitter.fit(
                *_table_holder,
                &samples,
                &sample_containers,
                _opt,
                &candidate_trees,
                &trees() );

            // -------------------------------------------------------------
            // Recalculate residuals, which is needed for the gradient boosting
            // algorithm.

            debug_log( "fit: Recalculating residuals..." );

            if ( hyperparameters().shrinkage_ > 0.0 )
                {
                    const auto ix = last_tree()->ix_perip_used();

                    std::vector<AUTOSQL_FLOAT> new_feature =
                        last_tree()->transform(
                            _table_holder->main_tables_[ix],
                            _table_holder->peripheral_tables_[ix],
                            _table_holder->subtables_[ix],
                            hyperparameters().use_timestamps_ );

                    _opt->update_yhat_old( *sample_weights, new_feature );

                    _opt->calc_residuals();
                }

            // -------------------------------------------------------------

            debug_log(
                "Trained FEATURE_" + std::to_string( ix_feature + 1 ) + "." );

            if ( _logger )
                {
                    _logger->log(
                        "Trained FEATURE_" + std::to_string( ix_feature + 1 ) +
                        "." );
                }

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

void DecisionTreeEnsemble::fit_subfeatures(
    const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    optimizationcriteria::OptimizationCriterion *_opt,
    multithreading::Communicator *_comm )
{
    // ----------------------------------------------------------------

    assert( _table_holder );

    assert(
        _table_holder->subtables_.size() ==
        _table_holder->main_tables_.size() );
    assert(
        _table_holder->subtables_.size() ==
        _table_holder->peripheral_tables_.size() );

    // ----------------------------------------------------------------
    // Set up the subfeatures.

    const auto num_tables = _table_holder->subtables_.size();

    auto subfeatures_avg =
        std::vector<containers::Optional<DecisionTreeEnsemble>>( num_tables );

    auto subfeatures_sum =
        std::vector<containers::Optional<DecisionTreeEnsemble>>( num_tables );

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( _table_holder->subtables_[i] )
                {
                    subfeatures_avg[i].reset(
                        new DecisionTreeEnsemble( *this ) );

                    subfeatures_sum[i].reset(
                        new DecisionTreeEnsemble( *this ) );
                }
        }

    // ----------------------------------------------------------------
    // If there are no subfeatures, we can stop here.

    const bool no_subfeatures = std::none_of(
        subfeatures_avg.cbegin(),
        subfeatures_avg.cend(),
        []( const containers::Optional<DecisionTreeEnsemble> &val ) {
            return val && true;
        } );

    if ( no_subfeatures )
        {
            subfeatures_avg_ = std::move( subfeatures_avg );
            subfeatures_sum_ = std::move( subfeatures_sum );

            return;
        }

    // ----------------------------------------------------------------
    // Create the output map (it says the same over all aggregations).

    const auto output_map = utils::Mapper::create_output_map(
        _table_holder->main_tables_[0].rows_ptr() );

    // ----------------------------------------------------------------
    // Fit the subfeatures_avg.

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( subfeatures_avg[i] )
                {
                    fit_subfeatures<aggregations::AggregationType::Avg>(
                        _table_holder,
                        _logger,
                        output_map,
                        i,
                        _opt,
                        _comm,
                        subfeatures_avg[i].get() );
                }
        }

    // ----------------------------------------------------------------
    // Fit the subfeatures_sum.

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( subfeatures_sum[i] )
                {
                    fit_subfeatures<aggregations::AggregationType::Sum>(
                        _table_holder,
                        _logger,
                        output_map,
                        i,
                        _opt,
                        _comm,
                        subfeatures_sum[i].get() );
                }
        }

    // ----------------------------------------------------------------
    // Store the subfeatures.

    subfeatures_avg_ = std::move( subfeatures_avg );

    subfeatures_sum_ = std::move( subfeatures_sum );

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

    model.peripheral_names() = JSON::array_to_vector<std::string>(
        JSON::get_array( _json_obj, "peripheral_" ) );

    model.impl().placeholder_population_.reset( new decisiontrees::Placeholder(
        *JSON::get_object( _json_obj, "population_" ) ) );

    // ----------------------------------------

    if ( _json_obj.has( "features_" ) )
        {
            // ----------------------------------------
            // Extract features.

            auto features = JSON::get_array( _json_obj, "features_" );

            for ( size_t i = 0; i < features->size(); ++i )
                {
                    auto obj =
                        features->getObject( static_cast<unsigned int>( i ) );

                    model.trees().push_back( decisiontrees::DecisionTree(
                        model.categories(),
                        model.hyperparameters().tree_hyperparameters_,
                        *obj ) );
                }

            // ----------------------------------------
            // Extract targets

            model.targets() = JSON::array_to_vector<std::string>(
                JSON::get_array( _json_obj, "targets_" ) );

            // ----------------------------------------
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
    assert( _index.size() == trees().size() );

    const auto num_selected_features =
        ( hyperparameters().num_selected_features_ > 0 &&
          hyperparameters().num_selected_features_ < _index.size() )
            ? ( hyperparameters().num_selected_features_ )
            : ( _index.size() );

    std::vector<decisiontrees::DecisionTree> selected_trees;

    for ( size_t i = 0; i < num_selected_features; ++i )
        {
            const auto ix = _index[i];

            assert( ix < trees().size() );

            selected_trees.push_back( trees()[ix] );
        }

    trees() = selected_trees;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_monitor(
    const std::string _name ) const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    obj.set( "name_", _name );

    // ----------------------------------------

    if ( has_been_fitted() )
        {
            // ----------------------------------------
            // Express model as JSON string
            {
                Poco::JSON::Object obj_json;

                // ----------------------------------------
                // Extract placeholders

                obj.set(
                    "peripheral_",
                    JSON::vector_to_array( peripheral_names() ) );

                obj.set( "population_", placeholder().to_json_obj() );

                // ----------------------------------------
                // Extract features

                Poco::JSON::Array features;

                for ( size_t i = 0; i < trees().size(); ++i )
                    {
                        features.add( trees()[i].to_monitor(
                            std::to_string( i + 1 ),
                            hyperparameters().use_timestamps_ ) );
                    }

                obj_json.set( "features_", features );

                // ----------------------------------------
                // Extract targets

                obj.set(
                    "targets_",
                    JSON::vector_to_array<std::string>( targets() ) );

                // ----------------------------------------
                // Add to obj

                obj.set( "json_", JSON::stringify( obj_json ) );

                // ----------------------------------------
            }

            // ----------------------------------------
            // Insert placeholders

            obj.set(
                "peripheral_", JSON::vector_to_array( peripheral_names() ) );

            obj.set( "population_", placeholder().to_json_obj() );

            // ----------------------------------------
            // Insert hyperparameters

            obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

            // ----------------------------------------
            // Insert sql

            std::vector<std::string> sql;

            for ( size_t i = 0; i < trees().size(); ++i )
                {
                    sql.push_back( trees()[i].to_sql(
                        std::to_string( i + 1 ),
                        hyperparameters().use_timestamps_ ) );
                }

            obj.set( "sql_", sql );

            // ----------------------------------------
            // Insert other pieces of information

            obj.set( "nfeatures_", trees().size() );

            // ----------------------------------------
        }

    return obj;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj() const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------
    // Extract placeholders

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    obj.set( "peripheral_", JSON::vector_to_array( peripheral_names() ) );

    obj.set( "population_", placeholder().to_json_obj() );

    // ----------------------------------------

    if ( has_been_fitted() )
        {
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

            obj.set(
                "targets_", JSON::vector_to_array<std::string>( targets() ) );

            // ----------------------------------------
        }

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::string DecisionTreeEnsemble::to_sql() const
{
    std::stringstream sql;

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            sql << trees()[i].to_sql(
                std::to_string( i + 1 ), hyperparameters().use_timestamps_ );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<AUTOSQL_FLOAT>> DecisionTreeEnsemble::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    // ------------------------------------------------------
    // Check plausibility.

    if ( num_features() == 0 )
        {
            throw std::runtime_error( "AutoSQL model has not been fitted!" );
        }

    // ------------------------------------------------------
    // thread_nums signify the thread number that a particular row belongs to.
    // The idea is to separate the join keys as clearly as possible.

    const auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    const auto thread_nums = utils::DataFrameScatterer::build_thread_nums(
        _population.join_keys(), num_threads );

    // -------------------------------------------------------
    // Launch threads and generate predictions on the subviews.

    auto features = std::make_shared<std::vector<AUTOSQL_FLOAT>>(
        _population.nrows() * num_features() );

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::transform_ensemble,
                i + 1,
                thread_nums,
                _population,
                _peripheral,
                std::shared_ptr<const logging::AbstractLogger>(),
                *this,
                features.get() ) );
        }

    // ------------------------------------------------------
    // Transform in main thread.

    try
        {
            Threadutils::transform_ensemble(
                0,
                thread_nums,
                _population,
                _peripheral,
                _logger,
                *this,
                features.get() );
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

std::vector<AUTOSQL_FLOAT> DecisionTreeEnsemble::transform(
    const decisiontrees::TableHolder &_table_holder,
    const size_t _num_feature,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    containers::Optional<aggregations::AggregationImpl> *_impl ) const
{
    assert( _num_feature < trees().size() );

    assert(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );

    assert(
        _table_holder.main_tables_.size() == _table_holder.subtables_.size() );

    const auto ix = trees()[_num_feature].ix_perip_used();

    assert( ix < _table_holder.main_tables_.size() );

    auto aggregation = trees()[_num_feature].make_aggregation();

    aggregation->set_aggregation_impl( _impl );

    auto new_feature = trees()[_num_feature].transform(
        _table_holder.main_tables_[ix],
        _table_holder.peripheral_tables_[ix],
        _table_holder.subtables_[ix],
        hyperparameters().use_timestamps_,
        aggregation.get() );

    aggregation->reset();

    return new_feature;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql

#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble() {}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<std::vector<std::string>> &_categories )
    : impl_( _categories )
{
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<std::vector<std::string>> &_categories,
    const std::vector<std::string> &_placeholder_peripheral,
    const decisiontrees::Placeholder &_placeholder_population )
    : impl_( _categories, _placeholder_peripheral, _placeholder_population )
{
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble( const DecisionTreeEnsemble &_other )
    : impl_( _other.impl() )
{
    debug_log( "Model: Copy constructor..." );

    if ( _other.loss_function_ )
        {
            assert( _other.loss_function_->type() != "" );

            loss_function(
                parse_loss_function( _other.loss_function_->type() ) );
        }
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    DecisionTreeEnsemble &&_other ) noexcept
    : impl_( std::move( _other.impl() ) ),
      loss_function_( std::move( _other.loss_function_ ) )
{
    debug_log( "Model: Move constructor..." );
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
        _same_units,
        _ix_feature,
        hyperparameters(),
        &aggregation_impl(),
        random_number_generator().get(),
        comm() );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::calculate_sampling_rate(
    const AUTOSQL_INT _population_nrows )
{
    AUTOSQL_FLOAT nrows = static_cast<AUTOSQL_FLOAT>( _population_nrows );

    utils::Reducer::reduce( std::plus<AUTOSQL_FLOAT>(), &nrows, comm() );

    if ( nrows <= 0.0 )
        {
            throw std::invalid_argument(
                "The population table needs to contain at least some data!" );
        }

    hyperparameters().sampling_rate_ = std::min(
        hyperparameters().sampling_factor_ * ( 2000.0 / nrows ), 1.0 );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrameView &_population_table )
{
    // -----------------------------------------------------
    // Sizes of arrays must match

    auto expected_size = _population_table.num_join_keys();

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
                std::to_string( _population_table.num_categoricals() ) + "!" );
        }

    if ( _population_table.num_discretes() !=
         num_columns_population_discrete() )
        {
            throw std::invalid_argument(
                "Wrong number of discrete columns in population table! "
                "Expected " +
                std::to_string( num_columns_population_discrete() ) + ", got " +
                std::to_string( _population_table.num_discretes() ) + "!" );
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
                        std::to_string( num_columns_peripheral_discrete()[i] ) +
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
        }

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

    if ( has_been_fitted() )
        {
            assert( linear_regressions().size() > 0 );
            assert( linear_regressions().size() == trees().size() );

            if ( linear_regressions()[0].size() !=
                 _population_table.num_targets() )
                {
                    throw std::invalid_argument(
                        "Number of targets cannot change throughout "
                        "different training episodes!" );
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    // ------------------------------------------------------

    const auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    multithreading::Communicator comm( num_threads );

    set_comm( &comm );

    // ------------------------------------------------------
    // Build thread_nums

    const auto thread_nums = utils::DataFrameScatterer::build_thread_nums(
        _population.join_keys(), num_threads );

    // ------------------------------------------------------
    // Create deep copies of this ensemble.

    std::vector<ensemble::DecisionTreeEnsemble> ensembles;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            ensembles.push_back( *this );
        }

    // ------------------------------------------------------
    // Spawn threads.

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::fit_ensemble,
                i + 1,
                thread_nums,
                _population,
                _peripheral,
                placeholder(),
                peripheral_names(),
                std::shared_ptr<const logging::AbstractLogger>(),
                &ensembles[i] ) );
        }

    // ------------------------------------------------------
    // Train ensemble in main thread.

    try
        {
            Threadutils::fit_ensemble(
                0,
                thread_nums,
                _population,
                _peripheral,
                placeholder(),
                peripheral_names(),
                _logger,
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

    for ( auto &thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const std::shared_ptr<const decisiontrees::TableHolder> &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    // ----------------------------------------------------------------

    debug_log( "fit: Beginning to fit features..." );

    // ----------------------------------------------------------------

    assert( _table_holder->main_tables_.size() > 0 );

    calculate_sampling_rate( _table_holder->main_tables_[0].nrows() );

    // ----------------------------------------------------------------
    // Store the column numbers, so we can make sure that the user
    // passes properly formatted data to predict(...)

    if ( has_been_fitted() == false )
        {
            debug_log( "fit: Storing column numbers..." );

            assert( _table_holder->main_tables_.size() > 0 );

            set_num_columns(
                _table_holder->peripheral_tables_,
                _table_holder->main_tables_[0] );
        }

    // ----------------------------------------------------------------
    // Make sure that the data passed by the user is plausible

    debug_log( "fit: Checking plausibility of input..." );

    assert( _table_holder->main_tables_.size() > 0 );

    check_plausibility(
        _table_holder->peripheral_tables_, _table_holder->main_tables_[0] );

    check_plausibility_of_targets( _table_holder->main_tables_[0] );

    // ----------------------------------------------------------------
    // Store names of the targets

    // targets() = _table_holder->main_tables_[0].targets().colnames();

    // ----------------------------------------------------------------
    // aggregations::AggregationImpl stores most of the data for the
    // aggregations. We do not want to reallocate the data all the time.

    assert( _table_holder->main_tables_.size() > 0 );

    aggregation_impl().reset( new aggregations::AggregationImpl(
        _table_holder->main_tables_[0].nrows() ) );

    if ( has_been_fitted() )
        {
            for ( auto &tree : trees() )
                {
                    tree.set_aggregation_impl( &aggregation_impl() );
                }
        }

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
    // Initialize the other objects we need

    loss_function( parse_loss_function( hyperparameters().loss_function_ ) );

    loss_function()->set_comm( comm() );

    optimizationcriteria::RSquaredCriterion opt( static_cast<AUTOSQL_FLOAT>(
        hyperparameters().tree_hyperparameters_->min_num_samples_ ) );

    // ----------------------------------------------------------------
    // Sample weights are needed for the random-forest-like functionality

    debug_log( "fit: Setting up sampling..." );

    if ( hyperparameters().seed_ < 0 )
        {
            throw std::invalid_argument( "Seed must be positive!" );
        }

    assert( _table_holder->main_tables_.size() > 0 );

    const auto sample_weights = std::make_shared<std::vector<AUTOSQL_FLOAT>>(
        _table_holder->main_tables_[0].nrows() );

    if ( !random_number_generator() )
        {
            random_number_generator().reset( new std::mt19937(
                static_cast<size_t>( hyperparameters().seed_ ) ) );
        }

    if ( hyperparameters().sampling_rate_ <= 0.0 )
        {
            std::fill( sample_weights->begin(), sample_weights->end(), 1.0 );
        }

    // ----------------------------------------------------------------
    // For the gradient-tree-boosting-like functionality, we intialize
    // yhat_old at 0.0. If this has already been fitted, we obviously
    // use the previous features.

    assert( _table_holder->main_tables_.size() > 0 );

    auto yhat_old = std::vector<std::vector<AUTOSQL_FLOAT>>(
        _table_holder->main_tables_[0].num_targets(),
        std::vector<AUTOSQL_FLOAT>( _table_holder->main_tables_[0].nrows() ) );

    /*if ( has_been_fitted() )
        {
            yhat_old = predict(
                table_holder->peripheral_tables_,
                table_holder->main_tables_[0] );
        }*/

    // ----------------------------------------------------------------
    // Calculate the pseudo-residuals - on which the tree will be
    // predicted

    assert( _table_holder->main_tables_.size() > 0 );

    auto residuals = loss_function()->calculate_residuals(
        yhat_old, _table_holder->main_tables_[0] );

    // ----------------------------------------------------------------
    // Sample containers are pointers to simple structs, which represent a match
    // between a key in the peripheral table and a key in the population table.

    debug_log( "fit: Creating samples..." );

    const auto num_peripheral = _table_holder->peripheral_tables_.size();

    assert( _table_holder->main_tables_.size() == num_peripheral );

    std::vector<AUTOSQL_SAMPLES> samples( num_peripheral );

    std::vector<AUTOSQL_SAMPLE_CONTAINER> sample_containers( num_peripheral );

    if ( hyperparameters().sampling_rate_ <= 0.0 )
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

    for ( size_t ix_feature = 0; ix_feature < hyperparameters().num_features_;
          ++ix_feature )
        {
            // ----------------------------------------------------------------
            // Sample for a random-forest-like algorithm - can be turned off
            // by setting _sampling_rate to 0.0

            debug_log( "fit: Sampling from population..." );

            if ( hyperparameters().sampling_rate_ > 0.0 )
                {
                    for ( size_t i = 0; i < num_peripheral; ++i )
                        {
                            samples.push_back( utils::Matchmaker::make_matches(
                                _table_holder->main_tables_[i],
                                _table_holder->peripheral_tables_[i],
                                sample_weights,
                                hyperparameters().use_timestamps_ ) );

                            sample_containers.push_back(
                                utils::Matchmaker::make_pointers(
                                    &samples.back() ) );
                        }
                }

            // ----------------------------------------------------------------
            // Reset the optimization criterion based on the residuals
            // generated
            // from the last prediction

            debug_log( "fit: Preparing optimization criterion..." );

            opt.set_comm( comm() );

            opt.init( residuals, *sample_weights );

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
                &opt,
                &candidate_trees,
                &trees() );

            // -------------------------------------------------------------
            // Recalculate residuals, which is needed for the gradient boosting
            // algorithm. If the shrinkage_ is 0.0, we still need a proper
            // utils::LinearRegression, which is why we use one with parameters
            // 0.0 to save time.

            debug_log( "fit: Recalculating residuals..." );

            if ( hyperparameters().shrinkage_ != 0.0 )
                {
                    fit_linear_regressions_and_recalculate_residuals(
                        *_table_holder,
                        hyperparameters().shrinkage_,
                        *sample_weights,
                        &yhat_old,
                        &residuals );
                }
            else
                {
                    assert( _table_holder->main_tables_.size() > 0 );

                    linear_regressions().push_back( utils::LinearRegression(
                        _table_holder->main_tables_[0].num_targets() ) );
                }

            // -------------------------------------------------------------

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

void DecisionTreeEnsemble::fit_linear_regressions_and_recalculate_residuals(
    const decisiontrees::TableHolder &_table_holder,
    const AUTOSQL_FLOAT _shrinkage,
    const std::vector<AUTOSQL_FLOAT> &_sample_weights,
    std::vector<std::vector<AUTOSQL_FLOAT>> *_yhat_old,
    std::vector<std::vector<AUTOSQL_FLOAT>> *_residuals )
{
    // ----------------------------------------------------------------

    const auto ix = last_tree()->ix_perip_used();

    assert( ix < _table_holder.main_tables_.size() );

    assert(
        _table_holder.main_tables_.size() ==
        _table_holder.peripheral_tables_.size() );
    assert(
        _table_holder.main_tables_.size() == _table_holder.subtables_.size() );

    // ----------------------------------------------------------------
    // Generate new feature

    std::vector<AUTOSQL_FLOAT> new_feature = last_tree()->transform(
        _table_holder.main_tables_[ix],
        _table_holder.peripheral_tables_[ix],
        _table_holder.subtables_[ix],
        hyperparameters().use_timestamps_ );

    // ----------------------------------------------------------------
    // Train a linear regression from the prediction of the last
    // tree on the residuals and generate predictions f_t on that basis

    linear_regressions().push_back( utils::LinearRegression() );

    last_linear_regression()->set_comm( comm() );

    last_linear_regression()->fit( new_feature, *_residuals, _sample_weights );

    const auto predictions = last_linear_regression()->predict( new_feature );

    last_linear_regression()->apply_shrinkage( _shrinkage );

    // ----------------------------------------------------------------
    // Find the optimal update_rates and update parameters of linear
    // regression accordingly

    auto update_rates = loss_function()->calculate_update_rates(
        *_yhat_old,
        predictions,
        _table_holder.main_tables_[ix],
        _sample_weights );

    // ----------------------------------------------------------------
    // Do the actual updates

    assert( update_rates.size() == predictions.size() );

    for ( size_t j = 0; j < predictions.size(); ++j )
        {
            for ( size_t i = 0; i < predictions[j].size(); ++i )
                {
                    const AUTOSQL_FLOAT update =
                        predictions[j][i] * update_rates[j] * _shrinkage;

                    ( *_yhat_old )[j][i] +=
                        ( ( std::isnan( update ) || std::isinf( update ) )
                              ? 0.0
                              : update );
                }
        }

    // ----------------------------------------------------------------
    // Recalculate the pseudo-residuals - on which the tree will
    // be predicted

    *_residuals = loss_function()->calculate_residuals(
        *_yhat_old, _table_holder.main_tables_[ix] );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::from_json_obj( const Poco::JSON::Object &_json_obj )
{
    // ----------------------------------------

    assert( categories() );

    DecisionTreeEnsemble model( categories() );

    // ----------------------------------------
    // Plausibility checks

    if ( JSON::get_array( _json_obj, "features_" )->size() == 0 )
        {
            std::invalid_argument(
                "JSON object does not contain any features!" );
        }

    if ( ( JSON::get_array( _json_obj, "update_rates1_" )->size() %
           JSON::get_array( _json_obj, "features_" )->size() ) != 0 )
        {
            std::invalid_argument(
                "Error in JSON: Number of elements in update_rates must be "
                "divisible by number of features!" );
        }

    if ( JSON::get_array( _json_obj, "update_rates1_" )->size() !=
         JSON::get_array( _json_obj, "update_rates2_" )->size() )
        {
            std::invalid_argument(
                "Error in JSON: Number of elements update_rates1_ must be "
                "equal to number of elements in update_rates2_!" );
        }

    // -------------------------------------------
    // Parse trees

    auto features = JSON::get_array( _json_obj, "features_" );

    for ( size_t i = 0; i < features->size(); ++i )
        {
            auto obj = features->getObject( static_cast<unsigned int>( i ) );

            model.trees().push_back( decisiontrees::DecisionTree( *obj ) );
        }

    // ----------------------------------------
    // Set categories for trees

    for ( auto &tree : model.trees() )
        {
            tree.set_categories( model.categories() );
        }

    // ----------------------------------------
    // Parse targets

    model.targets() = JSON::array_to_vector<std::string>(
        JSON::get_array( _json_obj, "targets_" ) );

    // ----------------------------------------
    // Parse parameters for linear regressions.

    model.parse_linear_regressions( _json_obj );

    // -------------------------------------------
    // Parse placeholders

    model.peripheral_names() = JSON::array_to_vector<std::string>(
        JSON::get_array( _json_obj, "peripheral_" ) );

    model.impl().placeholder_population_.reset( new decisiontrees::Placeholder(
        *JSON::get_object( _json_obj, "population_" ) ) );

    // ------------------------------------------
    // Parse num_columns

    model.num_columns_peripheral_categorical() =
        JSON::array_to_vector<AUTOSQL_INT>( JSON::get_array(
            _json_obj, "num_columns_peripheral_categorical_" ) );

    model.num_columns_peripheral_numerical() =
        JSON::array_to_vector<AUTOSQL_INT>(
            JSON::get_array( _json_obj, "num_columns_peripheral_numerical_" ) );

    model.num_columns_peripheral_discrete() =
        JSON::array_to_vector<AUTOSQL_INT>(
            JSON::get_array( _json_obj, "num_columns_peripheral_discrete_" ) );

    model.num_columns_population_categorical() = JSON::get_value<size_t>(
        _json_obj, "num_columns_population_categorical_" );

    model.num_columns_population_numerical() = JSON::get_value<size_t>(
        _json_obj, "num_columns_population_numerical_" );

    model.num_columns_population_discrete() = JSON::get_value<size_t>(
        _json_obj, "num_columns_population_discrete_" );

    // -------------------------------------------
    // Parse hyperparameters

    model.impl().hyperparameters_.reset( new descriptors::Hyperparameters(
        *JSON::get_object( _json_obj, "hyperparameters_" ) ) );

    // -------------------------------------------

    *this = std::move( model );

    // -------------------------------------------
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::load( const std::string &_path )
{
    std::stringstream json_stringstream;

    std::ifstream input( _path + "Model.json" );

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json_stringstream << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument(
                "File '" + _path + "Model.json' not found!" );
        }

    Poco::JSON::Parser parser;

    auto obj = parser.parse( json_stringstream.str() )
                   .extract<Poco::JSON::Object::Ptr>();

    from_json_obj( *obj );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    const DecisionTreeEnsemble &_other )
{
    debug_log( "Model: Copy assignment constructor..." );

    DecisionTreeEnsemble temp( _other );

    *this = std::move( temp );

    return *this;
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    DecisionTreeEnsemble &&_other ) noexcept
{
    debug_log( "Model: Move assignment constructor..." );

    if ( this == &_other )
        {
            return *this;
        }

    loss_function_ = std::move( _other.loss_function_ );

    impl_ = std::move( _other.impl_ );

    return *this;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::parse_linear_regressions(
    const Poco::JSON::Object &_json_obj )
{
    // -------------------------------------------------------------
    // Check input

    if ( JSON::get_array( _json_obj, "update_rates1_" )->size() !=
         JSON::get_array( _json_obj, "update_rates2_" )->size() )
        {
            throw std::runtime_error(
                "'update_rates1_' and 'update_rates2_' must have same "
                "length!" );
        }

    if ( JSON::get_array( _json_obj, "update_rates1_" )->size() == 0 )
        {
            throw std::runtime_error(
                "The must be at least some update rates!" );
        }

    const auto num_targets =
        JSON::get_array( _json_obj, "update_rates1_" )->getArray( 0 )->size();

    // -------------------------------------------------------------
    // Get slopes

    std::vector<std::vector<AUTOSQL_FLOAT>> slopes;

    {
        auto arr = JSON::get_array( _json_obj, "update_rates1_" );

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                if ( arr->getArray( static_cast<unsigned int>( i ) )->size() !=
                     num_targets )
                    {
                        throw std::runtime_error(
                            "All elements in 'update_rates1_' must have the "
                            "same length!" );
                    }

                slopes.push_back( JSON::array_to_vector<AUTOSQL_FLOAT>(
                    arr->getArray( static_cast<unsigned int>( i ) ) ) );
            }
    }

    // -------------------------------------------------------------
    // Get intercepts

    std::vector<std::vector<AUTOSQL_FLOAT>> intercepts;

    {
        auto arr = JSON::get_array( _json_obj, "update_rates2_" );

        for ( size_t i = 0; i < arr->size(); ++i )
            {
                if ( arr->getArray( static_cast<unsigned int>( i ) )->size() !=
                     num_targets )
                    {
                        throw std::runtime_error(
                            "All elements in 'update_rates2_' must have the "
                            "same length!" );
                    }

                intercepts.push_back( JSON::array_to_vector<AUTOSQL_FLOAT>(
                    arr->getArray( static_cast<unsigned int>( i ) ) ) );
            }
    }

    // -------------------------------------------------------------
    // Create linear regressions

    linear_regressions().clear();

    assert( false && "ToDo" );

    /* for ( size_t i = 0; i < slopes.size(); ++i )
         {
             std::vector<AUTOSQL_FLOAT> slopes_mat(
                 static_cast<AUTOSQL_INT>( 1 ),
                 static_cast<AUTOSQL_INT>( num_targets ) );

             std::copy( slopes[i].begin(), slopes[i].end(), slopes_mat.begin()
       );

             std::vector<AUTOSQL_FLOAT> intercepts_mat(
                 static_cast<AUTOSQL_INT>( 1 ),
                 static_cast<AUTOSQL_INT>( num_targets ) );

             std::copy(
                 intercepts[i].begin(),
                 intercepts[i].end(),
                 intercepts_mat.begin() );

             linear_regressions().push_back( utils::LinearRegression() );

             last_linear_regression()->set_slopes_and_intercepts(
                 slopes_mat, intercepts_mat );
         }*/

    // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

lossfunctions::LossFunction *DecisionTreeEnsemble::parse_loss_function(
    std::string _loss_function )
{
    if ( _loss_function == "CrossEntropyLoss" )
        {
            return new lossfunctions::CrossEntropyLoss();
        }
    else if ( _loss_function == "SquareLoss" )
        {
            return new lossfunctions::SquareLoss();
        }
    else
        {
            std::string warning_message = "Loss Function of type '";
            warning_message.append( _loss_function );
            warning_message.append( "' not known!" );

            throw std::invalid_argument( warning_message );
        }
}

// -------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_path )
{
    // If the path already exists, delete it to avoid
    // conflicts with already existing files.
    if ( Poco::File( _path ).exists() )
        {
            Poco::File( _path ).remove( true );
        }

    Poco::File( _path ).createDirectories();

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Unfitted models cannot be saved!" );
        }

    std::ofstream output( _path + "Model.json", std::ofstream::out );

    output << to_json();

    output.close();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::set_num_columns(
    const std::vector<containers::DataFrame> &_peripheral_tables,
    const containers::DataFrameView &_population_table )
{
    num_columns_peripheral_categorical().clear();

    for ( const auto &df : _peripheral_tables )
        {
            num_columns_peripheral_categorical().push_back(
                df.num_categoricals() );
        }

    num_columns_peripheral_discrete().clear();

    for ( const auto &df : _peripheral_tables )
        {
            num_columns_peripheral_discrete().push_back( df.num_discretes() );
        }

    num_columns_peripheral_numerical().clear();

    for ( const auto &df : _peripheral_tables )
        {
            num_columns_peripheral_numerical().push_back( df.num_numericals() );
        }

    num_columns_population_categorical() = _population_table.num_categoricals();

    num_columns_population_discrete() = _population_table.num_discretes();

    num_columns_population_numerical() = _population_table.num_numericals();
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

            for ( size_t i = 0; trees().size(); ++i )
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

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj()
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------
    // Extract placeholders

    obj.set( "peripheral_", JSON::vector_to_array( peripheral_names() ) );

    obj.set( "population_", placeholder().to_json_obj() );

    // ----------------------------------------

    if ( has_been_fitted() )
        {
            // ----------------------------------------
            // Extract expected number of columns

            obj.set(
                "num_columns_peripheral_categorical_",
                num_columns_peripheral_categorical() );

            obj.set(
                "num_columns_peripheral_numerical_",
                num_columns_peripheral_numerical() );

            obj.set(
                "num_columns_peripheral_discrete_",
                num_columns_peripheral_discrete() );

            obj.set(
                "num_columns_population_categorical_",
                num_columns_population_categorical() );

            obj.set(
                "num_columns_population_numerical_",
                num_columns_population_numerical() );

            obj.set(
                "num_columns_population_discrete_",
                num_columns_population_discrete() );

            // ----------------------------------------
            // Extract features

            {
                Poco::JSON::Array features;

                for ( auto &tree : trees() )
                    {
                        features.add( tree.to_json_obj() );
                    }

                obj.set( "features_", features );
            }

            // ----------------------------------------
            // Extract targets

            obj.set(
                "targets_", JSON::vector_to_array<std::string>( targets() ) );

            // ----------------------------------------
            // Extract linear regression

            assert( false && "ToDo" );

            // ----------------------------------------
            // Extract hyperparameters

            obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

            // ----------------------------------------
        }

    return obj;
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

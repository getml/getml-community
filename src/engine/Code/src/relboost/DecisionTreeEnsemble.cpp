#include "relboost/ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const std::vector<std::string>> &_encoding,
    const std::shared_ptr<const Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const Placeholder> &_placeholder )
    : impl_( DecisionTreeEnsembleImpl(
          _encoding, _hyperparameters, _peripheral, _placeholder ) ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _hyperparameters->loss_function_, impl().hyperparameters_, targets_ );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const std::vector<std::string>> &_encoding,
    const Poco::JSON::Object &_obj )
    : impl_( DecisionTreeEnsembleImpl(
          _encoding,
          std::make_shared<const Hyperparameters>(
              *JSON::get_object( _obj, "hyperparameters_" ) ),
          std::make_shared<const std::vector<std::string>>(
              JSON::array_to_vector<std::string>(
                  JSON::get_array( _obj, "peripheral_" ) ) ),
          std::make_shared<const Placeholder>(
              *JSON::get_object( _obj, "placeholder_" ) ) ) ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )
{
    // ------------------------------------------------------------------------

    initial_prediction() =
        JSON::get_value<Float>( _obj, "initial_prediction_" );

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        hyperparameters().loss_function_, impl().hyperparameters_, targets_ );

    // ------------------------------------------------------------------------

    impl().population_schema_.reset( new containers::Schema(
        *JSON::get_object( _obj, "population_schema_" ) ) );

    // ------------------------------------------------------------------------

    std::vector<containers::Schema> peripheral;

    const auto peripheral_arr = *JSON::get_array( _obj, "peripheral_schema_" );

    for ( size_t i = 0; i < peripheral_arr.size(); ++i )
        {
            peripheral.push_back( containers::Schema(
                *peripheral_arr.getObject( static_cast<unsigned int>( i ) ) ) );
        }

    impl().peripheral_schema_.reset(
        new std::vector<containers::Schema>( peripheral ) );

    // ------------------------------------------------------------------------

    const auto trees_objects = *JSON::get_array( _obj, "trees_" );

    for ( size_t i = 0; i < trees_objects.size(); ++i )
        {
            trees().push_back( decisiontrees::DecisionTree(
                _encoding,
                impl().hyperparameters_,
                loss_function_,
                *trees_objects.getObject( static_cast<unsigned int>( i ) ) ) );
        }

    // ----------------------------------------
    // Extract subensembles_avg_

    auto features_avg = JSON::get_array( _obj, "subfeatures1_" );

    subensembles_avg_ = std::vector<std::optional<DecisionTreeEnsemble>>(
        features_avg->size() );

    for ( size_t i = 0; i < features_avg->size(); ++i )
        {
            auto obj =
                features_avg->getObject( static_cast<unsigned int>( i ) );

            if ( obj )
                {
                    subensembles_avg_[i] =
                        std::make_optional<DecisionTreeEnsemble>(
                            encoding(), *obj );
                }
        }

    // ----------------------------------------
    // Extract subensembles_sum_

    auto features_sum = JSON::get_array( _obj, "subfeatures2_" );

    subensembles_sum_ = std::vector<std::optional<DecisionTreeEnsemble>>(
        features_sum->size() );

    for ( size_t i = 0; i < features_sum->size(); ++i )
        {
            auto obj =
                features_sum->getObject( static_cast<unsigned int>( i ) );

            if ( obj )
                {
                    subensembles_sum_[i] =
                        std::make_optional<DecisionTreeEnsemble>(
                            encoding(), *obj );
                }
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble( const DecisionTreeEnsemble &_other )
    : impl_( _other.impl() ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )

{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );

    subensembles_avg_ = _other.subensembles_avg_;

    subensembles_sum_ = _other.subensembles_sum_;

    set_comm( impl_.comm_ );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    DecisionTreeEnsemble &&_other ) noexcept
    : impl_( std::move( _other.impl() ) ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );

    subensembles_avg_ = std::move( _other.subensembles_avg_ );

    subensembles_sum_ = std::move( _other.subensembles_sum_ );

    set_comm( impl_.comm_ );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::calc_initial_prediction()
{
    auto sum = std::accumulate(
        targets().begin(), targets().end(), 0.0, std::plus<Float>() );

    auto count = static_cast<Float>( targets().size() );

    utils::Reducer::reduce( std::plus<Float>(), &sum, &comm() );

    utils::Reducer::reduce( std::plus<Float>(), &count, &comm() );

    initial_prediction() = sum / count;

    loss_function().apply_inverse( &initial_prediction() );
}

// ----------------------------------------------------------------------------

Float DecisionTreeEnsemble::calc_loss_reduction(
    const decisiontrees::DecisionTree &_decision_tree,
    const std::vector<Float> &_predictions ) const
{
    assert_true( _predictions.size() == targets().size() );

    return loss_function_->evaluate_tree(
        _decision_tree.update_rate(), _predictions );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::check_plausibility_of_targets(
    const containers::DataFrame &_population_table )
{
    if ( _population_table.num_targets() < 1 )
        {
            throw std::invalid_argument(
                "The population table must have at least one target column!" );
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
                                        "Target values for a classification "
                                        "problem have to be 0.0 or 1.0!" );
                                }
                        }
                }
        }
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::clean_up()
{
    *this = DecisionTreeEnsemble( impl().encoding_, to_json_obj() );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::extract_schemas(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    impl().population_schema_.reset(
        new containers::Schema( _population.to_schema() ) );

    std::vector<containers::Schema> peripheral;

    for ( auto &df : _peripheral )
        {
            peripheral.push_back( df.to_schema() );
        }

    impl().peripheral_schema_.reset(
        new std::vector<containers::Schema>( peripheral ) );
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
                "Relboost model has already been fitted!" );
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

    auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    // ------------------------------------------------------
    // Build thread_nums

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

    set_comm( &comm );

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
                std::shared_ptr<const logging::AbstractLogger>(),
                &ensembles[i] ) );
        }

    // ------------------------------------------------------
    // Train ensemble in main thread.

    try
        {
            Threadutils::fit_ensemble(
                0, thread_nums, _population, _peripheral, _logger, this );
        }
    catch ( std::exception &e )
        {
            for ( auto &thr : threads )
                {
                    thr.join();
                }

            throw std::invalid_argument( e.what() );
        }

    // ------------------------------------------------------
    // Join all other threads

    for ( auto &thr : threads )
        {
            thr.join();
        }

    // ------------------------------------------------------

    clean_up();

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_new_feature(
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function,
    const std::shared_ptr<const TableHolder> &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures )
{
    // ------------------------------------------------------------------------

    assert_true( _loss_function );

    assert_true( _table_holder );

    assert_true(
        _table_holder->main_tables_.size() ==
        _table_holder->peripheral_tables_.size() );

    assert_true( _table_holder->main_tables_.size() == _subfeatures.size() );

    assert_true( _table_holder->main_tables_.size() > 0 );

    // ------------------------------------------------------------------------
    // Recreate sample weights.

    const auto sample_weights = _loss_function->make_sample_weights();

    _loss_function->calc_sums();

    _loss_function->commit();

    // ------------------------------------------------------------------------

    std::vector<decisiontrees::DecisionTree> candidates;

    std::vector<Float> loss;

    std::vector<std::vector<Float>> predictions;

    for ( size_t ix_table_used = 0;
          ix_table_used < _table_holder->main_tables_.size();
          ++ix_table_used )
        {
            // ------------------------------------------------------------------------

            const auto &output_table =
                _table_holder->main_tables_[ix_table_used];

            const auto &input_table =
                _table_holder->peripheral_tables_[ix_table_used];

            const auto &subfeatures = _subfeatures[ix_table_used];

            // ------------------------------------------------------------------------

            assert_true( output_table.nrows() == sample_weights->size() );

            // ------------------------------------------------------------------------
            // Matches can potentially use a lot of memory - better to create
            // them anew when needed.

            const auto matches = utils::Matchmaker::make_matches(
                output_table,
                input_table,
                sample_weights,
                hyperparameters().use_timestamps_ );

            auto matches_ptr = utils::Matchmaker::make_pointers( matches );

            assert_true( matches.size() == matches_ptr.size() );

            debug_log(
                "Number of matches: " + std::to_string( matches.size() ) );

            // ------------------------------------------------------------------------
            // Build aggregations.

            std::vector<std::shared_ptr<lossfunctions::LossFunction>>
                aggregations;

            aggregations.push_back( std::make_shared<aggregations::Avg>(
                _loss_function,
                matches_ptr,
                input_table,
                output_table,
                &comm() ) );

            aggregations.push_back( std::make_shared<aggregations::Sum>(
                _loss_function, input_table, output_table, &comm() ) );

            // ------------------------------------------------------------------------
            // Iterate through aggregations.

            for ( auto &agg : aggregations )
                {
                    candidates.push_back( decisiontrees::DecisionTree(
                        impl().encoding_,
                        impl().hyperparameters_,
                        agg,
                        ix_table_used,
                        &comm() ) );

                    candidates.back().fit(
                        output_table,
                        input_table,
                        subfeatures,
                        matches_ptr.begin(),
                        matches_ptr.end() );

                    const auto new_predictions = candidates.back().transform(
                        output_table, input_table, subfeatures );

                    candidates.back().calc_update_rate( new_predictions );

                    auto loss_reduction = calc_loss_reduction(
                        candidates.back(), new_predictions );

                    loss.push_back( loss_reduction );

                    predictions.emplace_back( std::move( new_predictions ) );
                }

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
    // Find best candidate

    assert_true( loss.size() == candidates.size() );
    assert_true( predictions.size() == candidates.size() );

    const auto it = std::min_element( loss.begin(), loss.end() );

    const auto dist = std::distance( loss.begin(), it );

    const auto &best_predictions = predictions[dist];

    // ------------------------------------------------------------------------
    // Update yhat_old_.

    _loss_function->update_yhat_old(
        candidates[dist].update_rate() * hyperparameters().eta_,
        best_predictions );

    _loss_function->calc_gradients();

    _loss_function->commit();

    // ------------------------------------------------------------------------
    // Add best candidate to trees

    trees().emplace_back( std::move( candidates[dist] ) );

    // ------------------------------------------------------------------------
    // Get rid of data no longer needed.

    trees().back().clear();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_subensembles(
    const std::shared_ptr<const TableHolder> &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function )
{
    assert_true( _table_holder );

    assert_true( _loss_function );

    SubtreeHelper::fit_subensembles(
        _table_holder,
        _logger,
        *this,
        _loss_function,
        &comm(),
        &subensembles_avg_,
        &subensembles_sum_ );
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeEnsemble::generate_predictions(
    const decisiontrees::DecisionTree &_decision_tree,
    const TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures ) const
{
    const auto peripheral_used = _decision_tree.peripheral_used();

    assert_true( peripheral_used < _table_holder.main_tables_.size() );

    assert_true( peripheral_used < _table_holder.peripheral_tables_.size() );

    assert_true( peripheral_used < _subfeatures.size() );

    return _decision_tree.transform(
        _table_holder.main_tables_[peripheral_used],
        _table_holder.peripheral_tables_[peripheral_used],
        _subfeatures[peripheral_used] );
}

// ----------------------------------------------------------------------------

std::pair<
    std::shared_ptr<lossfunctions::LossFunction>,
    std::shared_ptr<const TableHolder>>
DecisionTreeEnsemble::init(
    const containers::DataFrameView &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    // ------------------------------------------------------------------------
    // Prepare targets.

    if ( _population.num_targets() != 1 )
        {
            throw std::runtime_error(
                "The population table needs to define exactly one target!" );
        }

    targets().resize( _population.nrows() );

    for ( size_t i = 0; i < _population.nrows(); ++i )
        {
            targets()[i] = _population.target( i, 0 );
        }

    // ------------------------------------------------------------------------

    calc_initial_prediction();

    // ------------------------------------------------------------------------
    // Initialize the loss function.

    loss_function().init_yhat_old( initial_prediction() );

    loss_function().calc_gradients();

    loss_function().calc_sampling_rate(
        hyperparameters().seed_, hyperparameters().sampling_factor_, &comm() );

    // ------------------------------------------------------------------------
    // Build table holder.

    const auto table_holder = std::make_shared<const TableHolder>(
        placeholder(), _population, _peripheral, peripheral_names() );

    // ------------------------------------------------------------------------

    return std::make_pair( loss_function_, table_holder );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    const DecisionTreeEnsemble &_other )
{
    debug_log( "DecisionTreeEnsemble: Copy assignment constructor..." );

    DecisionTreeEnsemble temp( _other );

    *this = std::move( temp );

    set_comm( impl_.comm_ );

    return *this;
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    DecisionTreeEnsemble &&_other ) noexcept
{
    debug_log( "DecisionTreeEnsemble: Move assignment constructor..." );

    if ( this == &_other )
        {
            return *this;
        }

    impl_ = std::move( _other.impl_ );

    targets_ = std::make_shared<std::vector<Float>>( 0 );

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );

    subensembles_avg_ = std::move( _other.subensembles_avg_ );

    subensembles_sum_ = std::move( _other.subensembles_sum_ );

    set_comm( impl_.comm_ );

    return *this;
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeEnsemble::predict(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    const auto features = transform( _population, _peripheral );

    assert_true( features.size() == num_features() );

    auto predictions =
        std::vector<Float>( _population.nrows(), initial_prediction() );

    for ( size_t j = 0; j < num_features(); ++j )
        {
            assert_true( features[j]->size() == _population.nrows() );

            for ( size_t i = 0; i < _population.nrows(); ++i )
                {
                    predictions[i] += ( *features[j] )[i] *
                                      hyperparameters().eta_ *
                                      trees()[j].update_rate();
                }
        }

    return predictions;
}

// ----------------------------------------------------------------------------

std::pair<
    std::vector<containers::Predictions>,
    std::vector<containers::Subfeatures>>
DecisionTreeEnsemble::prepare_subfeatures(
    const TableHolder &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    auto subpredictions = SubtreeHelper::make_predictions(
        _table_holder, subensembles_avg_, subensembles_sum_ );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( _table_holder, subpredictions );

    return std::make_pair( subpredictions, subfeatures );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json_obj(), fs );

    fs.close();
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

Poco::JSON::Object DecisionTreeEnsemble::to_monitor(
    const std::string _name ) const
{
    // ----------------------------------------

    Poco::JSON::Object obj;

    obj.set( "name_", _name );

    // ----------------------------------------

    if ( trees().size() > 0 )
        {
            // ----------------------------------------
            // Insert placeholders

            obj.set(
                "peripheral_",
                JSON::vector_to_array_ptr( peripheral_names() ) );

            obj.set( "population_", placeholder().to_json_obj() );

            // ----------------------------------------
            // Insert schema

            Poco::JSON::Array::Ptr peripheral_schema_arr(
                new Poco::JSON::Array() );

            for ( size_t i = 0; i < peripheral_schema().size(); ++i )
                {
                    peripheral_schema_arr->add(
                        peripheral_schema()[i].to_json_obj() );
                }

            obj.set( "peripheral_schema_", peripheral_schema_arr );

            obj.set( "population_schema_", population_schema().to_json_obj() );

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

            obj.set(
                "targets_",
                JSON::vector_to_array<std::string>(
                    population_schema().targets() ) );

            // ----------------------------------------
        }

    return obj;
}

// ----------------------------------------------------------------------------

containers::Features DecisionTreeEnsemble::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::shared_ptr<const logging::AbstractLogger> _logger ) const
{
    // ------------------------------------------------------
    // Check plausibility.

    if ( num_features() == 0 )
        {
            throw std::runtime_error( "Relboost model has not been fitted!" );
        }

    if ( _population.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    // ------------------------------------------------------
    // thread_nums signify the thread number that a particular row belongs to.
    // The idea is to separate the join keys as clearly as possible.

    auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

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
    // Launch threads and generate predictions on the subviews.

    auto features = containers::Features( num_features() );

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
                _population,
                _peripheral,
                std::shared_ptr<const logging::AbstractLogger>(),
                *this,
                &features ) );
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
                &features );
        }
    catch ( std::exception &e )
        {
            for ( auto &thr : threads )
                {
                    thr.join();
                }

            throw std::invalid_argument( e.what() );
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

std::vector<Float> DecisionTreeEnsemble::transform(
    const TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    size_t _n_feature ) const
{
    assert_true( _n_feature < num_features() );

    return generate_predictions(
        trees()[_n_feature], _table_holder, _subfeatures );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj(
    const bool _schema_only ) const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    // ------------------------------------------------------------------------

    obj.set( "peripheral_", JSON::vector_to_array_ptr( peripheral_names() ) );

    obj.set( "placeholder_", placeholder().to_json_obj() );

    // ------------------------------------------------------------------------

    // Subensembles in a snowflake model do not have schemata.
    if ( impl().population_schema_ )
        {
            Poco::JSON::Array::Ptr peripheral_schema_arr(
                new Poco::JSON::Array() );

            for ( auto &p : peripheral_schema() )
                {
                    peripheral_schema_arr->add( p.to_json_obj() );
                }

            obj.set( "peripheral_schema_", peripheral_schema_arr );

            obj.set( "population_schema_", population_schema().to_json_obj() );
        }

    // ----------------------------------------

    if ( _schema_only )
        {
            return obj;
        }

    // ------------------------------------------------------------------------

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Model has not been fitted!" );
        }

    // ------------------------------------------------------------------------

    obj.set( "initial_prediction_", initial_prediction() );

    // ------------------------------------------------------------------------

    if ( trees().size() == 0 )
        {
            throw std::runtime_error( "Model has not been fitted!" );
        }

    Poco::JSON::Array::Ptr trees_arr( new Poco::JSON::Array() );

    for ( auto &tree : trees() )
        {
            trees_arr->add( tree.to_json_obj() );
        }

    obj.set( "trees_", trees_arr );

    // ----------------------------------------
    // Extract subensembles_avg_

    Poco::JSON::Array::Ptr features_avg( new Poco::JSON::Array() );

    for ( const auto &sub : subensembles_avg_ )
        {
            if ( sub )
                {
                    features_avg->add( sub->to_json_obj() );
                }
            else
                {
                    features_avg->add( Poco::Dynamic::Var() );
                }
        }

    obj.set( "subfeatures1_", features_avg );

    // ----------------------------------------
    // Extract subensembles_sum_

    Poco::JSON::Array::Ptr features_sum( new Poco::JSON::Array() );

    for ( const auto &sub : subensembles_sum_ )
        {
            if ( sub )
                {
                    features_sum->add( sub->to_json_obj() );
                }
            else
                {
                    features_sum->add( Poco::Dynamic::Var() );
                }
        }

    obj.set( "subfeatures2_", features_sum );

    // ------------------------------------------------------------------------

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
}  // namespace ensemble
}  // namespace relboost

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
      targets_( std::make_shared<std::vector<RELBOOST_FLOAT>>( 0 ) )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _hyperparameters->objective_, impl().hyperparameters_, targets_ );
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
                  JSON::get_array( _obj, "peripheral_names_" ) ) ),
          std::make_shared<const Placeholder>(
              *JSON::get_object( _obj, "placeholder_" ) ) ) ),
      targets_( std::make_shared<std::vector<RELBOOST_FLOAT>>( 0 ) )
{
    initial_prediction() =
        JSON::get_value<RELBOOST_FLOAT>( _obj, "initial_prediction_" );

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        hyperparameters().objective_, impl().hyperparameters_, targets_ );

    const auto trees_objects = *JSON::get_array( _obj, "trees_" );

    for ( size_t i = 0; i < trees_objects.size(); ++i )
        {
            trees().push_back( decisiontrees::DecisionTree(
                _encoding,
                impl().hyperparameters_,
                loss_function_,
                *trees_objects.getObject( i ) ) );
        }
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble( const DecisionTreeEnsemble &_other )
    : impl_( _other.impl() ),
      targets_( std::make_shared<std::vector<RELBOOST_FLOAT>>( 0 ) )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    DecisionTreeEnsemble &&_other ) noexcept
    : impl_( std::move( _other.impl() ) ), targets_( _other.targets_ )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );
}

// ----------------------------------------------------------------------------

RELBOOST_FLOAT DecisionTreeEnsemble::calc_loss_reduction(
    const decisiontrees::DecisionTree &_decision_tree,
    const std::vector<RELBOOST_FLOAT> &_yhat_old,
    const std::vector<RELBOOST_FLOAT> &_predictions ) const
{
    assert( _yhat_old.size() == targets().size() );
    assert( _predictions.size() == targets().size() );

    std::vector<RELBOOST_FLOAT> yhat_new( _yhat_old.size() );

    const auto update_rate = _decision_tree.update_rate();

    const auto update_function = [update_rate](
                                     const RELBOOST_FLOAT y_old,
                                     const RELBOOST_FLOAT y_new ) {
        return y_old + y_new * update_rate;
    };

    std::transform(
        _yhat_old.begin(),
        _yhat_old.end(),
        _predictions.begin(),
        yhat_new.begin(),
        update_function );

    return loss_function_->evaluate_tree( yhat_new );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::clean_up()
{
    table_holder_.reset();
    yhat_old_.reset();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    // ------------------------------------------------------

    // multithreading::Communicator comm( _num_threads );

    // ------------------------------------------------------
    // Build thread_nums

    const auto num_threads = 1;
    // get_num_threads( _ensemble->hyperparameters().num_threads_ );

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
                this ) );
        }

    // ------------------------------------------------------
    // Train ensemble in main thread.

    try
        {
            Threadutils::fit_ensemble(
                0, thread_nums, _population, _peripheral, this );
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
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_new_feature()
{
    // ------------------------------------------------------------------------

    assert(
        table_holder().main_tables_.size() ==
        table_holder().peripheral_tables_.size() );

    assert( table_holder().main_tables_.size() > 0 );

    // ------------------------------------------------------------------------
    // Create new sample weights.

    const auto sample_weights =
        sampler().make_sample_weights( table_holder().main_tables_[0].nrows() );

    loss_function().calc_sample_index( sample_weights );

    loss_function().calc_sums();

    loss_function().commit();

    // ------------------------------------------------------------------------

    std::vector<decisiontrees::DecisionTree> candidates;

    std::vector<RELBOOST_FLOAT> loss;

    std::vector<std::vector<RELBOOST_FLOAT>> predictions;

    for ( size_t ix_table_used = 0;
          ix_table_used < table_holder().main_tables_.size();
          ++ix_table_used )
        {
            // ------------------------------------------------------------------------

            const auto &output_table =
                table_holder().main_tables_[ix_table_used];

            const auto &input_table =
                table_holder().peripheral_tables_[ix_table_used];

            // ------------------------------------------------------------------------

            assert( output_table.nrows() == sample_weights->size() );

            // ------------------------------------------------------------------------
            // Matches can potentially use a lot of memory - better to create
            // them anew when needed.

            const auto matches = utils::Matchmaker::make_matches(
                output_table,
                input_table,
                sample_weights,
                hyperparameters().use_timestamps_ );

            auto matches_ptr = utils::Matchmaker::make_pointers( matches );

            assert( matches.size() == matches_ptr.size() );

            debug_log(
                "Number of matches: " + std::to_string( matches.size() ) );

            // ------------------------------------------------------------------------
            // Build aggregations.

            std::vector<std::shared_ptr<lossfunctions::LossFunction>>
                aggregations;

            aggregations.push_back( std::make_shared<aggregations::Avg>(
                loss_function_, matches_ptr, input_table, output_table ) );

            aggregations.push_back( std::make_shared<aggregations::Sum>(
                loss_function_, input_table, output_table ) );

            // ------------------------------------------------------------------------
            // Iterate through aggregations.

            for ( auto &agg : aggregations )
                {
                    candidates.push_back( decisiontrees::DecisionTree(
                        impl().encoding_,
                        impl().hyperparameters_,
                        agg,
                        ix_table_used ) );

                    candidates.back().fit(
                        output_table,
                        input_table,
                        matches_ptr.begin(),
                        matches_ptr.end() );

                    const auto new_predictions = candidates.back().transform(
                        output_table, input_table );

                    candidates.back().calc_update_rate(
                        yhat_old(), new_predictions );

                    auto loss_reduction = calc_loss_reduction(
                        candidates.back(), yhat_old(), new_predictions );

                    loss.push_back( loss_reduction );

                    predictions.emplace_back( std::move( new_predictions ) );
                }

            // ------------------------------------------------------------------------
        }

    // ------------------------------------------------------------------------
    // Find best candidate

    assert( loss.size() == candidates.size() );
    assert( predictions.size() == candidates.size() );

    const auto it = std::min_element( loss.begin(), loss.end() );

    const auto dist = std::distance( loss.begin(), it );

    const auto &best_predictions = predictions[dist];

    // ------------------------------------------------------------------------
    // Update yhat_old_.

    assert( yhat_old_ );

    update_predictions(
        candidates[dist].update_rate(), best_predictions, &yhat_old() );

    loss_function().calc_gradients( yhat_old_ );

    loss_function().commit();

    // ------------------------------------------------------------------------
    // Add best candidate to trees

    trees().push_back( std::move( candidates[dist] ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::generate_predictions(
    const decisiontrees::DecisionTree &_decision_tree,
    const TableHolder &_table_holder ) const
{
    const auto peripheral_used = _decision_tree.peripheral_used();

    return _decision_tree.transform(
        _table_holder.main_tables_[peripheral_used],
        _table_holder.peripheral_tables_[peripheral_used] );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::init(
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
    // Calculate initial prediction.

    initial_prediction() = std::accumulate(
        targets().begin(), targets().end(), 0.0, std::plus<RELBOOST_FLOAT>() );

    initial_prediction() /= static_cast<RELBOOST_FLOAT>( targets().size() );

    loss_function().apply_inverse( &initial_prediction() );

    // ------------------------------------------------------------------------
    // Calculate gradient.

    yhat_old_ = std::make_shared<std::vector<RELBOOST_FLOAT>>(
        _population.nrows(), initial_prediction() );

    loss_function().calc_gradients( yhat_old_ );

    // ------------------------------------------------------------------------
    // Build table holder.

    table_holder_ = std::make_shared<const TableHolder>(
        placeholder(), _population, _peripheral, peripheral_names() );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble &DecisionTreeEnsemble::operator=(
    const DecisionTreeEnsemble &_other )
{
    debug_log( "DecisionTreeEnsemble: Copy assignment constructor..." );

    DecisionTreeEnsemble temp( _other );

    *this = std::move( temp );

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

    targets_ = _other.targets_;

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _other.loss_function().type(), impl().hyperparameters_, targets_ );

    return *this;
}

// ----------------------------------------------------------------------------

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::predict(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    const auto features = transform( _population, _peripheral );

    assert( features->size() == _population.nrows() * num_features() );

    auto predictions = std::vector<RELBOOST_FLOAT>(
        _population.nrows(), initial_prediction() );

    for ( size_t i = 0; i < _population.nrows(); ++i )
        {
            for ( size_t j = 0; j < num_features(); ++j )
                {
                    predictions[i] += ( *features )[i * num_features() + j] *
                                      hyperparameters().eta_ *
                                      trees()[j].update_rate();
                }
        }

    return predictions;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::save( const std::string &_fname ) const
{
    std::ofstream fs( _fname, std::ofstream::out );

    Poco::JSON::Stringifier::stringify( to_json_obj(), fs );

    fs.close();
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::score(
    const METRICS_FLOAT *const _yhat,
    const size_t _yhat_nrows,
    const size_t _yhat_ncols,
    const METRICS_FLOAT *const _y,
    const size_t _y_nrows,
    const size_t _y_ncols )
{
    // ------------------------------------------------------------------------
    // Build up names.

    std::vector<std::string> names;

    if ( loss_function().type() == "CrossEntropyLoss" )
        {
            names.push_back( "accuracy_" );
            names.push_back( "auc_" );
            names.push_back( "cross_entropy_" );
        }
    else if ( loss_function().type() == "SquareLoss" )
        {
            names.push_back( "mae_" );
            names.push_back( "rmse_" );
            names.push_back( "rsquared_" );
        }
    else
        {
            assert( false && "loss function type not recognized" );
        }

    // ------------------------------------------------------------------------
    // Do the actual scoring.

    Poco::JSON::Object obj;

    for ( const auto &name : names )
        {
            // TODO: Replace nullptr with &comm() after multithreading is
            // implemented.
            const auto metric = metrics::MetricParser::parse( name, nullptr );

            const auto scores = metric->score(
                _yhat, _yhat_nrows, _yhat_ncols, _y, _y_nrows, _y_ncols );

            for ( auto it = scores.begin(); it != scores.end(); ++it )
                {
                    obj.set( it->first, it->second );
                }
        }

    // ------------------------------------------------------------------------
    // Store scores.

    scores().from_json_obj( obj );

    // ------------------------------------------------------------------------
    // Extract values that can be send back to the client.

    Poco::JSON::Object client_obj;

    for ( const auto &name : names )
        {
            client_obj.set( name, obj.getArray( name ) );
        }

    // ------------------------------------------------------------------------

    return client_obj;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<RELBOOST_FLOAT>> DecisionTreeEnsemble::transform(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral ) const
{
    // ------------------------------------------------------
    // Check plausibility.

    if ( num_features() == 0 )
        {
            throw std::runtime_error( "Relboost model has not been fitted!" );
        }

    // ------------------------------------------------------
    // thread_nums signify the thread number that a particular row belongs to.
    // The idea is to separate the join keys as clearly as possible.

    const auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    const auto thread_nums = utils::DataFrameScatterer::build_thread_nums(
        _population.join_keys(), num_threads );

    // ------------------------------------------------------
    // Build deep copies of this ensemble.

    std::vector<DecisionTreeEnsemble> ensembles;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            ensembles.push_back( *this );
        }

    // -------------------------------------------------------
    // Launch threads and generate predictions on the subviews.

    auto features = std::make_shared<std::vector<RELBOOST_FLOAT>>(
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
                *this,
                features.get() );
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

std::vector<RELBOOST_FLOAT> DecisionTreeEnsemble::transform(
    const TableHolder &_table_holder, size_t _n_feature ) const
{
    assert( _n_feature < num_features() );

    return generate_predictions( trees()[_n_feature], _table_holder );
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj() const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    obj.set( "initial_prediction_", initial_prediction() );

    obj.set( "peripheral_names_", JSON::vector_to_array( peripheral_names() ) );

    obj.set( "placeholder_", placeholder().to_json_obj() );

    // ------------------------------------------------------------------------

    Poco::JSON::Array arr;

    for ( auto &tree : trees() )
        {
            arr.add( tree.to_json_obj() );
        }

    obj.set( "trees_", arr );

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

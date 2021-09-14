#include "relboost/ensemble/ensemble.hpp"

namespace relboost
{
namespace ensemble
{
// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble(
    const std::shared_ptr<const Hyperparameters> &_hyperparameters,
    const std::shared_ptr<const std::vector<std::string>> &_peripheral,
    const std::shared_ptr<const containers::Placeholder> &_placeholder )
    : impl_( DecisionTreeEnsembleImpl(
          _hyperparameters, _peripheral, _placeholder ) ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )
{
    loss_function_ = lossfunctions::LossFunctionParser::parse(
        _hyperparameters->loss_function_, impl().hyperparameters_, targets_ );

    if ( impl().placeholder_ )
        {
            placeholder().check_data_model( peripheral(), true );
        }
}

// ----------------------------------------------------------------------------

DecisionTreeEnsemble::DecisionTreeEnsemble( const Poco::JSON::Object &_obj )
    : impl_( DecisionTreeEnsembleImpl(
          std::make_shared<const Hyperparameters>(
              *JSON::get_object( _obj, "hyperparameters_" ) ),
          _obj.has( "peripheral_" )
              ? std::make_shared<const std::vector<std::string>>(
                    JSON::array_to_vector<std::string>(
                        JSON::get_array( _obj, "peripheral_" ) ) )
              : nullptr,
          _obj.has( "placeholder_" )
              ? std::make_shared<const containers::Placeholder>(
                    *JSON::get_object( _obj, "placeholder_" ) )
              : nullptr ) ),
      targets_( std::make_shared<std::vector<Float>>( 0 ) )
{
    // ------------------------------------------------------------------------

    initial_prediction() =
        JSON::get_value<Float>( _obj, "initial_prediction_" );

    loss_function_ = lossfunctions::LossFunctionParser::parse(
        hyperparameters().loss_function_, impl().hyperparameters_, targets_ );

    // ------------------------------------------------------------------------
    // Note that subensembles do not have schemata.

    if ( _obj.has( "population_schema_" ) )
        {
            impl().population_schema_ = std::make_shared<const helpers::Schema>(
                helpers::Schema::from_json(
                    *JSON::get_object( _obj, "population_schema_" ) ) );
        }

    // ------------------------------------------------------------------------
    // Note that subensembles do not have schemata.

    if ( _obj.has( "peripheral_schema_" ) )
        {
            impl().peripheral_schema_ =
                helpers::Schema::from_json( *jsonutils::JSON::get_object_array(
                    _obj, "peripheral_schema_" ) );
        }

    // ------------------------------------------------------------------------

    allow_http() = _obj.has( "allow_http_" )
                       ? JSON::get_value<bool>( _obj, "allow_http_" )
                       : false;

    // ------------------------------------------------------------------------

    const auto trees_objects = *JSON::get_array( _obj, "trees_" );

    for ( size_t i = 0; i < trees_objects.size(); ++i )
        {
            trees().push_back( decisiontrees::DecisionTree(
                impl().hyperparameters_,
                loss_function_,
                *trees_objects.getObject( static_cast<unsigned int>( i ) ) ) );
        }

    // ----------------------------------------

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
                        std::make_optional<DecisionTreeEnsemble>( *obj );
                }
        }

    // ----------------------------------------

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
                        std::make_optional<DecisionTreeEnsemble>( *obj );
                }
        }

    // ------------------------------------------------------------------------

    if ( impl().placeholder_ )
        {
            placeholder().check_data_model( peripheral(), true );
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

std::pair<Int, std::vector<size_t>> DecisionTreeEnsemble::calc_thread_nums(
    const containers::DataFrame &_population ) const
{
    // ------------------------------------------------------

    auto num_threads =
        Threadutils::get_num_threads( hyperparameters().num_threads_ );

    // ------------------------------------------------------

    const auto [thread_nums, n_unique] =
        utils::DataFrameScatterer::build_thread_nums(
            true, _population.nrows(), _population.join_keys(), num_threads );

    // ------------------------------------------------------

    if ( num_threads > n_unique )
        {
            num_threads = n_unique;
        }

    // ------------------------------------------------------

    return std::make_pair( num_threads, thread_nums );

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::clean_up()
{
    *this = DecisionTreeEnsemble( to_json_obj() );
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importances(
    const std::vector<Float> &_importance_factors,
    const fastprop::subfeatures::FastPropContainer &_fast_prop_container,
    const bool _is_subfeatures ) const
{
    auto importance_maker = utils::ImportanceMaker();

    assert_true( _importance_factors.size() == trees().size() );

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            const auto importances = column_importance_for_tree(
                _importance_factors.at( i ),
                _fast_prop_container,
                _is_subfeatures,
                trees().at( i ) );

            importance_maker.merge( importances );
        }

    if ( _is_subfeatures )
        {
            importance_maker.transfer_population();
        }

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

std::map<helpers::ColumnDescription, Float>
DecisionTreeEnsemble::column_importance_for_tree(
    const Float _importance_factor,
    const fastprop::subfeatures::FastPropContainer &_fast_prop_container,
    const bool _is_subfeatures,
    const decisiontrees::DecisionTree &_tree ) const
{
    if ( _importance_factor == 0.0 )
        {
            return std::map<helpers::ColumnDescription, Float>();
        }

    const auto p = _tree.peripheral_used();

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
                importance_maker.importance_factors_avg(),
                _fast_prop_container,
                true );

            importance_maker.merge( importances_avg );

            const auto importances_sum = sub_sum->column_importances(
                importance_maker.importance_factors_sum(),
                _fast_prop_container,
                true );

            importance_maker.merge( importances_sum );
        }

    _tree.handle_fast_prop_importances(
        _fast_prop_container, _is_subfeatures, &importance_maker );

    importance_maker.normalize();

    importance_maker.multiply( _importance_factor );

    return importance_maker.importances();
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::extract_schemas(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral )
{
    impl().population_schema_ =
        std::make_shared<helpers::Schema>( _population.to_schema() );

    auto peripheral_schema = std::make_shared<std::vector<helpers::Schema>>();

    for ( auto &df : _peripheral )
        {
            peripheral_schema->push_back( df.to_schema() );
        }

    impl().peripheral_schema_ = peripheral_schema;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit( const FitParams &_params )
{
    if ( num_features() != 0 )
        {
            throw std::runtime_error( "RelMT model has already been fitted!" );
        }

    if ( _params.population_.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    check_plausibility_of_targets( _params.population_ );

    extract_schemas( _params.population_, _params.peripheral_ );

    fit_spawn_threads(
        _params.population_,
        _params.peripheral_,
        _params.row_indices_,
        _params.word_indices_,
        _params.feature_container_,
        _params.logger_ );

    clean_up();
}

// ----------------------------------------------------------------------------

std::vector<std::tuple<decisiontrees::DecisionTree, Float, std::vector<Float>>>
DecisionTreeEnsemble::fit_candidate_features(
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function,
    const std::shared_ptr<const TableHolder> &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const std::shared_ptr<const std::vector<Float>> &_sample_weights ) const
{
    assert_true( _table_holder );

    std::vector<
        std::tuple<decisiontrees::DecisionTree, Float, std::vector<Float>>>
        candidates;

    for ( size_t ix_table_used = 0;
          ix_table_used < _table_holder->main_tables().size();
          ++ix_table_used )
        {
            // ------------------------------------------------------------------------

            if ( _table_holder->propositionalization().at( ix_table_used ) )
                {
                    continue;
                }

            // ------------------------------------------------------------------------

            const auto &output_table =
                _table_holder->main_tables().at( ix_table_used );

            const auto &input_table = containers::DataFrame(
                _table_holder->peripheral_tables().at( ix_table_used ) );

            const auto &subfeatures = _subfeatures.at( ix_table_used );

            // ------------------------------------------------------------------------

            assert_true( _sample_weights );

            assert_true( output_table.nrows() == _sample_weights->size() );

            // ------------------------------------------------------------------------
            // Matches can potentially use a lot of memory - better to create
            // them anew when needed.

            auto matches = utils::Matchmaker::make_matches(
                output_table, input_table, _sample_weights );

            debug_log(
                "Number of matches: " + std::to_string( matches.size() ) );

            // ------------------------------------------------------------------------

            std::vector<std::shared_ptr<lossfunctions::LossFunction>>
                aggregations;

            aggregations.push_back( std::make_shared<aggregations::Avg>(
                _loss_function,
                matches,
                input_table,
                output_table,
                hyperparameters().allow_null_weights_,
                &comm() ) );

            aggregations.push_back( std::make_shared<aggregations::Sum>(
                _loss_function, input_table, output_table, &comm() ) );

            // ------------------------------------------------------------------------

            for ( auto &agg : aggregations )
                {
                    _loss_function->commit();

                    auto new_candidate = decisiontrees::DecisionTree(
                        impl().hyperparameters_, agg, ix_table_used, &comm() );

                    new_candidate.fit(
                        output_table,
                        input_table,
                        subfeatures,
                        matches.begin(),
                        matches.end() );

                    auto new_predictions = new_candidate.transform(
                        output_table, input_table, subfeatures );

                    assert_true( new_predictions );

                    _loss_function->reduce_predictions(
                        new_candidate.intercept(), &new_predictions );

                    new_candidate.calc_update_rate( *new_predictions );

                    const auto new_loss_reduction =
                        _loss_function->evaluate_tree(
                            new_candidate.update_rate(), *new_predictions );

                    candidates.emplace_back( std::make_tuple(
                        std::move( new_candidate ),
                        new_loss_reduction,
                        std::move( *new_predictions ) ) );
                }

            // ------------------------------------------------------------------------
        }

    return candidates;
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_new_features(
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function,
    const std::shared_ptr<const TableHolder> &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const size_t _num_features )
{
    // ------------------------------------------------------------------------

    assert_true( _loss_function );

    assert_true( _table_holder );

    assert_true(
        _table_holder->main_tables().size() ==
        _table_holder->peripheral_tables().size() );

    assert_true( _table_holder->main_tables().size() == _subfeatures.size() );

    assert_true( _table_holder->main_tables().size() > 0 );

    // ------------------------------------------------------------------------

    const auto sample_weights = _loss_function->make_sample_weights();

    _loss_function->calc_sums();

    // ------------------------------------------------------------------------

    auto candidates = fit_candidate_features(
        _loss_function, _table_holder, _subfeatures, sample_weights );

    // ------------------------------------------------------------------------

    keep_best_candidates( _loss_function, _num_features, &candidates );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::fit_spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const helpers::RowIndexContainer &_row_indices,
    const helpers::WordIndexContainer &_word_indices,
    const std::optional<const helpers::FeatureContainer> &_feature_container,
    const std::shared_ptr<const logging::AbstractLogger> _logger )
{
    // ------------------------------------------------------

    const auto [num_threads, thread_nums] = calc_thread_nums( _population );

    // ------------------------------------------------------

    multithreading::Communicator comm( num_threads );

    set_comm( &comm );

    // ------------------------------------------------------

    std::vector<ensemble::DecisionTreeEnsemble> ensembles;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            ensembles.push_back( *this );
        }

    // ------------------------------------------------------

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::fit_ensemble,
                ThreadutilsFitParams{
                    .comm_ = comm,
                    .ensemble_ = ensembles.at( i ),
                    .feature_container_ = _feature_container,
                    .peripheral_ = _peripheral,
                    .population_ = _population,
                    .row_indices_ = _row_indices,
                    .this_thread_num_ = i + 1,
                    .thread_nums_ = thread_nums,
                    .word_indices_ = _word_indices } ) );
        }

    // ------------------------------------------------------

    try
        {
            Threadutils::fit_ensemble( ThreadutilsFitParams{
                .comm_ = comm,
                .ensemble_ = *this,
                .feature_container_ = _feature_container,
                .logger_ = _logger,
                .peripheral_ = _peripheral,
                .population_ = _population,
                .row_indices_ = _row_indices,
                .this_thread_num_ = 0,
                .thread_nums_ = thread_nums,
                .word_indices_ = _word_indices } );
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

std::pair<
    std::shared_ptr<lossfunctions::LossFunction>,
    std::shared_ptr<const TableHolder>>
DecisionTreeEnsemble::init(
    const containers::DataFrameView &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const helpers::RowIndexContainer &_row_indices,
    const helpers::WordIndexContainer &_word_indices,
    const std::optional<const helpers::FeatureContainer> &_feature_container )
{
    // ------------------------------------------------------------------------

    targets().resize( _population.nrows() );

    assert_true( _population.num_targets() == 1 );

    for ( size_t i = 0; i < _population.nrows(); ++i )
        {
            targets()[i] = _population.target( i, 0 );
        }

    // ------------------------------------------------------------------------

    calc_initial_prediction();

    // ------------------------------------------------------------------------

    loss_function().init_yhat_old( initial_prediction() );

    loss_function().calc_gradients();

    loss_function().calc_sampling_rate(
        false, hyperparameters().sampling_factor_, &comm() );

    // ------------------------------------------------------------------------

    const auto table_holder = std::make_shared<const TableHolder>(
        placeholder(),
        _population,
        _peripheral,
        peripheral(),
        _row_indices,
        _word_indices,
        _feature_container );

    // ------------------------------------------------------------------------

    return std::make_pair( loss_function_, table_holder );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::keep_best_candidates(
    const std::shared_ptr<lossfunctions::LossFunction> &_loss_function,
    const size_t _num_features,
    std::vector<
        std::tuple<decisiontrees::DecisionTree, Float, std::vector<Float>>>
        *_candidates )
{
    using TupleT =
        std::tuple<decisiontrees::DecisionTree, Float, std::vector<Float>>;

    const auto loss_is_smaller = []( const TupleT &t1, const TupleT &t2 ) {
        return std::get<1>( t1 ) < std::get<1>( t2 );
    };

    std::ranges::sort( *_candidates, loss_is_smaller );

    const auto num_remaining = _num_features - num_features();

    const auto num_keep = std::min( _candidates->size(), num_remaining );

    for ( size_t i = 0; i < num_keep; ++i )
        {
            auto &candidate = std::get<0>( _candidates->at( i ) );

            const auto &pred = std::get<2>( _candidates->at( i ) );

            _loss_function->update_yhat_old(
                candidate.update_rate() * hyperparameters().shrinkage_, pred );

            _loss_function->calc_gradients();

            _loss_function->commit();

            trees().emplace_back( std::move( candidate ) );

            trees().back().clear();
        }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<std::string, std::string>>
DecisionTreeEnsemble::make_peripheral_map() const
{
    assert_true( peripheral_schema().size() == peripheral().size() );

    const auto peripheral_map =
        std::make_shared<std::map<std::string, std::string>>();

    for ( size_t i = 0; i < peripheral_schema().size(); ++i )
        {
            ( *peripheral_map )[peripheral().at( i )] =
                peripheral_schema().at( i ).name();
        }

    return peripheral_map;
}

// ----------------------------------------------------------------------------

std::vector<containers::Predictions> DecisionTreeEnsemble::make_subpredictions(
    const TableHolder &_table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator *_comm ) const
{
    return SubtreeHelper::make_predictions(
        _table_holder, subensembles_avg_, subensembles_sum_, _logger, _comm );
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

containers::Features DecisionTreeEnsemble::transform(
    const TransformParams &_params ) const
{
    // ------------------------------------------------------

    if ( _params.population_.nrows() == 0 )
        {
            throw std::runtime_error(
                "Population table needs to contain at least some data!" );
        }

    // -------------------------------------------------------

    const auto init_feature = [&_params]( const size_t ix ) {
        return std::make_shared<std::vector<Float>>(
            _params.population_.nrows() );
    };

    auto range = _params.index_ | std::views::transform( init_feature );

    auto features =
        stl::collect::vector<std::shared_ptr<std::vector<Float>>>( range );

    // -------------------------------------------------------

    transform_spawn_threads(
        _params.population_,
        _params.peripheral_,
        _params.index_,
        _params.word_indices_,
        _params.feature_container_,
        _params.logger_,
        &features );

    // -------------------------------------------------------

    return features;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Float>> DecisionTreeEnsemble::transform(
    const TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    size_t _n_feature ) const
{
    assert_true( _n_feature < num_features() );

    const auto &decision_tree = trees().at( _n_feature );

    const auto peripheral_used = decision_tree.peripheral_used();

    assert_true( peripheral_used < _table_holder.main_tables().size() );

    assert_true( peripheral_used < _table_holder.peripheral_tables().size() );

    assert_true( peripheral_used < _subfeatures.size() );

    return decision_tree.transform(
        _table_holder.main_tables().at( peripheral_used ),
        _table_holder.peripheral_tables().at( peripheral_used ),
        _subfeatures.at( peripheral_used ) );
}

// ----------------------------------------------------------------------------

void DecisionTreeEnsemble::transform_spawn_threads(
    const containers::DataFrame &_population,
    const std::vector<containers::DataFrame> &_peripheral,
    const std::vector<size_t> &_index,
    const std::optional<helpers::WordIndexContainer> &_word_indices,
    const std::optional<const helpers::FeatureContainer> &_feature_container,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    containers::Features *_features ) const
{
    // -------------------------------------------------------

    const auto [num_threads, thread_nums] = calc_thread_nums( _population );

    // -------------------------------------------------------

    multithreading::Communicator comm( num_threads );

    // -------------------------------------------------------

    std::vector<std::thread> threads;

    for ( size_t i = 0; i < num_threads - 1; ++i )
        {
            threads.push_back( std::thread(
                Threadutils::transform_ensemble,
                ThreadutilsTransformParams{
                    .comm_ = comm,
                    .ensemble_ = *this,
                    .feature_container_ = _feature_container,
                    .features_ = *_features,
                    .index_ = _index,
                    .peripheral_ = _peripheral,
                    .population_ = _population,
                    .this_thread_num_ = i + 1,
                    .thread_nums_ = thread_nums,
                    .word_indices_ = _word_indices } ) );
        }

    // ------------------------------------------------------

    try
        {
            Threadutils::transform_ensemble( ThreadutilsTransformParams{
                .comm_ = comm,
                .ensemble_ = *this,
                .feature_container_ = _feature_container,
                .features_ = *_features,
                .index_ = _index,
                .logger_ = _logger,
                .peripheral_ = _peripheral,
                .population_ = _population,
                .this_thread_num_ = 0,
                .thread_nums_ = thread_nums,
                .word_indices_ = _word_indices } );
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
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeEnsemble::to_json_obj(
    const bool _schema_only ) const
{
    // ------------------------------------------------------------------------

    Poco::JSON::Object obj;

    // ------------------------------------------------------------------------

    obj.set( "type_", "Relboost" );

    // ------------------------------------------------------------------------

    obj.set( "hyperparameters_", hyperparameters().to_json_obj() );

    // ------------------------------------------------------------------------
    // When relboost is used as a predictor, it has no peripheral_ or
    // placeholder_.

    if ( impl().peripheral_ )
        {
            obj.set( "peripheral_", JSON::vector_to_array_ptr( peripheral() ) );
        }

    if ( impl().placeholder_ )
        {
            obj.set( "placeholder_", placeholder().to_json_obj() );
        }

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

    obj.set( "allow_http_", allow_http() );

    // ------------------------------------------------------------------------

    obj.set( "initial_prediction_", initial_prediction() );

    // ------------------------------------------------------------------------

    Poco::JSON::Array::Ptr trees_arr( new Poco::JSON::Array() );

    for ( auto &tree : trees() )
        {
            trees_arr->add( tree.to_json_obj() );
        }

    obj.set( "trees_", trees_arr );

    // ----------------------------------------

    Poco::JSON::Array::Ptr features_avg( new Poco::JSON::Array() );

    for ( const auto &sub : subensembles_avg_ )
        {
            if ( sub )
                {
                    auto obj = Poco::JSON::Object::Ptr(
                        new Poco::JSON::Object( sub->to_json_obj() ) );
                    features_avg->add( obj );
                }
            else
                {
                    features_avg->add( Poco::Dynamic::Var() );
                }
        }

    obj.set( "subfeatures1_", features_avg );

    // ----------------------------------------

    Poco::JSON::Array::Ptr features_sum( new Poco::JSON::Array() );

    for ( const auto &sub : subensembles_sum_ )
        {
            if ( sub )
                {
                    auto obj = Poco::JSON::Object::Ptr(
                        new Poco::JSON::Object( sub->to_json_obj() ) );
                    features_sum->add( obj );
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

void DecisionTreeEnsemble::subfeatures_to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const helpers::VocabularyTree &_vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix,
    const size_t _offset,
    const std::shared_ptr<const std::map<std::string, std::string>>
        _peripheral_map,
    std::vector<std::string> *_sql ) const
{
    assert_true( subensembles_avg_.size() == subensembles_sum_.size() );

    assert_true( subensembles_avg_.size() == _vocabulary.subtrees().size() );

    assert_true( _sql_dialect_generator );

    assert_true( _peripheral_map );

    for ( size_t i = 0; i < subensembles_avg_.size(); ++i )
        {
            if ( subensembles_avg_.at( i ) )
                {
                    assert_true( _vocabulary.subtrees().at( i ) );

                    const auto sub_avg = subensembles_avg_.at( i )->to_sql(
                        _categories,
                        _vocabulary.subtrees().at( i ).value(),
                        _sql_dialect_generator,
                        _feature_prefix + std::to_string( i + 1 ) + "_",
                        0,
                        true,
                        _peripheral_map );

                    _sql->insert( _sql->end(), sub_avg.begin(), sub_avg.end() );

                    assert_true( subensembles_sum_.at( i ) );

                    const auto sub_sum = subensembles_sum_.at( i )->to_sql(
                        _categories,
                        _vocabulary.subtrees().at( i ).value(),
                        _sql_dialect_generator,
                        _feature_prefix + std::to_string( i + 1 ) + "_",
                        subensembles_avg_.at( i )->num_features(),
                        true,
                        _peripheral_map );

                    _sql->insert( _sql->end(), sub_sum.begin(), sub_sum.end() );

                    const auto to_feature_name =
                        [&_feature_prefix,
                         i]( const size_t _feature_num ) -> std::string {
                        return "feature_" + _feature_prefix +
                               std::to_string( i + 1 ) + "_" +
                               std::to_string( _feature_num + 1 );
                    };

                    const auto iota = stl::iota<size_t>(
                        0,
                        subensembles_avg_.at( i )->num_features() +
                            subensembles_sum_.at( i )->num_features() );

                    const auto autofeatures = stl::collect::vector<std::string>(
                        iota | std::views::transform( to_feature_name ) );

                    const auto it = _peripheral_map->find(
                        subensembles_avg_.at( i )->placeholder().name() );

                    assert_true( it != _peripheral_map->end() );

                    if ( autofeatures.size() > 0 )
                        {
                            const auto feature_table =
                                _sql_dialect_generator->make_feature_table(
                                    it->second,
                                    autofeatures,
                                    {},
                                    {},
                                    {},
                                    "_" + _feature_prefix +
                                        std::to_string( i + 1 ) );

                            _sql->push_back( feature_table + "\n" );
                        }
                }
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> DecisionTreeEnsemble::to_sql(
    const std::shared_ptr<const std::vector<strings::String>> &_categories,
    const helpers::VocabularyTree &_vocabulary,
    const std::shared_ptr<const helpers::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix,
    const size_t _offset,
    const bool _subfeatures,
    const std::shared_ptr<const std::map<std::string, std::string>>
        &_peripheral_map ) const
{
    assert_true( _categories );

    std::vector<std::string> sql;

    if ( _subfeatures )
        {
            subfeatures_to_sql(
                _categories,
                _vocabulary,
                _sql_dialect_generator,
                _feature_prefix,
                _offset,
                _peripheral_map ? _peripheral_map : make_peripheral_map(),
                &sql );
        }

    for ( size_t i = 0; i < trees().size(); ++i )
        {
            const auto &tree = trees().at( i );

            const auto p = tree.peripheral_used();

            assert_true( p < placeholder().joined_tables_.size() );

            const auto &prop_out = placeholder().propositionalization_;

            const auto &prop_in =
                placeholder().joined_tables_.at( p ).propositionalization_;

            const bool has_normal_subfeatures = std::any_of(
                prop_in.begin(), prop_in.end(), std::logical_not() );

            const bool output_has_prop = std::any_of(
                prop_out.begin(), prop_out.end(), std::identity() );

            const bool input_has_prop =
                std::any_of( prop_in.begin(), prop_in.end(), std::identity() );

            const auto has_subfeatures = std::make_tuple(
                has_normal_subfeatures, output_has_prop, input_has_prop );

            sql.push_back( tree.to_sql(
                *_categories,
                _vocabulary,
                _sql_dialect_generator,
                _feature_prefix,
                std::to_string( _offset + i + 1 ),
                has_subfeatures ) );
        }

    return sql;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

#include "relcit/ensemble/ensemble.hpp"

namespace relcit
{
namespace ensemble
{
// ----------------------------------------------------------------------------

void SubtreeHelper::fit_subensemble(
    const std::string& _agg_type,
    const std::shared_ptr<const TableHolder>& _table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::shared_ptr<const std::map<Int, Int>>& _output_map,
    const Hyperparameters& _hyperparameters,
    const size_t _ix_perip_used,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    multithreading::Communicator* _comm,
    std::optional<DecisionTreeEnsemble>* _subensemble )
{
    assert_true( _table_holder );

    assert_true( _loss_function );

    assert_true( _table_holder->subtables_[_ix_perip_used] );

    const auto subtable_holder = std::make_shared<const TableHolder>(
        *_table_holder->subtables_[_ix_perip_used] );

    assert_true( subtable_holder->main_tables_.size() > 0 );

    const auto input_table = containers::DataFrameView(
        _table_holder->peripheral_tables_[_ix_perip_used],
        subtable_holder->main_tables_[0].rows_ptr() );

    // The input map is needed for propagating sampling.
    const auto input_map =
        utils::Mapper::create_rows_map( input_table.rows_ptr() );

    const auto aggregation_index =
        std::make_shared<aggregations::AggregationIndex>(
            input_table,
            _table_holder->main_tables_[_ix_perip_used],
            input_map,
            _output_map,
            _hyperparameters.use_timestamps_ );

    auto intermediate_agg = std::shared_ptr<lossfunctions::LossFunction>();

    if ( _agg_type == "AVG" )
        {
            intermediate_agg = std::make_shared<aggregations::Avg>(
                aggregation_index,
                _loss_function,
                _table_holder->peripheral_tables_[_ix_perip_used],
                _table_holder->main_tables_[_ix_perip_used],
                _comm );
        }
    else if ( _agg_type == "SUM" )
        {
            intermediate_agg = std::make_shared<aggregations::Sum>(
                aggregation_index,
                _loss_function,
                _table_holder->peripheral_tables_[_ix_perip_used],
                _table_holder->main_tables_[_ix_perip_used],
                _comm );
        }
    else
        {
            assert_true( false && "agg_type not known!" );
        }

    ( *_subensemble )->init_as_subensemble( _comm );

    ( *_subensemble )
        ->fit_subensembles( subtable_holder, _logger, intermediate_agg );

    auto predictions =
        ( *_subensemble )
            ->make_subpredictions( *subtable_holder, _logger, _comm );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( *subtable_holder, predictions );

    const auto num_features =
        static_cast<size_t>( _hyperparameters.num_subfeatures_ );

    utils::Logger::log(
        "RelCITModel: Training subfeatures...", _logger, _comm );

    for ( size_t i = 0; i < num_features; ++i )
        {
            ( *_subensemble )
                ->fit_new_feature(
                    intermediate_agg, subtable_holder, subfeatures );

            const auto progress = ( ( i + 1 ) * 100 ) / num_features;

            utils::Logger::log(
                "Trained FEATURE_" + std::to_string( i + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );
        }

    _loss_function->reset_yhat_old();

    _loss_function->calc_gradients();

    _loss_function->commit();
}

// ----------------------------------------------------------------------------

void SubtreeHelper::fit_subensembles(
    const std::shared_ptr<const TableHolder>& _table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const DecisionTreeEnsemble& _ensemble,
    const std::shared_ptr<lossfunctions::LossFunction>& _loss_function,
    multithreading::Communicator* _comm,
    std::vector<std::optional<DecisionTreeEnsemble>>* _subensembles_avg,
    std::vector<std::optional<DecisionTreeEnsemble>>* _subensembles_sum )
{
    // ----------------------------------------------------------------

    const auto hyperparameters =
        std::make_shared<const Hyperparameters>( _ensemble.hyperparameters() );

    const auto peripheral = std::make_shared<const std::vector<std::string>>(
        _ensemble.peripheral() );

    const auto placeholder = _ensemble.placeholder();

    // ----------------------------------------------------------------

    assert_true( _table_holder );

    assert_true(
        _table_holder->subtables_.size() ==
        _table_holder->main_tables_.size() );

    assert_true(
        _table_holder->subtables_.size() ==
        _table_holder->peripheral_tables_.size() );

    assert_true(
        _table_holder->subtables_.size() == placeholder.joined_tables_.size() );

    // ----------------------------------------------------------------
    // Set up the subensembles.

    const auto num_tables = _table_holder->subtables_.size();

    auto subensembles_avg =
        std::vector<std::optional<DecisionTreeEnsemble>>( num_tables );

    auto subensembles_sum =
        std::vector<std::optional<DecisionTreeEnsemble>>( num_tables );

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( _table_holder->subtables_.at( i ) )
                {
                    const auto joined_table =
                        std::make_shared<const containers::Placeholder>(
                            placeholder.joined_tables_.at( i ) );

                    assert_true( joined_table->joined_tables_.size() > 0 );

                    subensembles_avg.at( i ) =
                        std::make_optional<DecisionTreeEnsemble>(
                            hyperparameters, peripheral, joined_table );

                    subensembles_sum.at( i ) =
                        std::make_optional<DecisionTreeEnsemble>(
                            hyperparameters, peripheral, joined_table );
                }
            else
                {
                    assert_true(
                        placeholder.joined_tables_.at( i )
                            .joined_tables_.size() == 0 );
                }
        }

    // ----------------------------------------------------------------
    // If there are no subfeatures, we can stop here.

    const bool no_subfeatures = std::none_of(
        subensembles_avg.cbegin(),
        subensembles_avg.cend(),
        []( const std::optional<DecisionTreeEnsemble>& val ) {
            return val && true;
        } );

    if ( no_subfeatures )
        {
            *_subensembles_avg = std::move( subensembles_avg );
            *_subensembles_sum = std::move( subensembles_sum );

            return;
        }

    // ----------------------------------------------------------------
    // Create the rows map (it stays the same over all aggregations).

    const auto rows_map = utils::Mapper::create_rows_map(
        _table_holder->main_tables_[0].rows_ptr() );

    // ----------------------------------------------------------------
    // Fit the subensembles_avg.

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( subensembles_avg[i] )
                {
                    fit_subensemble(
                        "AVG",
                        _table_holder,
                        _logger,
                        rows_map,
                        _ensemble.hyperparameters(),
                        i,
                        _loss_function,
                        _comm,
                        &subensembles_avg[i] );
                }
        }

    // ----------------------------------------------------------------
    // Fit the subensembles_sum.

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( subensembles_sum[i] )
                {
                    fit_subensemble(
                        "SUM",
                        _table_holder,
                        _logger,
                        rows_map,
                        _ensemble.hyperparameters(),
                        i,
                        _loss_function,
                        _comm,
                        &subensembles_sum[i] );
                }
        }

    // ----------------------------------------------------------------
    // Store the subensembles.

    *_subensembles_avg = std::move( subensembles_avg );

    *_subensembles_sum = std::move( subensembles_sum );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<containers::Predictions> SubtreeHelper::make_predictions(
    const TableHolder& _table_holder,
    const std::vector<std::optional<DecisionTreeEnsemble>>& _subensembles_avg,
    const std::vector<std::optional<DecisionTreeEnsemble>>& _subensembles_sum,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm )
{
    const auto size = _table_holder.subtables_.size();

    assert_true( size == _subensembles_avg.size() );
    assert_true( size == _subensembles_sum.size() );

    std::vector<containers::Predictions> predictions( size );

    for ( size_t i = 0; i < size; ++i )
        {
            if ( !_table_holder.subtables_.at( i ) )
                {
                    continue;
                }

            const auto& subtable_holder = *_table_holder.subtables_.at( i );

            assert_true( subtable_holder.main_tables_.size() > 0 );

            assert_true( _subensembles_avg.at( i ) );

            assert_true( _subensembles_sum.at( i ) );

            make_predictions_for_one_subensemble(
                _subensembles_avg.at( i ),
                subtable_holder,
                _logger,
                _comm,
                &predictions.at( i ) );

            make_predictions_for_one_subensemble(
                _subensembles_sum.at( i ),
                subtable_holder,
                _logger,
                _comm,
                &predictions.at( i ) );
        }

    return predictions;
}

// ----------------------------------------------------------------------------

void SubtreeHelper::make_predictions_for_one_subensemble(
    const std::optional<DecisionTreeEnsemble>& _subensemble,
    const TableHolder& _subtable_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    multithreading::Communicator* _comm,
    containers::Predictions* _predictions )
{
    assert_true( _subensemble );

    const auto subpredictions =
        _subensemble->make_subpredictions( _subtable_holder, _logger, _comm );

    const auto subsubfeatures =
        SubtreeHelper::make_subfeatures( _subtable_holder, subpredictions );

    utils::Logger::log(
        "RelCITModel: Building subfeatures...", _logger, _comm );

    const auto num_features = _subensemble->num_features();

    for ( size_t i = 0; i < num_features; ++i )
        {
            _predictions->emplace_back( _subensemble->transform(
                _subtable_holder, subsubfeatures, i ) );

            assert_true( std::all_of(
                _predictions->back().begin(),
                _predictions->back().end(),
                []( Float val ) {
                    return !std::isnan( val ) && !std::isinf( val );
                } ) );

            const auto progress = ( ( i + 1 ) * 100 ) / num_features;

            utils::Logger::log(
                "Built FEATURE_" + std::to_string( i + 1 ) +
                    ". Progress: " + std::to_string( progress ) + "\%.",
                _logger,
                _comm );
        }
}

// ----------------------------------------------------------------------------

std::vector<containers::Subfeatures> SubtreeHelper::make_subfeatures(
    const TableHolder& _table_holder,
    const std::vector<containers::Predictions>& _predictions )
{
    const auto size = _table_holder.subtables_.size();

    assert_true( size == _predictions.size() );

    auto subfeatures = std::vector<containers::Subfeatures>( size );

    for ( size_t i = 0; i < size; ++i )
        {
            if ( !_table_holder.subtables_.at( i ) )
                {
                    continue;
                }

            assert_true( _table_holder.subtables_.at( i ) );

            assert_true(
                _table_holder.subtables_.at( i )->main_tables_.size() > 0 );

            assert_true( _table_holder.subtables_.at( i )
                             ->main_tables_.at( 0 )
                             .rows_ptr() );

            const auto rows_map = utils::Mapper::create_rows_map(
                _table_holder.subtables_.at( i )
                    ->main_tables_.at( 0 )
                    .rows_ptr() );

            const auto& p = _predictions.at( i );

            for ( size_t j = 0; j < p.size(); ++j )
                {
                    assert_true(
                        _table_holder.subtables_.at( i )
                            ->main_tables_.at( 0 )
                            .rows_ptr()
                            ->size() == p.at( j ).size() );

                    assert_true( std::all_of(
                        p.at( j ).begin(),
                        p.at( j ).end(),
                        []( const Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );

                    const auto column = containers::Column<Float>(
                        p.at( j ).data(),
                        "FEATURE_" + std::to_string( j + 1 ),
                        p.at( j ).size() );

                    assert_true( std::all_of(
                        column.begin(), column.end(), []( const Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );

                    const auto view =
                        containers::ColumnView<Float, std::map<Int, Int>>(
                            column, rows_map );

                    assert_true( std::all_of(
                        view.col().begin(),
                        view.col().end(),
                        []( const Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );

                    subfeatures.at( i ).push_back( view );
                }
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relcit

#include "relboost/ensemble/ensemble.hpp"

namespace relboost
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
        ( *_subensemble )->make_subpredictions( *subtable_holder );

    auto subfeatures =
        SubtreeHelper::make_subfeatures( *subtable_holder, predictions );

    for ( size_t i = 0; i < _hyperparameters.num_subfeatures_; ++i )
        {
            ( *_subensemble )
                ->fit_new_feature(
                    intermediate_agg, subtable_holder, subfeatures );
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
        _ensemble.peripheral_names() );

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
            if ( _table_holder->subtables_[i] )
                {
                    const auto joined_table =
                        std::make_shared<const Placeholder>(
                            placeholder.joined_tables_[i] );

                    assert_true( joined_table->joined_tables_.size() > 0 );

                    subensembles_avg[i] =
                        std::make_optional<DecisionTreeEnsemble>(
                            _ensemble.encoding(),
                            hyperparameters,
                            peripheral,
                            joined_table );

                    subensembles_sum[i] =
                        std::make_optional<DecisionTreeEnsemble>(
                            _ensemble.encoding(),
                            hyperparameters,
                            peripheral,
                            joined_table );
                }
            else
                {
                    assert_true(
                        placeholder.joined_tables_[i].joined_tables_.size() ==
                        0 );
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
    const std::vector<std::optional<DecisionTreeEnsemble>>& _subensembles_sum )
{
    const auto size = _table_holder.subtables_.size();

    assert_true( size == _subensembles_avg.size() );
    assert_true( size == _subensembles_sum.size() );

    std::vector<containers::Predictions> predictions( size );

    for ( size_t i = 0; i < size; ++i )
        {
            if ( !_table_holder.subtables_[i] )
                {
                    continue;
                }

            assert_true( _table_holder.subtables_[i] );

            const auto& subtable_holder = *_table_holder.subtables_[i];

            assert_true( subtable_holder.main_tables_.size() > 0 );

            assert_true( _subensembles_avg[i] );

            assert_true( _subensembles_sum[i] );

            auto subpredictions =
                _subensembles_avg[i]->make_subpredictions( subtable_holder );

            auto subsubfeatures = SubtreeHelper::make_subfeatures(
                subtable_holder, subpredictions );

            for ( size_t j = 0; j < _subensembles_avg[i]->num_features(); ++j )
                {
                    predictions[i].emplace_back(
                        _subensembles_avg[i]->transform(
                            subtable_holder, subsubfeatures, j ) );

                    assert_true( std::all_of(
                        predictions[i].back().begin(),
                        predictions[i].back().end(),
                        []( Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );
                }

            subpredictions =
                _subensembles_sum[i]->make_subpredictions( subtable_holder );

            subsubfeatures = SubtreeHelper::make_subfeatures(
                subtable_holder, subpredictions );

            for ( size_t j = 0; j < _subensembles_sum[i]->num_features(); ++j )
                {
                    predictions[i].emplace_back(
                        _subensembles_sum[i]->transform(
                            subtable_holder, subsubfeatures, j ) );

                    assert_true( std::all_of(
                        predictions[i].back().begin(),
                        predictions[i].back().end(),
                        []( Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );
                }
        }

    return predictions;
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
            if ( !_table_holder.subtables_[i] )
                {
                    continue;
                }

            assert_true( _table_holder.subtables_[i] );

            assert_true( _table_holder.subtables_[i]->main_tables_.size() > 0 );

            assert_true(
                _table_holder.subtables_[i]->main_tables_[0].rows_ptr() );

            const auto rows_map = utils::Mapper::create_rows_map(
                _table_holder.subtables_[i]->main_tables_[0].rows_ptr() );

            const auto& p = _predictions[i];

            for ( size_t j = 0; j < p.size(); ++j )
                {
                    assert_true(
                        _table_holder.subtables_[i]
                            ->main_tables_[0]
                            .rows_ptr()
                            ->size() == p[j].size() );

                    assert_true( std::all_of(
                        p[j].begin(), p[j].end(), []( const Float val ) {
                            return !std::isnan( val ) && !std::isinf( val );
                        } ) );

                    const auto column = containers::Column<Float>(
                        p[j].data(),
                        "FEATURE_" + std::to_string( j + 1 ),
                        p[j].size() );

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

                    subfeatures[i].push_back( view );
                }
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost

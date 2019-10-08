#include "relboost/ensemble/ensemble.hpp"

/*namespace relboost
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
    DecisionTreeEnsemble* _subensemble )
{
    assert_true( _table_holder );

    const auto subtable_holder = std::make_shared<const TableHolder>(
        *_table_holder->subtables_[_ix_perip_used] );

    assert_true( subtable_holder->main_tables_.size() > 0 );

    const auto input_table = containers::DataFrameView(
        _table_holder->peripheral_tables_[_ix_perip_used],
        subtable_holder->main_tables_[0].rows_ptr() );

    // The input map is needed for propagating sampling.
    const auto input_map =
        utils::Mapper::create_rows_map( input_table.rows_ptr() );

    const auto aggregation_index = aggregations::AggregationIndex(
        input_table,
        _table_holder->main_tables_[_ix_perip_used],
        input_map,
        _output_map,
        _hyperparameters.use_timestamps_ );

    auto intermediate_agg = std::unique_ptr<lossfunctions::LossFunction>();

    if ( _agg_type == "AVG" )
        {
            intermediate_agg = std::make_unique<aggregations::Avg>(
                aggregation_index,
                _loss_function,
                input_table,
                _table_holder->main_tables_[_ix_perip_used],
                _comm );
        }

    _subensemble->fit(
        subtable_holder,
        _logger,
        _hyperparameters.num_subfeatures_,
        intermediate_agg.get(),
        _comm );

    _opt->reset_yhat_old();
}

// ----------------------------------------------------------------------------

void SubtreeHelper::fit_subensembles(
    const std::shared_ptr<const decisiontrees::TableHolder>& _table_holder,
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const DecisionTreeEnsemble& _ensemble,
    optimizationcriteria::OptimizationCriterion* _opt,
    multithreading::Communicator* _comm,
    std::vector<containers::Optional<DecisionTreeEnsemble>>* _subensembles_avg,
    std::vector<containers::Optional<DecisionTreeEnsemble>>* _subensembles_sum )
{
    // ----------------------------------------------------------------

    const auto hyperparameters =
        std::make_shared<const descriptors::Hyperparameters>(
            _ensemble.hyperparameters() );

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
    // Set up the subfeatures.

    const auto num_tables = _table_holder->subtables_.size();

    auto subensembles_avg =
        std::vector<containers::Optional<DecisionTreeEnsemble>>( num_tables );

    auto subensembles_sum =
        std::vector<containers::Optional<DecisionTreeEnsemble>>( num_tables );

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( _table_holder->subtables_[i] )
                {
                    const auto joined_table =
                        std::make_shared<const decisiontrees::Placeholder>(
                            placeholder.joined_tables_[i] );

                    assert_true( joined_table->joined_tables_.size() > 0 );

                    subensembles_avg[i].reset( new DecisionTreeEnsemble(
                        _ensemble.categories(),
                        hyperparameters,
                        peripheral,
                        joined_table ) );

                    subensembles_sum[i].reset( new DecisionTreeEnsemble(
                        _ensemble.categories(),
                        hyperparameters,
                        peripheral,
                        joined_table ) );
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
        []( const containers::Optional<DecisionTreeEnsemble>& val ) {
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
                    fit_subensemble<aggregations::AggregationType::Avg>(
                        _table_holder,
                        _logger,
                        rows_map,
                        _ensemble.hyperparameters(),
                        i,
                        _opt,
                        _comm,
                        subensembles_avg[i].get() );
                }
        }

    // ----------------------------------------------------------------
    // Fit the subensembles_sum.

    for ( size_t i = 0; i < num_tables; ++i )
        {
            if ( subensembles_sum[i] )
                {
                    fit_subensemble<aggregations::AggregationType::Sum>(
                        _table_holder,
                        _logger,
                        rows_map,
                        _ensemble.hyperparameters(),
                        i,
                        _opt,
                        _comm,
                        subensembles_sum[i].get() );
                }
        }

    // ----------------------------------------------------------------
    // Store the subfeatures.

    *_subensembles_avg = std::move( subensembles_avg );

    *_subensembles_sum = std::move( subensembles_sum );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<containers::Predictions> SubtreeHelper::make_predictions(
    const decisiontrees::TableHolder& _table_holder,
    const std::vector<containers::Optional<DecisionTreeEnsemble>>&
        _subensembles_avg,
    const std::vector<containers::Optional<DecisionTreeEnsemble>>&
        _subensembles_sum )
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

            assert_true( _table_holder.subtables_[i]->main_tables_.size() > 0 );

            auto impl = containers::Optional<aggregations::AggregationImpl>(
                new aggregations::AggregationImpl(
                    _table_holder.subtables_[i]->main_tables_[0].nrows() ) );

            assert_true( _subensembles_avg[i] );

            assert_true( _subensembles_sum[i] );

            auto predictions_avg = _subensembles_avg[i]->transform(
                *_table_holder.subtables_[i], &impl );

            auto predictions_sum = _subensembles_sum[i]->transform(
                *_table_holder.subtables_[i], &impl );

            for ( auto& p : predictions_avg )
                {
                    predictions[i].emplace_back( std::move( p ) );
                }

            for ( auto& p : predictions_sum )
                {
                    predictions[i].emplace_back( std::move( p ) );
                }
        }

    return predictions;
}

// ----------------------------------------------------------------------------

std::vector<containers::Subfeatures> SubtreeHelper::make_subfeatures(
    const decisiontrees::TableHolder& _table_holder,
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

            assert_true( _table_holder.subtables_[i]->main_tables_.size() > 0 );

            const auto rows_map = utils::Mapper::create_rows_map(
                _table_holder.subtables_[i]->main_tables_[0].rows_ptr() );

            const auto& p = _predictions[i];

            for ( size_t j = 0; j < p.size(); ++j )
                {
                    const auto column = containers::Column<Float>(
                        p[j].data(),
                        "FEATURE_" + std::to_string( j + 1 ),
                        p[j].size() );

                    const auto view =
                        containers::ColumnView<Float, std::map<Int, Int>>(
                            column, rows_map );

                    subfeatures[i].push_back( view );
                }
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace relboost*/

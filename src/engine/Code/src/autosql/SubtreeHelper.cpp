#include "autosql/ensemble/ensemble.hpp"

namespace autosql
{
namespace ensemble
{
// ----------------------------------------------------------------------------

std::vector<containers::Predictions> SubtreeHelper::make_predictions(
    const decisiontrees::TableHolder& _table_holder,
    const std::vector<containers::Optional<DecisionTreeEnsemble>>&
        _subfeatures_avg,
    const std::vector<containers::Optional<DecisionTreeEnsemble>>&
        _subfeatures_sum )
{
    const auto size = _table_holder.subtables_.size();

    assert( size == _subfeatures_avg.size() );
    assert( size == _subfeatures_sum.size() );

    std::vector<containers::Predictions> predictions( size );

    for ( size_t i = 0; i < size; ++i )
        {
            if ( !_table_holder.subtables_[i] )
                {
                    continue;
                }

            assert( _table_holder.subtables_[i]->main_tables_.size() > 0 );

            auto impl = containers::Optional<aggregations::AggregationImpl>(
                new aggregations::AggregationImpl(
                    _table_holder.subtables_[i]->main_tables_[0].nrows() ) );

            assert( _subfeatures_avg[i] );

            assert( _subfeatures_sum[i] );

            auto predictions_avg = _subfeatures_avg[i]->transform(
                *_table_holder.subtables_[i], &impl );

            auto predictions_sum = _subfeatures_sum[i]->transform(
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

    assert( size == _predictions.size() );

    auto subfeatures = std::vector<containers::Subfeatures>( size );

    for ( size_t i = 0; i < size; ++i )
        {
            if ( !_table_holder.subtables_[i] )
                {
                    continue;
                }

            assert( _table_holder.subtables_[i]->main_tables_.size() > 0 );

            const auto rows_map = utils::Mapper::create_rows_map(
                _table_holder.subtables_[i]->main_tables_[0].rows_ptr() );

            const auto& p = _predictions[i];

            for ( size_t j = 0; p.size(); ++j )
                {
                    const auto column = containers::Column<AUTOSQL_FLOAT>(
                        p[j].data(),
                        "FEATURE_" + std::to_string( j + 1 ),
                        p[j].size() );

                    const auto view = containers::ColumnView<
                        AUTOSQL_FLOAT,
                        std::map<AUTOSQL_INT, AUTOSQL_INT>>( column, rows_map );

                    subfeatures[i].push_back( view );
                }
        }

    return subfeatures;
}

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql
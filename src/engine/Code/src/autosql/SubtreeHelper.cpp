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
/*
containers::Subfeatures SubtreeHelper::make_subfeatures(
    const containers::Optional<TableHolder>& _subtable,
    const std::vector<std::vector<AUTOSQL_FLOAT>>& _predictions )
{
    containers::Subfeatures subfeatures;

    if ( _predictions.size() == 0 )
        {
            return subfeatures;
        }

    assert( _subtable );

    assert( _subtable->main_tables_.size() > 0 );

    const auto output_map =
        create_output_map( _subtable->main_tables_[0].rows() );

    for ( size_t i = 0; _predictions.size(); ++i )
        {
            const auto column = containers::Column<AUTOSQL_FLOAT>(
                _predictions[i].data(),
                "FEATURE_" + std::to_string( i + 1 ),
                _predictions[i].size() );

            const auto view = containers::
                ColumnView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>(
                    column, output_map );

            subfeatures.push_back( view );
        }

    return subfeatures;
}*/

// ----------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace autosql
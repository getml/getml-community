#include "autosql/decisiontrees/decisiontrees.hpp"

namespace autosql
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<AUTOSQL_INT>>
SubtreeHelper::create_population_indices(
    const size_t _nrows, const AUTOSQL_SAMPLE_CONTAINER& _sample_container )
{
    std::set<AUTOSQL_INT> population_indices;

    for ( auto& sample : _sample_container )
        {
            assert( sample->ix_x_perip >= 0 );

            assert( sample->ix_x_perip < _nrows );

            population_indices.insert( sample->ix_x_perip );
        }

    return std::make_shared<const std::vector<AUTOSQL_INT>>(
        population_indices.begin(), population_indices.end() );
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::map<AUTOSQL_INT, AUTOSQL_INT>>
SubtreeHelper::create_output_map( const std::vector<size_t>& _rows )
{
    auto output_map = std::make_shared<std::map<AUTOSQL_INT, AUTOSQL_INT>>();

    for ( size_t i = 0; i < _rows.size(); ++i )
        {
            ( *output_map )[static_cast<AUTOSQL_INT>( _rows[i] )] =
                static_cast<AUTOSQL_INT>( i );
        }

    return output_map;
}

// ----------------------------------------------------------------------------

std::vector<std::vector<AUTOSQL_FLOAT>> SubtreeHelper::make_predictions(
    const containers::Optional<TableHolder>& _subtable,
    const bool _use_timestamps,
    const std::vector<DecisionTree>& _subtrees )
{
    std::vector<std::vector<AUTOSQL_FLOAT>> predictions;

    if ( !_subtable )
        {
            return predictions;
        }

    assert( _subtable->main_tables_.size() > 0 );

    assert(
        _subtable->main_tables_.size() > _subtable->peripheral_tables_.size() );

    assert( _subtable->main_tables_.size() > _subtable->subtables_.size() );

    containers::Optional<aggregations::AggregationImpl> aggregation_impl(
        new aggregations::AggregationImpl(
            _subtable->main_tables_[0].nrows() ) );

    for ( auto& tree : _subtrees )
        {
            assert( false && "ToDO" );

            // agg.set_aggregation_impl( &aggregation_impl );

            assert( tree.ix_perip_used() < _subtable->main_tables_.size() );

            const auto ix = tree.ix_perip_used();

            assert( false && "ToDO" );

            auto new_prediction = tree.transform(
                _subtable->main_tables_[ix],
                _subtable->peripheral_tables_[ix],
                _subtable->subtables_[ix],
                _use_timestamps,
                nullptr );

            predictions.push_back( new_prediction );
        }

    return predictions;
}

// ----------------------------------------------------------------------------

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
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace autosql
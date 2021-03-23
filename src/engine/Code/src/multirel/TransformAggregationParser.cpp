#include "multirel/aggregations/aggregations.hpp"

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::shared_ptr<AbstractTransformAggregation>
TransformAggregationParser::parse_aggregation(
    const std::string& _aggregation,
    const descriptors::SameUnitsContainer& _same_units_discrete,
    const descriptors::SameUnitsContainer& _same_units_numerical,
    const descriptors::ColumnToBeAggregated& _column_to_be_aggregated,
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const containers::Subfeatures& _subfeatures )
{
    return AggregationParser<AbstractTransformAggregation>::parse_aggregation(
        _aggregation,
        _same_units_discrete,
        _same_units_numerical,
        _column_to_be_aggregated,
        _population,
        _peripheral,
        _subfeatures,
        nullptr,
        nullptr,
        nullptr );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

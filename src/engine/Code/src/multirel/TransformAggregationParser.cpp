#include "multirel/aggregations/aggregations.hpp"

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::shared_ptr<AbstractTransformAggregation>
TransformAggregationParser::parse_aggregation(
    const TransformAggregationParams& _params )
{
    return AggregationParser<
        AbstractTransformAggregation,
        TransformAggregationParams>::parse_aggregation( _params );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

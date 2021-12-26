#include "multirel/aggregations/FitAggregationParser.hpp"

// ----------------------------------------------------------------------------

#include "multirel/aggregations/AggregationParser.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

std::shared_ptr<aggregations::AbstractFitAggregation>
FitAggregationParser::parse_aggregation(const FitAggregationParams& _params) {
  return AggregationParser<AbstractFitAggregation,
                           FitAggregationParams>::parse_aggregation(_params);
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

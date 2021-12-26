#ifndef MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>

// ----------------------------------------------------------------------------

#include "multirel/aggregations/AbstractTransformAggregation.hpp"
#include "multirel/aggregations/TransformAggregationParams.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

struct TransformAggregationParser {
  /// Returns the appropriate aggregation from the aggregation string and
  /// other information.
  /// This is a separate method to make sure that the aggregations are
  /// actually compiled in the aggregation module.
  static std::shared_ptr<AbstractTransformAggregation> parse_aggregation(
      const TransformAggregationParams& _params);
};

// ----------------------------------------------------------------------------

}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_

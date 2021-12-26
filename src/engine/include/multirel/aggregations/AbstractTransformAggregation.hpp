#ifndef MULTIREL_AGGREGATIONS_ABSTRACTTRANSFORMAGGREGATION_HPP_
#define MULTIREL_AGGREGATIONS_ABSTRACTTRANSFORMAGGREGATION_HPP_

// ----------------------------------------------------------------------------

#include <cstddef>
#include <optional>

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace aggregations {
// ----------------------------------------------------------------------------

class AbstractTransformAggregation {
 public:
  AbstractTransformAggregation(){};

  virtual ~AbstractTransformAggregation() = default;

  // --------------------------------------

  /// Returns the aggregated values of the match pointers.
  virtual Float aggregate(
      const containers::MatchPtrs& _match_ptrs, const size_t _skip,
      const std::optional<containers::Column<Float>>& _time_stamp) const = 0;

  /// Moves the match pointers aggregating NULL values to the beginning.
  virtual containers::MatchPtrs::iterator separate_null_values(
      containers::MatchPtrs* _match_ptrs) const = 0;

  // --------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_ABSTRACTTRANSFORMAGGREGATION_HPP_

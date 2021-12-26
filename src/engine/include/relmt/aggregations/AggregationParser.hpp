#ifndef RELMT_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define RELMT_AGGREGATIONS_AGGREGATIONPARSER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>

// ----------------------------------------------------------------------------

#include "relmt/lossfunctions/lossfunctions.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace aggregations {
// ------------------------------------------------------------------------

struct AggregationParser {
  /// Returns the aggregation associated with the type
  static std::shared_ptr<lossfunctions::LossFunction> parse(
      const std::string& _type,
      const std::shared_ptr<lossfunctions::LossFunction>& _child);
};

// ------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_AGGREGATIONS_AGGREGATIONPARSER_HPP_

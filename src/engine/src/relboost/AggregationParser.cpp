#include "relboost/aggregations/AggregationParser.hpp"

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "relboost/aggregations/Avg.hpp"
#include "relboost/aggregations/Sum.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace aggregations {
// ----------------------------------------------------------------------------

std::shared_ptr<lossfunctions::LossFunction> AggregationParser::parse(
    const std::string& _type,
    const std::shared_ptr<lossfunctions::LossFunction>& _child) {
  if (_type == "AVG") {
    return std::make_shared<Avg>(_child);
  } else if (_type == "SUM") {
    return std::make_shared<Sum>(_child);
  } else {
    throw std::runtime_error("Unknown aggregation: '" + _type + "' !");
    return std::shared_ptr<lossfunctions::LossFunction>();
  }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

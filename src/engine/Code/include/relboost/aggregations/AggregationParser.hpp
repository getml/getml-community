#ifndef RELBOOST_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define RELBOOST_AGGREGATIONS_AGGREGATIONPARSER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace aggregations
{
// ------------------------------------------------------------------------

struct AggregationParser
{
    /// Returns the aggregation associated with the type
    static std::shared_ptr<lossfunctions::LossFunction> parse(
        const std::string& _type,
        const std::shared_ptr<lossfunctions::LossFunction>& _child );
};

// ------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_AGGREGATIONS_AGGREGATIONPARSER_HPP_
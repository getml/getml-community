#ifndef RELBOOSTXX_AGGREGATIONS_AGGREGATIONPARSER_HPP_
#define RELBOOSTXX_AGGREGATIONS_AGGREGATIONPARSER_HPP_

// ----------------------------------------------------------------------------

namespace relcit
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
}  // namespace relcit

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_AGGREGATIONS_AGGREGATIONPARSER_HPP_

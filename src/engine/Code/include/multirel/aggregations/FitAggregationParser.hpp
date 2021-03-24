#ifndef MULTIREL_AGGREGATIONS_FITAGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_FITAGGREGATIONPARSER_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

struct FitAggregationParser
{
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    /// This is a separate method to make sure that the aggregations are
    /// actually compiled in the aggregation module.
    static std::shared_ptr<AbstractFitAggregation> parse_aggregation(
        const std::string& _aggregation, const FitAggregationParams& _params );
};

// ----------------------------------------------------------------------------

}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_FITAGGREGATIONPARSER_HPP_

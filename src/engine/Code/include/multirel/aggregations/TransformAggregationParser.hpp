#ifndef MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_
#define MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

struct TransformAggregationParser
{
    /// Returns the appropriate aggregation from the aggregation string and
    /// other information.
    /// This is a separate method to make sure that the aggregations are
    /// actually compiled in the aggregation module.
    static std::shared_ptr<AbstractTransformAggregation> parse_aggregation(
        const std::string& _aggregation,
        const descriptors::SameUnitsContainer& _same_units_discrete,
        const descriptors::SameUnitsContainer& _same_units_numerical,
        const descriptors::ColumnToBeAggregated& _column_to_be_aggregated,
        const containers::DataFrameView& _population,
        const containers::DataFrame& _peripheral,
        const containers::Subfeatures& _subfeatures );
};

// ----------------------------------------------------------------------------

}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARSER_HPP_

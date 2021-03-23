#include "multirel/aggregations/aggregations.hpp"

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

std::shared_ptr<aggregations::AbstractFitAggregation>
FitAggregationParser::parse_aggregation(
    const std::string& _aggregation,
    const descriptors::SameUnitsContainer& _same_units_discrete,
    const descriptors::SameUnitsContainer& _same_units_numerical,
    const descriptors::ColumnToBeAggregated& _column_to_be_aggregated,
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const containers::Subfeatures& _subfeatures,
    const std::shared_ptr<AggregationImpl>& _aggregation_impl,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion>&
        _optimization_criterion,
    containers::Matches* _matches )
{
    return AggregationParser<AbstractFitAggregation>::parse_aggregation(
        _aggregation,
        _same_units_discrete,
        _same_units_numerical,
        _column_to_be_aggregated,
        _population,
        _peripheral,
        _subfeatures,
        _aggregation_impl,
        _optimization_criterion,
        _matches );
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

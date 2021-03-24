#ifndef MULTIREL_AGGREGATIONS_FITAGGREGATIONPARAMS_HPP_
#define MULTIREL_AGGREGATIONS_FITAGGREGATIONPARAMS_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// FitAggregationParams are the parameters used to construct the fit
/// aggregation.
struct FitAggregationParams
{
    /// The impl for the aggregation.
    const std::shared_ptr<AggregationImpl> aggregation_impl;

    /// Describes the column we want to aggregate.
    const descriptors::ColumnToBeAggregated column_to_be_aggregated;

    /// A pointer to the matches used.
    containers::Matches *const matches;

    /// The optimization criterion used.
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
        optimization_criterion;

    /// The population table.
    const containers::DataFrameView population;

    /// The peripheral table.
    const containers::DataFrame peripheral;

    /// Contains descriptors for same-unit-pairs of the discrete columns.
    const descriptors::SameUnitsContainer same_units_discrete;

    /// Contains descriptors for same-unit-pairs of the numerical columns.
    const descriptors::SameUnitsContainer same_units_numerical;

    /// Contains the subfeatures.
    const containers::Subfeatures subfeatures;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_FITAGGREGATIONPARAMS_HPP_

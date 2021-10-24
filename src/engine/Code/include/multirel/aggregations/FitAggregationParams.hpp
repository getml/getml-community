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
    const std::shared_ptr<AggregationImpl> aggregation_impl_;

    /// The type of the aggregation to use.
    const std::string aggregation_type_;

    /// Describes the column we want to aggregate.
    const descriptors::ColumnToBeAggregated column_to_be_aggregated_;

    /// A pointer to the matches used.
    containers::Matches* const matches_ = nullptr;

    /// The optimization criterion used.
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion>
        optimization_criterion_;

    /// The population table.
    const containers::DataFrameView population_;

    /// The peripheral table.
    const containers::DataFrame peripheral_;

    /// Contains descriptors for same-unit-pairs of the discrete columns.
    const descriptors::SameUnitsContainer same_units_discrete_;

    /// Contains descriptors for same-unit-pairs of the numerical columns.
    const descriptors::SameUnitsContainer same_units_numerical_;

    /// Contains the subfeatures.
    const containers::Subfeatures subfeatures_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_FITAGGREGATIONPARAMS_HPP_

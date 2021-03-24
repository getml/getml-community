#ifndef MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARAMS_HPP_
#define MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARAMS_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

/// TransformAggregation are the parameters used to construct the transform
/// aggregation.
struct TransformAggregationParams
{
    /// Describes the column we want to aggregate.
    const descriptors::ColumnToBeAggregated column_to_be_aggregated;

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

#endif  // MULTIREL_AGGREGATIONS_TRANSFORMAGGREGATIONPARAMS_HPP_

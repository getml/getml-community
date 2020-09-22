#ifndef MULTIREL_DECISIONTREES_VALUETOBEAGGREGATEDCREATOR_HPP_
#define MULTIREL_DECISIONTREES_VALUETOBEAGGREGATEDCREATOR_HPP_

namespace multirel
{
namespace decisiontrees
{
// ----------------------------------------------------------------------------

class ValueToBeAggregatedCreator
{
   public:
    /// An aggregation contains a value to be aggregated. This is set by
    /// this function.
    static void create(
        const DecisionTreeImpl &_impl,
        const descriptors::ColumnToBeAggregated &_column_to_be_aggregated,
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        const containers::Subfeatures &_subfeatures,
        aggregations::AbstractAggregation *_aggregation );

   private:
    /// Create the value to be aggregated for same_unit_discrete.
    static void create_same_unit_discrete(
        const DecisionTreeImpl &_impl,
        size_t _ix_column_used,
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        aggregations::AbstractAggregation *_aggregation );

    /// Create the value to be aggregated for same_unit_numerical.
    static void create_same_unit_numerical(
        const DecisionTreeImpl &_impl,
        size_t _ix_column_used,
        const containers::DataFrameView &_population,
        const containers::DataFrame &_peripheral,
        aggregations::AbstractAggregation *_aggregation );
};

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel

#endif  // MULTIREL_DECISIONTREES_VALUETOBEAGGREGATEDCREATOR_HPP_


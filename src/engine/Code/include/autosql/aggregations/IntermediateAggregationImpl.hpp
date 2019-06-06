#ifndef AUTOSQL_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_
#define AUTOSQL_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

struct IntermediateAggregationImpl
{
    // --------------------------------------

    IntermediateAggregationImpl(
        const containers::DataFrameView& _output_table,
        const AggregationIndex& _index,
        optimizationcriteria::OptimizationCriterion* const _parent )
        : count_( std::vector<AUTOSQL_FLOAT>( _output_table.nrows() ) ),
          index_( _index ),
          parent_( _parent ),
          updates_current_( containers::IntSet( _output_table.nrows() ) ),
          updates_stored_( containers::IntSet( _output_table.nrows() ) ),
          yhat_( std::vector<AUTOSQL_FLOAT>( _output_table.nrows() ) ),
          yhat_committed_(
              std::vector<AUTOSQL_FLOAT>( _output_table.nrows() ) ),
          yhat_stored_( std::vector<AUTOSQL_FLOAT>( _output_table.nrows() ) )
    {
    }

    ~IntermediateAggregationImpl() = default;

    // --------------------------------------

    /// Trivial accessor
    const AggregationIndex& index() const { return index_; }

    // --------------------------------------

    /// Vector counts - count_ remains unchanged, so count_stored_ and
    /// count_committed_ is not needed
    std::vector<AUTOSQL_FLOAT> count_;

    /// Used to map ix_input to ix_aggregation
    const AggregationIndex index_;

    /// The parent of this IntermediateAggregation can
    /// be either another aggregation or the final
    /// optimization criterion.
    optimizationcriteria::OptimizationCriterion* const parent_;

    /// Vector sums
    std::vector<AUTOSQL_FLOAT> sum_;

    /// Vector containing sums that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> sum_committed_;

    /// Vector sum_cubed_
    std::vector<AUTOSQL_FLOAT> sum_cubed_;

    /// Vector containing sum_cubed_ that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> sum_cubed_committed_;

    /// Vector sum_squared_
    std::vector<AUTOSQL_FLOAT> sum_squared_;

    /// Vector containing sum_squared_ that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> sum_squared_committed_;

    /// Contains the population_ix of all samples that have been updated
    /// since the last time we had a new critical value. Unlike
    /// updates_stored_, updates_current_ will be cleared every time we get
    /// to a new critical value.
    containers::IntSet updates_current_;

    /// Contains the population_ix of all samples that have been updated
    /// since the last commit. Will be cleared by revert_to_commit(),
    /// commit() or clear.
    containers::IntSet updates_stored_;

    /// Vector containing predictions
    std::vector<AUTOSQL_FLOAT> yhat_;

    /// Vector containing predictions that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> yhat_committed_;

    /// Vector containing predictions that have been
    /// stored, but not yet committed
    std::vector<AUTOSQL_FLOAT> yhat_stored_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace autosql

#endif  // AUTOSQL_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_

#ifndef MULTIREL_AGGREGATIONS_AGGREGATIONIMPL_HPP_
#define MULTIREL_AGGREGATIONS_AGGREGATIONIMPL_HPP_

namespace multirel
{
namespace aggregations
{
// ----------------------------------------------------------------------------

struct AggregationImpl
{
    AggregationImpl( size_t _sample_size )
        : count_( std::vector<Float>( _sample_size ) ),
          count_committed_( std::vector<Float>( _sample_size ) ),
          match_ptr_( std::vector<containers::Match*>( _sample_size ) ),
          match_ptr_committed_(
              std::vector<containers::Match*>( _sample_size ) ),
          sum_( std::vector<Float>( _sample_size ) ),
          sum_committed_( std::vector<Float>( _sample_size ) ),
          sum_cubed_( std::vector<Float>( _sample_size ) ),
          sum_cubed_committed_( std::vector<Float>( _sample_size ) ),
          sum_squared_( std::vector<Float>( _sample_size ) ),
          sum_squared_committed_( std::vector<Float>( _sample_size ) ),
          updates_current_(
              containers::IntSet( static_cast<Int>( _sample_size ) ) ),
          updates_stored_(
              containers::IntSet( static_cast<Int>( _sample_size ) ) ),
          yhat_( std::vector<Float>( _sample_size ) ),
          yhat_committed_( std::vector<Float>( _sample_size ) ),
          yhat_stored_( std::vector<Float>( _sample_size ) )
    {
    }

    ~AggregationImpl() = default;

    // ------------------------------------------------------------

    /// Vector counts
    std::vector<Float> count_;

    /// Vector containing counts that have been
    /// committed
    std::vector<Float> count_committed_;

    /// Vector of pointers to the sample currently in place - needed
    /// by some aggregation like MIN or MAX
    std::vector<containers::Match*> match_ptr_;

    /// Vector of pointers to the sample currently in place (committed)
    std::vector<containers::Match*> match_ptr_committed_;

    /// Vector sums
    std::vector<Float> sum_;

    /// Vector containing sums that have been
    /// committed
    std::vector<Float> sum_committed_;

    /// Vector sum_cubed_
    std::vector<Float> sum_cubed_;

    /// Vector containing sum_cubed_ that have been
    /// committed
    std::vector<Float> sum_cubed_committed_;

    /// Vector sum_squared_
    std::vector<Float> sum_squared_;

    /// Vector containing sum_squared_ that have been
    /// committed
    std::vector<Float> sum_squared_committed_;

    /// Contains the population_ix of all samples that have been updated
    /// since the last time we had a new critical value. Unlike
    /// updates_stored_, updates_current_ will be cleared every time we get
    /// to a new critical value.
    containers::IntSet updates_current_;

    /// Contains the population_ix of all samples that have been updated
    /// since the last commit. Will be cleared by revert_to_commit(),
    /// commit() or clear.
    containers::IntSet updates_stored_;

    /// Value to be aggregated - note the the length is usually different
    /// from yhat
    containers::ColumnView<Float, std::map<Int, Int>> value_to_be_aggregated_;

    /// Value to be aggregated to be used for aggregations that can be
    /// categorical
    containers::ColumnView<Int, std::map<Int, Int>>
        value_to_be_aggregated_categorical_;

    /// Value to be compared - this applies when the value to be aggregated
    /// is a timestamp difference or same unit numerical
    /// Note the the length is usually different from value_to_be_aggregated_,
    /// but always equal to the length of yhat_.
    containers::ColumnView<Float, std::vector<size_t>> value_to_be_compared_;

    /// Vector containing predictions
    std::vector<Float> yhat_;

    /// Vector containing predictions that have been
    /// committed
    std::vector<Float> yhat_committed_;

    /// Vector containing predictions that have been stored
    /// but not committed
    std::vector<Float> yhat_stored_;
};

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace multirel

#endif  // MULTIREL_AGGREGATIONS_AGGREGATIONIMPL_HPP_

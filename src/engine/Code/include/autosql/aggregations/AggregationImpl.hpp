#ifndef AUTOSQL_AGGREGATIONS_AGGREGATIONIMPL_HPP_
#define AUTOSQL_AGGREGATIONS_AGGREGATIONIMPL_HPP_

namespace autosql
{
namespace aggregations
{
// ----------------------------------------------------------------------------

struct AggregationImpl
{
    AggregationImpl( AUTOSQL_INT _sample_size )
        : count_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          count_committed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sample_ptr_( std::vector<Sample*>( _sample_size ) ),
          sample_ptr_committed_( std::vector<Sample*>( _sample_size ) ),
          sum_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sum_committed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sum_cubed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sum_cubed_committed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sum_squared_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          sum_squared_committed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          updates_current_( containers::IntSet( _sample_size ) ),
          updates_stored_( containers::IntSet( _sample_size ) ),
          yhat_( containers::Matrix<AUTOSQL_FLOAT>( _sample_size, 1 ) ),
          yhat_committed_( std::vector<AUTOSQL_FLOAT>( _sample_size ) ),
          yhat_stored_( std::vector<AUTOSQL_FLOAT>( _sample_size ) )
    {
    }

    ~AggregationImpl() = default;

    // ------------------------------------------------------------

    /// Vector counts
    std::vector<AUTOSQL_FLOAT> count_;

    /// Vector containing counts that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> count_committed_;

    /// Vector of pointers to the sample currently in place - needed
    /// by some aggregation like MIN or MAX
    std::vector<Sample*> sample_ptr_;

    /// Vector of pointers to the sample currently in place (committed)
    std::vector<Sample*> sample_ptr_committed_;

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

    /// Value to be aggregated - note the the length is usually different
    /// from yhat
    containers::ColumnView<AUTOSQL_FLOAT, std::map<AUTOSQL_INT, AUTOSQL_INT>>
        value_to_be_aggregated_;

    /// Value to be aggregated to be used for aggregations that can be
    /// categorical
    containers::ColumnView<AUTOSQL_INT, std::map<AUTOSQL_INT, AUTOSQL_INT>>
        value_to_be_aggregated_categorical_;

    /// Value to be compared - this applies when the value to be aggregated
    /// is a timestamp difference or same unit numerical
    /// Note the the length is usually different from value_to_be_aggregated_,
    /// but always equal to the length of yhat_.
    containers::ColumnView<AUTOSQL_FLOAT, std::vector<AUTOSQL_INT>>
        value_to_be_compared_;

    /// Vector containing predictions
    containers::Matrix<AUTOSQL_FLOAT> yhat_;

    /// Vector containing predictions that have been
    /// committed
    std::vector<AUTOSQL_FLOAT> yhat_committed_;

    /// Vector containing predictions that have been stored
    /// but not committed
    std::vector<AUTOSQL_FLOAT> yhat_stored_;
};

// ----------------------------------------------------------------------------
}
}

#endif  // AUTOSQL_AGGREGATIONS_AGGREGATIONIMPL_HPP_

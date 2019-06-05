#ifndef SQLNET_RSQUAREDCRITERION_HPP_
#define SQLNET_RSQUAREDCRITERION_HPP_

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

class RSquaredCriterion : public OptimizationCriterion
{
   public:
    RSquaredCriterion( const SQLNET_FLOAT _min_num_samples );

    ~RSquaredCriterion() = default;

    // --------------------------------------

    /// Sorts a specific subsection of the values defined by _begin and _end.
    /// Returns the indices from greatest to smallest. This is useful for
    /// combining categories.
    std::vector<SQLNET_INT> argsort(
        const SQLNET_INT _begin, const SQLNET_INT _end ) const final;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    SQLNET_INT find_maximum() final;

    /// Calculates statistics that have to be calculated only once
    void init(
        containers::Matrix<SQLNET_FLOAT>& _y,
        containers::Matrix<SQLNET_FLOAT>& _sample_weights ) final;

    /// Needed for numeric stability
    void init_yhat(
        const containers::Matrix<SQLNET_FLOAT>& _yhat,
        const containers::IntSet& _indices ) final;

    /// Updates all samples designated by _indices
    void update_samples(
        const containers::IntSet& _indices,
        const containers::Matrix<SQLNET_FLOAT>& _new_values,
        const std::vector<SQLNET_FLOAT>& _old_values ) final;

    // --------------------------------------

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    void commit() final { impl().commit( sufficient_statistics_committed_ ); }

    /// Because we do not know the number of categories a priori,
    /// we have to extend it, when necessary
    void extend_storage_size( SQLNET_INT _storage_size_extension ) final
    {
        impl().extend_storage_size(
            _storage_size_extension, sufficient_statistics_current_.ncols() );
    }

    /// Resets sufficient statistics to zero
    void reset() final
    {
        impl().reset(
            sufficient_statistics_current_, sufficient_statistics_committed_ );
    }

    /// Reverts to the committed version
    void revert_to_commit() final
    {
        std::copy(
            sufficient_statistics_committed_.begin(),
            sufficient_statistics_committed_.end(),
            sufficient_statistics_current_.begin() );
    }

#ifdef SQLNET_PARALLEL

    /// Trivial setter
    void set_comm( SQLNET_COMMUNICATOR* _comm ) final
    {
        comm_ = _comm;
        impl().set_comm( _comm );
    }

#endif  // SQLNET_PARALLEL

    /// Resizes the sufficient_statistics_stored_ and values_stored_.
    /// The size is inferred from the number of columns in
    /// sufficient_statistics_committed_.
    /// Also resets storage_ix_ to zero.
    void set_storage_size( const SQLNET_INT _storage_size ) final
    {
        impl().set_storage_size(
            _storage_size, sufficient_statistics_current_.ncols() );
    }

    /// Trivial accessor.
    const SQLNET_INT storage_ix() const final { return impl().storage_ix(); }

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const SQLNET_FLOAT _num_samples_smaller,
        const SQLNET_FLOAT _num_samples_greater ) final
    {
        impl().store_current_stage(
            _num_samples_smaller,
            _num_samples_greater,
            sufficient_statistics_current_ );
    }

    /// Trivial getter
    SQLNET_FLOAT value() final { return impl().value(); }

    /// Trivial getter
    SQLNET_FLOAT values_stored( const SQLNET_INT _i ) final
    {
        return impl().values_stored( _i );
    }

    // --------------------------------------

   private:
    /// Implements the formula for calculating R squared.
    SQLNET_FLOAT calculate_r_squared(
        const SQLNET_INT _i,
        const containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics ) const;

    // --------------------------------------

   private:
    OptimizationCriterionImpl& impl() { return impl_; }

    const OptimizationCriterionImpl& impl() const { return impl_; }

    // --------------------------------------

   private:
#ifdef SQLNET_PARALLEL

    /// MPI communicator
    SQLNET_COMMUNICATOR* comm_;

#endif  // SQLNET_PARALLEL

    /// Implementation class for common methods
    /// among optimization criteria.
    OptimizationCriterionImpl impl_;

    /// Minimum number of samples required to be left
    /// on the resulting leaves for a split to occur
    const SQLNET_FLOAT min_num_samples_;

    /// Stores the weights associated which each sample (which is needed
    /// for a random-forest- or boosting-like approach)
    containers::Matrix<SQLNET_FLOAT> sample_weights_;

    /// Stores the sufficient statistics after commit(...)
    /// is called
    containers::Matrix<SQLNET_FLOAT> sufficient_statistics_committed_;

    /// The current sufficient statistics, which can be changed by
    /// update_sample(...) or revert_to_commit()
    containers::Matrix<SQLNET_FLOAT> sufficient_statistics_current_;

    /// Total sum of the weighted samples
    SQLNET_FLOAT sum_sample_weights_;

    /// Cross-correlations of all the centered target values y - length is
    /// y.ncols() * (y.ncols() + 1) / 2
    containers::Matrix<SQLNET_FLOAT> sum_y_centered_y_centered_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    SQLNET_FLOAT* sum_y_centered_yhat_committed_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    SQLNET_FLOAT* sum_y_centered_yhat_current_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    SQLNET_FLOAT* sum_yhat_committed_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    SQLNET_FLOAT* sum_yhat_current_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    SQLNET_FLOAT* sum_yhat_yhat_committed_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    SQLNET_FLOAT* sum_yhat_yhat_current_;

    /// Targets of our predicton task
    containers::Matrix<SQLNET_FLOAT> y_;

    // Target values substracted by their mean (for numerical
    // stability)
    containers::Matrix<SQLNET_FLOAT> y_centered_;

    // Mean of all yhats - for numeric stability. Calculated in
    // init_yhat(...)
    SQLNET_FLOAT y_hat_mean_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql
#endif  // SQLNET_RSQUAREDCRITERION_HPP_

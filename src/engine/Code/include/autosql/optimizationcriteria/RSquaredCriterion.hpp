#ifndef AUTOSQL_RSQUAREDCRITERION_HPP_
#define AUTOSQL_RSQUAREDCRITERION_HPP_

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

class RSquaredCriterion : public OptimizationCriterion
{
   public:
    RSquaredCriterion( const AUTOSQL_FLOAT _min_num_samples );

    ~RSquaredCriterion() = default;

    // --------------------------------------

    /// Sorts a specific subsection of the values defined by _begin and _end.
    /// Returns the indices from greatest to smallest. This is useful for
    /// combining categories.
    std::vector<AUTOSQL_INT> argsort(
        const AUTOSQL_INT _begin, const AUTOSQL_INT _end ) const final;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    AUTOSQL_INT find_maximum() final;

    /// Calculates statistics that have to be calculated only once
    void init(
        const std::vector<std::vector<AUTOSQL_FLOAT>>& _y,
        const std::vector<AUTOSQL_FLOAT>& _sample_weights ) final;

    /// Needed for numeric stability
    void init_yhat(
        const std::vector<AUTOSQL_FLOAT>& _yhat,
        const containers::IntSet& _indices ) final;

    /// Updates all samples designated by _indices
    void update_samples(
        const containers::IntSet& _indices,
        const std::vector<AUTOSQL_FLOAT>& _new_values,
        const std::vector<AUTOSQL_FLOAT>& _old_values ) final;

    // --------------------------------------

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    void commit() final { impl().commit( &sufficient_statistics_committed_ ); }

    /// Resets sufficient statistics to zero
    void reset() final
    {
        impl().reset(
            &sufficient_statistics_current_,
            &sufficient_statistics_committed_ );
    }

    /// Reverts to the committed version
    void revert_to_commit() final
    {
        std::copy(
            sufficient_statistics_committed_.begin(),
            sufficient_statistics_committed_.end(),
            sufficient_statistics_current_.begin() );
    }

    /// Trivial setter
    void set_comm( multithreading::Communicator* _comm ) final
    {
        comm_ = _comm;
        impl().set_comm( _comm );
    }

    /// Trivial accessor.
    const AUTOSQL_INT storage_ix() const final { return impl().storage_ix(); }

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater ) final
    {
        impl().store_current_stage(
            _num_samples_smaller,
            _num_samples_greater,
            sufficient_statistics_current_ );
    }

    /// Trivial getter
    AUTOSQL_FLOAT value() final { return impl().value(); }

    /// Trivial getter
    AUTOSQL_FLOAT values_stored( const size_t _i ) final
    {
        return impl().values_stored( _i );
    }

    // --------------------------------------

   private:
    /// Implements the formula for calculating R squared.
    AUTOSQL_FLOAT calculate_r_squared(
        const size_t _i,
        const std::deque<std::vector<AUTOSQL_FLOAT>>& _sufficient_statistics )
        const;

    // --------------------------------------

   private:
    OptimizationCriterionImpl& impl() { return impl_; }

    const OptimizationCriterionImpl& impl() const { return impl_; }

    // --------------------------------------

   private:
    /// Communicator
    multithreading::Communicator* comm_;

    /// Implementation class for common methods
    /// among optimization criteria.
    OptimizationCriterionImpl impl_;

    /// Minimum number of samples required to be left
    /// on the resulting leaves for a split to occur
    const AUTOSQL_FLOAT min_num_samples_;

    /// Stores the weights associated which each sample (which is needed
    /// for a random-forest- or boosting-like approach)
    std::vector<AUTOSQL_FLOAT> sample_weights_;

    /// Stores the sufficient statistics after commit(...)
    /// is called
    std::vector<AUTOSQL_FLOAT> sufficient_statistics_committed_;

    /// The current sufficient statistics, which can be changed by
    /// update_sample(...) or revert_to_commit()
    std::vector<AUTOSQL_FLOAT> sufficient_statistics_current_;

    /// Total sum of the weighted samples
    AUTOSQL_FLOAT sum_sample_weights_;

    /// Cross-correlations of all the centered target values y - length is
    /// y.ncols() * (y.ncols() + 1) / 2
    std::vector<AUTOSQL_FLOAT> sum_y_centered_y_centered_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    AUTOSQL_FLOAT* sum_y_centered_yhat_committed_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    AUTOSQL_FLOAT* sum_y_centered_yhat_current_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    AUTOSQL_FLOAT* sum_yhat_committed_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    AUTOSQL_FLOAT* sum_yhat_current_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    AUTOSQL_FLOAT* sum_yhat_yhat_committed_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    AUTOSQL_FLOAT* sum_yhat_yhat_current_;

    /// Targets of our predicton task
    std::vector<std::vector<AUTOSQL_FLOAT>> y_;

    // Target values substracted by their mean (for numerical
    // stability)
    std::vector<std::vector<AUTOSQL_FLOAT>> y_centered_;

    // Mean of all yhats - for numeric stability. Calculated in
    // init_yhat(...)
    AUTOSQL_FLOAT y_hat_mean_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql
#endif  // AUTOSQL_RSQUAREDCRITERION_HPP_

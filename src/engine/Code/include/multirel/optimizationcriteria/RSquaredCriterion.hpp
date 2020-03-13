#ifndef MULTIREL_RSQUAREDCRITERION_HPP_
#define MULTIREL_RSQUAREDCRITERION_HPP_

namespace multirel
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

class RSquaredCriterion : public OptimizationCriterion
{
   public:
    RSquaredCriterion(
        const std::shared_ptr<const descriptors::Hyperparameters>&
            _hyperparameters,
        const std::string& _loss_function_type,
        const containers::DataFrameView& _main_table,
        multithreading::Communicator* _comm );

    ~RSquaredCriterion() = default;

    // --------------------------------------

    /// Sorts a specific subsection of the values defined by _begin and _end.
    /// Returns the indices from greatest to smallest. This is useful for
    /// combining categories.
    std::vector<size_t> argsort(
        const size_t _begin, const size_t _end ) const final;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    Int find_maximum() final;

    /// Calculates statistics that have to be calculated only once
    void init( const std::vector<Float>& _sample_weights ) final;

    /// Needed for numeric stability
    void init_yhat(
        const std::vector<Float>& _yhat,
        const containers::IntSet& _indices ) final;

    /// Updates all samples designated by _indices
    void update_samples(
        const containers::IntSet& _indices,
        const std::vector<Float>& _new_values,
        const std::vector<Float>& _old_values ) final;

    // --------------------------------------

    /// Calculates the residuals
    void calc_residuals() final { impl().calc_residuals( &y_ ); }

    /// Calculates the sampling rate.
    void calc_sampling_rate() final { impl().calc_sampling_rate(); }

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    void commit() final { impl().commit( &sufficient_statistics_committed_ ); }

    /// Generates a new set of sample weights.
    std::shared_ptr<std::vector<Float>> make_sample_weights() final
    {
        return impl().make_sample_weights();
    }

    /// Resets sufficient statistics to zero
    void reset() final
    {
        impl().reset(
            &sufficient_statistics_current_,
            &sufficient_statistics_committed_ );
    }

    /// Resets the storage size to zero.
    void reset_storage_size() final { impl().reset_storage_size(); }

    /// Resets yhat_old to the initial value.
    void reset_yhat_old() final { impl().reset_yhat_old(); }

    /// Reverts to the committed version
    void revert_to_commit() final
    {
        assert_true(
            sufficient_statistics_current_.size() ==
            sufficient_statistics_committed_.size() );
        std::copy(
            sufficient_statistics_committed_.begin(),
            sufficient_statistics_committed_.end(),
            sufficient_statistics_current_.begin() );
    }

    /// Trivial accessor.
    const size_t storage_ix() const final { return impl().storage_ix(); }

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const Float _num_samples_smaller,
        const Float _num_samples_greater ) final
    {
        impl().store_current_stage(
            _num_samples_smaller,
            _num_samples_greater,
            sufficient_statistics_current_ );
    }

    /// Updates yhat_old based on _yhat_new.
    void update_yhat_old(
        const std::vector<Float>& _sample_weights,
        const std::vector<Float>& _yhat_new ) final
    {
        impl().update_yhat_old( y_, _sample_weights, _yhat_new );
    }

    /// Trivial getter
    Float value() final { return impl().value(); }

    /// Trivial getter
    Float values_stored( const size_t _i ) final
    {
        return impl().values_stored( _i );
    }

    // --------------------------------------

   private:
    /// Implements the formula for calculating R squared.
    Float calculate_r_squared(
        const size_t _i,
        const std::deque<std::vector<Float>>& _sufficient_statistics ) const;

    // --------------------------------------

   private:
    OptimizationCriterionImpl& impl() { return impl_; }

    const OptimizationCriterionImpl& impl() const { return impl_; }

    // --------------------------------------

   private:
    /// Communicator
    multithreading::Communicator* comm_;

    /// Shared ptr to the hyperparameters.
    std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

    /// Implementation class for common methods
    /// among optimization criteria.
    OptimizationCriterionImpl impl_;

    /// Stores the weights associated which each sample (which is needed
    /// for a random-forest- or boosting-like approach)
    std::vector<Float> sample_weights_;

    /// Stores the sufficient statistics after commit(...)
    /// is called
    std::vector<Float> sufficient_statistics_committed_;

    /// The current sufficient statistics, which can be changed by
    /// update_sample(...) or revert_to_commit()
    std::vector<Float> sufficient_statistics_current_;

    /// Total sum of the weighted samples
    Float sum_sample_weights_;

    /// Cross-correlations of all the centered target values y - length is
    /// y.ncols() * (y.ncols() + 1) / 2
    std::vector<Float> sum_y_centered_y_centered_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    Float* sum_y_centered_yhat_committed_;

    /// Correlations of centered target values y and yhat - length is
    /// the number of columns in y  ( = y.ncols() )
    Float* sum_y_centered_yhat_current_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    Float* sum_yhat_committed_;

    /// Sum of all yhats - by construction, yhat
    /// is a vector
    Float* sum_yhat_current_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    Float* sum_yhat_yhat_committed_;

    /// Sum of all yhats squared - by construction, yhat
    /// is a vector
    Float* sum_yhat_yhat_current_;

    /// Targets of our predicton task
    std::vector<std::vector<Float>> y_;

    // Target values substracted by their mean (for numerical
    // stability)
    std::vector<std::vector<Float>> y_centered_;

    // Mean of all yhats - for numeric stability. Calculated in
    // init_yhat(...)
    Float y_hat_mean_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace multirel
#endif  // MULTIREL_RSQUAREDCRITERION_HPP_

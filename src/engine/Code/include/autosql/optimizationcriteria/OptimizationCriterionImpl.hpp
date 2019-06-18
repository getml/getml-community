#ifndef AUTOSQL_OPTIMIZATIONCRITERIONIMPL_HPP_
#define AUTOSQL_OPTIMIZATIONCRITERIONIMPL_HPP_

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

class OptimizationCriterionImpl
{
   public:
    OptimizationCriterionImpl(
        const std::shared_ptr<const descriptors::Hyperparameters>&
            _hyperparameters,
        const std::string& _loss_function_type,
        const containers::DataFrameView& _main_table,
        multithreading::Communicator* _comm );

    ~OptimizationCriterionImpl() = default;

    // --------------------------------------

    /// Commits the current stage, accepting it as the new state of the
    /// tree
    void commit( std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed );

    /// Resets sufficient statistics to zero
    void reset(
        std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_current,
        std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed );

    /// Returns the sum of all sufficient statistics stored in the individual
    /// processes
    std::deque<std::vector<AUTOSQL_FLOAT>> reduce_sufficient_statistics_stored()
        const;

    /// Reverts to the committed version
    void revert_to_commit();

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater,
        const std::vector<AUTOSQL_FLOAT>& _sufficient_statistics_current );

    /// Updates yhat_old based on _yhat_new.
    void update_yhat_old(
        const std::vector<AUTOSQL_FLOAT>& _sample_weights,
        const std::vector<AUTOSQL_FLOAT>& _yhat_new );

    // --------------------------------------

    /// Calculates the residuals.
    void calc_residuals()
    {
        residuals_ =
            loss_function()->calculate_residuals( yhat_old_, main_table_ );
    }

    /// Calculates the sampling rate.
    void calc_sampling_rate()
    {
        assert( comm_ != nullptr );
        assert( hyperparameters_ );
        sampler_.calc_sampling_rate(
            main_table_.nrows(), hyperparameters_->sampling_factor_, comm_ );
    }

    /// Generates a new set of sample weights.
    std::shared_ptr<std::vector<AUTOSQL_FLOAT>> make_sample_weights()
    {
        return sampler_.make_sample_weights( main_table_.nrows() );
    }

    /// Returns a const reference to the residuals.
    const std::vector<std::vector<AUTOSQL_FLOAT>>& residuals() const
    {
        return residuals_;
    }

    /// Resets the storage size to zero.
    void reset_storage_size()
    {
        max_ix_ = -1;
        sufficient_statistics_stored_.clear();
        values_stored_.clear();
    }

    /// Sets the indicator of the best split
    inline void set_max_ix( const AUTOSQL_INT _max_ix ) { max_ix_ = _max_ix; }

    /// Returns the current storage_ix.
    inline const size_t storage_ix() const
    {
        return sufficient_statistics_stored_.size();
    }

    /// Trivial getter
    inline AUTOSQL_FLOAT value() { return value_; }

    /// Trivial getter
    inline AUTOSQL_FLOAT values_stored( const size_t _i )
    {
        if ( _i < storage_ix() )
            {
                assert( _i < values_stored().size() );

                return values_stored()[_i];
            }
        else
            {
                return 0.0;
            }
    }

    /// Trivial getter
    inline std::vector<AUTOSQL_FLOAT>& values_stored()
    {
        return values_stored_;
    }

    // --------------------------------------

   private:
    /// Trivial (private) accessor
    inline lossfunctions::LossFunction* loss_function()
    {
        assert( loss_function_ );
        return loss_function_.get();
    }

    /// Trivial (private) accessor
    inline std::deque<std::vector<AUTOSQL_FLOAT>>&
    sufficient_statistics_stored()
    {
        return sufficient_statistics_stored_;
    }

    /// Trivial (private) accessor
    inline const std::deque<std::vector<AUTOSQL_FLOAT>>&
    sufficient_statistics_stored() const
    {
        return sufficient_statistics_stored_;
    }

    // --------------------------------------

   private:
    /// Multithreading communicator
    multithreading::Communicator* comm_;

    /// The hyperparameters used to train the model.
    std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

    /// The loss function used.
    std::shared_ptr<lossfunctions::LossFunction> loss_function_;

    /// The main table containing the targets..
    containers::DataFrameView main_table_;

    /// Indicates the best split.
    AUTOSQL_INT max_ix_;

    /// The derivatives of the loss function for the current prediction.
    std::vector<std::vector<AUTOSQL_FLOAT>> residuals_;

    /// For creating the sample weights
    utils::Sampler sampler_;

    /// Stores the sufficient statistics when store_current_stage(...)
    /// is called
    std::deque<std::vector<AUTOSQL_FLOAT>> sufficient_statistics_stored_;

    /// Value of the optimization criterion of the currently
    /// committed stage
    AUTOSQL_FLOAT value_;

    /// Stores the values calculated by find maximum. Can be resized
    /// by set_storage_size
    std::vector<AUTOSQL_FLOAT> values_stored_;

    /// The current predictions generated by the  previous features.
    std::vector<std::vector<AUTOSQL_FLOAT>> yhat_old_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // AUTOSQL_OPTIMIZATIONCRITERIONIMPL_HPP_

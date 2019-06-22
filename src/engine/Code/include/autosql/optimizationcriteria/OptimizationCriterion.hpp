#ifndef AUTOSQL_OPTIMIZATIONCRITERION_HPP_
#define AUTOSQL_OPTIMIZATIONCRITERION_HPP_

namespace autosql
{
namespace optimizationcriteria
{
// ----------------------------------------------------------------------------

class OptimizationCriterion
{
   public:
    OptimizationCriterion() {}

    virtual ~OptimizationCriterion() = default;

    // --------------------------------------

    /// Sorts a specific subsection of the values defined by _begin and _end.
    /// Returns the indices from greatest to smallest. This is useful for
    /// combining categories.
    virtual std::vector<Int> argsort(
        const Int _begin, const Int _end ) const = 0;

    /// Calculates the residuals
    virtual void calc_residuals() = 0;

    /// Calculates the sampling rate.
    virtual void calc_sampling_rate() = 0;

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    virtual void commit() = 0;

    /// Calculates statistics that have to be calculated only once
    virtual void init( const std::vector<Float>& _sample_weights ) = 0;

    /// Some optimization criteria need this for numeric stability
    virtual void init_yhat(
        const std::vector<Float>& _yhat,
        const containers::IntSet& _indices ) = 0;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    virtual Int find_maximum() = 0;

    /// Generates a new set of sample weights.
    virtual std::shared_ptr<std::vector<Float>>
    make_sample_weights() = 0;

    /// Resets sufficient statistics to zero
    virtual void reset() = 0;

    /// Resets the storage size to zero.
    virtual void reset_storage_size() = 0;

    /// Resets yhat_old to the initial value.
    virtual void reset_yhat_old() = 0;

    /// Reverts to the committed version
    virtual void revert_to_commit() = 0;

    /// An intermediate aggregation has no storage, so it
    /// is redelegated to the parent.
    virtual const Int storage_ix() const = 0;

    /// Stores the current stage of the sufficient statistics
    virtual void store_current_stage(
        const Float _num_samples_smaller,
        const Float _num_samples_greater ) = 0;

    /// Updates all samples designated by _indices
    virtual void update_samples(
        const containers::IntSet& _indices,
        const std::vector<Float>& _new_values,
        const std::vector<Float>& _old_values ) = 0;

    /// Updates yhat_old based on _yhat_new.
    virtual void update_yhat_old(
        const std::vector<Float>& _sample_weights,
        const std::vector<Float>& _yhat_new ) = 0;

    /// Trivial getter
    virtual Float value() = 0;

    /// Trivial getter
    virtual Float values_stored( const size_t _i ) = 0;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // AUTOSQL_OPTIMIZATIONCRITERION_HPP_

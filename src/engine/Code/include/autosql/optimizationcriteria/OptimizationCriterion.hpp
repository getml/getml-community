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
    virtual std::vector<AUTOSQL_INT> argsort(
        const AUTOSQL_INT _begin, const AUTOSQL_INT _end ) const = 0;

    /// Calculates the residuals
    virtual void calc_residuals() = 0;

    /// Calculates the sampling rate.
    virtual void calc_sampling_rate() = 0;

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    virtual void commit() = 0;

    /// Calculates statistics that have to be calculated only once
    virtual void init( const std::vector<AUTOSQL_FLOAT>& _sample_weights ) = 0;

    /// Some optimization criteria need this for numeric stability
    virtual void init_yhat(
        const std::vector<AUTOSQL_FLOAT>& _yhat,
        const containers::IntSet& _indices ) = 0;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    virtual AUTOSQL_INT find_maximum() = 0;

    /// Generates a new set of sample weights.
    virtual std::shared_ptr<std::vector<AUTOSQL_FLOAT>>
    make_sample_weights() = 0;

    /// Resets sufficient statistics to zero
    virtual void reset() = 0;

    /// Resets the storage size to zero.
    virtual void reset_storage_size() = 0;

    /// Reverts to the committed version
    virtual void revert_to_commit() = 0;

    /// An intermediate aggregation has no storage, so it
    /// is redelegated to the parent.
    virtual const AUTOSQL_INT storage_ix() const = 0;

    /// Stores the current stage of the sufficient statistics
    virtual void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater ) = 0;

    /// Updates all samples designated by _indices
    virtual void update_samples(
        const containers::IntSet& _indices,
        const std::vector<AUTOSQL_FLOAT>& _new_values,
        const std::vector<AUTOSQL_FLOAT>& _old_values ) = 0;

    /// Updates yhat_old based on _yhat_new.
    virtual void update_yhat_old(
        const std::vector<AUTOSQL_FLOAT>& _sample_weights,
        const std::vector<AUTOSQL_FLOAT>& _yhat_new ) = 0;

    /// Trivial getter
    virtual AUTOSQL_FLOAT value() = 0;

    /// Trivial getter
    virtual AUTOSQL_FLOAT values_stored( const size_t _i ) = 0;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // AUTOSQL_OPTIMIZATIONCRITERION_HPP_

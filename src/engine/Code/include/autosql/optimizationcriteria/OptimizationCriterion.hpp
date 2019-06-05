#ifndef SQLNET_OPTIMIZATIONCRITERION_HPP_
#define SQLNET_OPTIMIZATIONCRITERION_HPP_

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
    virtual std::vector<SQLNET_INT> argsort(
        const SQLNET_INT _begin, const SQLNET_INT _end ) const = 0;

    /// Commits the current stage, accepting it as the new state of the
    /// optimization criterion
    virtual void commit() = 0;

    /// Because we do not know the number of categories a priori,
    /// we have to extend it, when necessary
    virtual void extend_storage_size( SQLNET_INT _storage_size_extension ) = 0;

    /// Calculates statistics that have to be calculated only once
    virtual void init(
        containers::Matrix<SQLNET_FLOAT>& _y,
        containers::Matrix<SQLNET_FLOAT>& _sample_weights ) = 0;

    /// Some optimization criteria need this for numeric stability
    virtual void init_yhat(
        const containers::Matrix<SQLNET_FLOAT>& _yhat,
        const containers::IntSet& _indices ) = 0;

    /// Finds the index associated with the maximum of the optimization
    /// criterion
    virtual SQLNET_INT find_maximum() = 0;

    /// Resets sufficient statistics to zero
    virtual void reset() = 0;

    /// Reverts to the committed version
    virtual void revert_to_commit() = 0;

#ifdef SQLNET_PARALLEL

    /// Trivial setter
    virtual void set_comm( SQLNET_COMMUNICATOR* _comm ) = 0;

#endif  // SQLNET_PARALLEL

    /// Resizes the sufficient_statistics_stored_ and values_stored_.
    /// The size is inferred from the number of columns in
    /// sufficient_statistics_committed_.
    /// Also resets storage_ix_ to zero.
    virtual void set_storage_size( const SQLNET_INT _storage_size ) = 0;

    /// An intermediate aggregation has no storage, so it
    /// is redelegated to the parent.
    virtual const SQLNET_INT storage_ix() const = 0;

    /// Stores the current stage of the sufficient statistics
    virtual void store_current_stage(
        const SQLNET_FLOAT _num_samples_smaller,
        const SQLNET_FLOAT _num_samples_greater ) = 0;

    /// Updates all samples designated by _indices
    virtual void update_samples(
        const containers::IntSet& _indices,
        const containers::Matrix<SQLNET_FLOAT>& _new_values,
        const std::vector<SQLNET_FLOAT>& _old_values ) = 0;

    /// Trivial getter
    virtual SQLNET_FLOAT value() = 0;

    /// Trivial getter
    virtual SQLNET_FLOAT values_stored( const SQLNET_INT _i ) = 0;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // SQLNET_OPTIMIZATIONCRITERION_HPP_

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
    OptimizationCriterionImpl();

    ~OptimizationCriterionImpl() = default;

    // --------------------------------------

    /// Commits the current stage, accepting it as the new state of the
    /// tree
    void commit(
        containers::Matrix<AUTOSQL_FLOAT>& _sufficient_statistics_committed );

    /// Because we do not know the number of categories a priori,
    /// we have to extend it, when necessary
    void extend_storage_size(
        AUTOSQL_INT _storage_size_extension,
        AUTOSQL_INT _sufficient_statistics_ncols );

    /// Resets sufficient statistics to zero
    void reset(
        containers::Matrix<AUTOSQL_FLOAT>& _sufficient_statistics_current,
        containers::Matrix<AUTOSQL_FLOAT>& _sufficient_statistics_committed );

    /// Returns the sum of all sufficient statistics stored in the individual
    /// processes
    containers::Matrix<AUTOSQL_FLOAT> reduce_sufficient_statistics_stored()
        const;

    /// Reverts to the committed version
    void revert_to_commit();

    /// Resizes the sufficient_statistics_stored_ and values_stored_.
    /// The size is inferred from the number of columns in
    /// sufficient_statistics_committed_.
    /// Also resets storage_ix_ to zero.
    void set_storage_size(
        AUTOSQL_INT _storage_size, AUTOSQL_INT _sufficient_statistics_ncols );

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater,
        containers::Matrix<AUTOSQL_FLOAT> _sufficient_statistics_current );

    // --------------------------------------

#ifdef AUTOSQL_PARALLEL

    /// Trivial setter
    inline void set_comm( AUTOSQL_COMMUNICATOR* _comm ) { comm_ = _comm; }

#endif  // AUTOSQL_PARALLEL

    /// Sets the indicator of the best split
    inline void set_max_ix( const AUTOSQL_INT _max_ix ) { max_ix_ = _max_ix; }

    /// Returns the current storage_ix
    inline const AUTOSQL_INT storage_ix() const { return storage_ix_; }

    /// Trivial getter
    inline AUTOSQL_FLOAT value() { return value_; }

    /// Trivial getter
    inline AUTOSQL_FLOAT values_stored( const AUTOSQL_INT _i )
    {
        assert( _i >= 0 );
        assert(
            storage_ix_ <= static_cast<AUTOSQL_INT>( values_stored().size() ) );

        if ( _i < storage_ix_ )
            {
                return values_stored()[_i];
            }
        else
            {
                return 0.0;
            }
    }

    /// Trivial getter
    inline containers::Matrix<AUTOSQL_FLOAT>& values_stored()
    {
        return values_stored_;
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    inline containers::Matrix<AUTOSQL_FLOAT>& sufficient_statistics_stored()
    {
        return sufficient_statistics_stored_;
    }

    /// Trivial accessor
    inline const containers::Matrix<AUTOSQL_FLOAT>&
    sufficient_statistics_stored() const
    {
        return sufficient_statistics_stored_;
    }

    // --------------------------------------

   private:
#ifdef AUTOSQL_PARALLEL

    /// MPI communicator
    AUTOSQL_COMMUNICATOR* comm_;

#endif  // AUTOSQL_PARALLEL

    /// Indicates the best split.
    AUTOSQL_INT max_ix_;

    /// Stores the sufficient statistics when store_current_stage(...)
    /// is called
    containers::Matrix<AUTOSQL_FLOAT> sufficient_statistics_stored_;

    /// Indicates how many times something has already been written
    /// into the the storage after the last time set_storage_size(...)
    /// has been called
    AUTOSQL_INT storage_ix_;

    /// Value of the optimization criterion of the currently
    /// committed stage
    AUTOSQL_FLOAT value_;

    /// Stores the values calculated by find maximum. Can be resized
    /// by set_storage_size
    containers::Matrix<AUTOSQL_FLOAT> values_stored_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // AUTOSQL_OPTIMIZATIONCRITERIONIMPL_HPP_

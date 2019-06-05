#ifndef SQLNET_OPTIMIZATIONCRITERIONIMPL_HPP_
#define SQLNET_OPTIMIZATIONCRITERIONIMPL_HPP_

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
        containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_committed );

    /// Because we do not know the number of categories a priori,
    /// we have to extend it, when necessary
    void extend_storage_size(
        SQLNET_INT _storage_size_extension,
        SQLNET_INT _sufficient_statistics_ncols );

    /// Resets sufficient statistics to zero
    void reset(
        containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_current,
        containers::Matrix<SQLNET_FLOAT>& _sufficient_statistics_committed );

    /// Returns the sum of all sufficient statistics stored in the individual
    /// processes
    containers::Matrix<SQLNET_FLOAT> reduce_sufficient_statistics_stored()
        const;

    /// Reverts to the committed version
    void revert_to_commit();

    /// Resizes the sufficient_statistics_stored_ and values_stored_.
    /// The size is inferred from the number of columns in
    /// sufficient_statistics_committed_.
    /// Also resets storage_ix_ to zero.
    void set_storage_size(
        SQLNET_INT _storage_size, SQLNET_INT _sufficient_statistics_ncols );

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const SQLNET_FLOAT _num_samples_smaller,
        const SQLNET_FLOAT _num_samples_greater,
        containers::Matrix<SQLNET_FLOAT> _sufficient_statistics_current );

    // --------------------------------------

#ifdef SQLNET_PARALLEL

    /// Trivial setter
    inline void set_comm( SQLNET_COMMUNICATOR* _comm ) { comm_ = _comm; }

#endif  // SQLNET_PARALLEL

    /// Sets the indicator of the best split
    inline void set_max_ix( const SQLNET_INT _max_ix ) { max_ix_ = _max_ix; }

    /// Returns the current storage_ix
    inline const SQLNET_INT storage_ix() const { return storage_ix_; }

    /// Trivial getter
    inline SQLNET_FLOAT value() { return value_; }

    /// Trivial getter
    inline SQLNET_FLOAT values_stored( const SQLNET_INT _i )
    {
        assert( _i >= 0 );
        assert(
            storage_ix_ <= static_cast<SQLNET_INT>( values_stored().size() ) );

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
    inline containers::Matrix<SQLNET_FLOAT>& values_stored()
    {
        return values_stored_;
    }

    // --------------------------------------

   private:
    /// Trivial accessor
    inline containers::Matrix<SQLNET_FLOAT>& sufficient_statistics_stored()
    {
        return sufficient_statistics_stored_;
    }

    /// Trivial accessor
    inline const containers::Matrix<SQLNET_FLOAT>&
    sufficient_statistics_stored() const
    {
        return sufficient_statistics_stored_;
    }

    // --------------------------------------

   private:
#ifdef SQLNET_PARALLEL

    /// MPI communicator
    SQLNET_COMMUNICATOR* comm_;

#endif  // SQLNET_PARALLEL

    /// Indicates the best split.
    SQLNET_INT max_ix_;

    /// Stores the sufficient statistics when store_current_stage(...)
    /// is called
    containers::Matrix<SQLNET_FLOAT> sufficient_statistics_stored_;

    /// Indicates how many times something has already been written
    /// into the the storage after the last time set_storage_size(...)
    /// has been called
    SQLNET_INT storage_ix_;

    /// Value of the optimization criterion of the currently
    /// committed stage
    SQLNET_FLOAT value_;

    /// Stores the values calculated by find maximum. Can be resized
    /// by set_storage_size
    containers::Matrix<SQLNET_FLOAT> values_stored_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // SQLNET_OPTIMIZATIONCRITERIONIMPL_HPP_

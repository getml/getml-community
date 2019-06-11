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
    void commit( std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed );

    /// Resets sufficient statistics to zero
    void reset(
        std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_current,
        std::vector<AUTOSQL_FLOAT>* _sufficient_statistics_committed );

    /// Returns the sum of all sufficient statistics stored in the individual
    /// processes
    std::deque<std::vector<AUTOSQL_FLOAT>>
    reduce_sufficient_statistics_stored() const;

    /// Reverts to the committed version
    void revert_to_commit();

    /// Stores the current stage of the sufficient statistics
    void store_current_stage(
        const AUTOSQL_FLOAT _num_samples_smaller,
        const AUTOSQL_FLOAT _num_samples_greater,
        const std::vector<AUTOSQL_FLOAT>& _sufficient_statistics_current );

    // --------------------------------------

    /// Resets the storage size to zero.
    void reset_storage_size()
    {
        max_ix_ = -1;
        sufficient_statistics_stored_.clear();
        values_stored_.clear();
    }

    /// Trivial setter
    inline void set_comm( multithreading::Communicator* _comm )
    {
        comm_ = _comm;
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
    /// Trivial accessor
    inline std::deque<std::vector<AUTOSQL_FLOAT>>&
    sufficient_statistics_stored()
    {
        return sufficient_statistics_stored_;
    }

    /// Trivial accessor
    inline const std::deque<std::vector<AUTOSQL_FLOAT>>&
    sufficient_statistics_stored() const
    {
        return sufficient_statistics_stored_;
    }

    // --------------------------------------

   private:
    /// MPI communicator
    multithreading::Communicator* comm_;

    /// Indicates the best split.
    AUTOSQL_INT max_ix_;

    /// Stores the sufficient statistics when store_current_stage(...)
    /// is called
    std::deque<std::vector<AUTOSQL_FLOAT>> sufficient_statistics_stored_;

    /// Value of the optimization criterion of the currently
    /// committed stage
    AUTOSQL_FLOAT value_;

    /// Stores the values calculated by find maximum. Can be resized
    /// by set_storage_size
    std::vector<AUTOSQL_FLOAT> values_stored_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace autosql

#endif  // AUTOSQL_OPTIMIZATIONCRITERIONIMPL_HPP_

#ifndef RELBOOST_CONTAINERS_DATAFRAME_HPP_
#define RELBOOST_CONTAINERS_DATAFRAME_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

struct DataFrame
{
    // ---------------------------------------------------------------------

    typedef relboost::containers::Matrix<RELBOOST_FLOAT> FloatMatrixType;

    typedef relboost::containers::Matrix<RELBOOST_INT> IntMatrixType;

    // ---------------------------------------------------------------------

    DataFrame(
        const Matrix<RELBOOST_INT>& _categorical,
        const Matrix<RELBOOST_FLOAT>& _discrete,
        const std::vector<Matrix<RELBOOST_INT>>& _join_keys,
        const std::string& _name,
        const Matrix<RELBOOST_FLOAT>& _numerical,
        const Matrix<RELBOOST_FLOAT>& _target,
        const std::vector<Matrix<RELBOOST_FLOAT>>& _time_stamps );

    DataFrame(
        const Matrix<RELBOOST_INT>& _categorical,
        const Matrix<RELBOOST_FLOAT>& _discrete,
        const std::vector<std::shared_ptr<RELBOOST_INDEX>>& _indices,
        const std::vector<Matrix<RELBOOST_INT>>& _join_keys,
        const std::string& _name,
        const Matrix<RELBOOST_FLOAT>& _numerical,
        const Matrix<RELBOOST_FLOAT>& _target,
        const std::vector<Matrix<RELBOOST_FLOAT>>& _time_stamps );

    ~DataFrame() = default;

    // ---------------------------------------------------------------------

    /// Trivial getter
    size_t nrows() const
    {
        assert( join_keys_.size() > 0 );
        return join_keys_[0].nrows_;
    }

    /// Trivial getter
    RELBOOST_FLOAT time_stamps( size_t _nrow ) const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );
        assert( _nrow < time_stamps_[0].nrows_ );

        return time_stamps_[0][_nrow];
    }

    /// Trivial getter
    RELBOOST_FLOAT upper_time_stamps( size_t _nrow ) const
    {
        assert( time_stamps_.size() == 1 || time_stamps_.size() == 2 );

        if ( time_stamps_.size() == 1 )
            {
                return NAN;
            }

        assert( _nrow < time_stamps_[1].nrows_ );

        return time_stamps_[1][_nrow];
    }

    // ---------------------------------------------------------------------

    /// Creates the indices for this data frame
    static std::vector<std::shared_ptr<RELBOOST_INDEX>> create_indices(
        const std::vector<Matrix<RELBOOST_INT>>& _join_keys );

    /// Creates a subview
    DataFrame create_subview(
        const std::string& _name,
        const std::string& _join_key,
        const std::string& _time_stamp,
        const std::string& _upper_time_stamp ) const;

    // ---------------------------------------------------------------------

    /// Pointer to categorical columns.
    const Matrix<RELBOOST_INT> categorical_;

    /// Pointer to discrete columns.
    const Matrix<RELBOOST_FLOAT> discrete_;

    /// Indices assiciated with join keys.
    const std::vector<std::shared_ptr<RELBOOST_INDEX>> indices_;

    /// Join keys of this data frame.
    const std::vector<Matrix<RELBOOST_INT>> join_keys_;

    /// Name of the data frame.
    const std::string name_;

    /// Pointer to numerical columns.
    const Matrix<RELBOOST_FLOAT> numerical_;

    /// Pointer to target column.
    const Matrix<RELBOOST_FLOAT> target_;

    /// Time stamps of this data frame.
    const std::vector<Matrix<RELBOOST_FLOAT>> time_stamps_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_DATAFRAME_HPP_

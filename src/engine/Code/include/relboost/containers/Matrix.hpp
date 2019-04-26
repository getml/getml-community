#ifndef RELBOOST_CONTAINERS_MATRIX_HPP_
#define RELBOOST_CONTAINERS_MATRIX_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

template <typename T>
struct Matrix
{
    // ---------------------------------------------------------------------

    Matrix(
        const T* const _data,
        const std::string& _name,
        const size_t _nrows,
        const std::string& _unit )
        : data_( _data ), name_( _name ), nrows_( _nrows ), unit_( _unit )
    {
    }

    Matrix(
        const T* const _data, const std::string& _name, const size_t _nrows )
        : data_( _data ), name_( _name ), nrows_( _nrows ), unit_( "" )
    {
    }

    ~Matrix() = default;

    // ---------------------------------------------------------------------

   public:
    const T& operator[]( size_t _i ) const
    {
        assert( _i < nrows_ );

        return *( data_ + _i );
    }

    // ---------------------------------------------------------------------

    /// Pointer to the underlying data.
    const T* const data_;

    /// Name of the column
    const std::string name_;

    /// Number of rows
    const size_t nrows_;

    /// Unit of the column
    const std::string unit_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_MATRIX_HPP_

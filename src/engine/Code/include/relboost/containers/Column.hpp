#ifndef RELBOOST_CONTAINERS_COLUMN_HPP_
#define RELBOOST_CONTAINERS_COLUMN_HPP_

namespace relboost
{
namespace containers
{
// -------------------------------------------------------------------------

template <typename T>
struct Column
{
    // ---------------------------------------------------------------------

    typedef const T* iterator;

    // ---------------------------------------------------------------------

    Column(
        const T* const _data,
        const std::string& _name,
        const size_t _nrows,
        const std::string& _unit )
        : data_( _data ), name_( _name ), nrows_( _nrows ), unit_( _unit )
    {
    }

    Column(
        const T* const _data, const std::string& _name, const size_t _nrows )
        : data_( _data ), name_( _name ), nrows_( _nrows ), unit_( "" )
    {
    }

    ~Column() = default;

    // ---------------------------------------------------------------------

    /// Iterator begin
    iterator begin() const { return data_; }

    /// Iterator end
    iterator end() const { return data_ + nrows_; }

    /// Access operator
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

    // ---------------------------------------------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_COLUMN_HPP_

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
        const std::vector<std::string>& _colnames,
        const T* const _data,
        const size_t _nrows,
        const std::vector<std::string>& _units )
        : colnames_( _colnames ),
          data_( _data ),
          nrows_( _nrows ),
          units_( _units )
    {
        assert( _colnames.size() == _units.size() );
    }

    Matrix(
        const std::vector<std::string>& _colnames,
        const T* const _data,
        const size_t _nrows )
        : colnames_( _colnames ),
          data_( _data ),
          nrows_( _nrows ),
          units_( std::vector<std::string>( _colnames.size() ) )
    {
    }

    Matrix( const size_t _nrows )
        : colnames_( std::vector<std::string>( 0 ) ),
          data_( nullptr ),
          nrows_( _nrows ),
          units_( std::vector<std::string>( 0 ) )
    {
    }

    ~Matrix() = default;

    // ---------------------------------------------------------------------

   public:
    const T& operator()( size_t _i, size_t _j ) const
    {
        assert( _i < nrows_ );
        assert( _j < colnames_.size() );
        return *( data_ + _i * colnames_.size() + _j );
    }

    const T& operator[]( size_t _i ) const
    {
        assert( _i < nrows_ );
        assert( colnames_.size() == 1 );
        return *( data_ + _i );
    }

    // ---------------------------------------------------------------------

    /// Names of the columns
    const std::vector<std::string> colnames_;

    /// Pointer to the underlying data.
    const T* const data_;

    /// Number of rows
    const size_t nrows_;

    /// Units of the columns
    const std::vector<std::string> units_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace relboost

#endif  // RELBOOST_CONTAINERS_MATRIX_HPP_

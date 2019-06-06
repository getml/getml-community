#ifndef AUTOSQL_CONTAINER_MATRIX_HPP_
#define AUTOSQL_CONTAINER_MATRIX_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class Matrix
{
   public:
    typedef std::vector<T> DataType;

    typedef typename DataType::iterator iterator;

    typedef typename DataType::const_iterator const_iterator;

   public:
    Matrix( size_t _nrows = 0, size_t _ncols = 0 ) {}

    ~Matrix() {}

    // -------------------------------

    /// Appends another matrix through rowbinding
    void append( Matrix<T> _other );

    /// Sets nrows_, ncols_ to zero and intialises
    /// data_ with an empty vector
    void clear();

    // -------------------------------

    /// Const iterator to beginning of data
    inline const_iterator cbegin() const { return data_.cbegin(); }

    /// Const iterator to end of data
    inline const_iterator cend() const { return data_.cend(); }

    /// Iterator to beginning of data
    inline iterator begin() { return data_.begin(); }

    /// Returns a pointer to the underlying data
    T *data() { return data_.data(); }

    /// Returns a pointer to the underlying data
    const T *data() const { return data_.data(); }

    /// Iterator to end of data
    inline iterator end() { return data_.end(); }

    /// Accessor to data
    inline T &operator()( const size_t _i, const size_t _j )
    {
        assert( _i < nrows_ );
        assert( _j < ncols_ );
        assert( _i * _j == data_.size() );

        return data_[ncols_ * _i + _j];
    }

    /// Accessor to data
    inline T operator()( const size_t _i, const size_t _j ) const
    {
        assert( _i < nrows_ );
        assert( _j < ncols_ );
        assert( _i * _j == data_.size() );

        return data_[ncols_ * _i + _j];
    }

    /// Trivial getter
    inline size_t ncols() const { return ncols_; }

    /// Trivial getter
    inline size_t nrows() const { return nrows_; }

    /// Returns size of data
    inline const size_t size() const { return nrows_ * ncols_; }

    // -------------------------------

   private:
    /// The actual data.
    DataType data_;

    /// Number of columns
    size_t ncols_;

    /// Number of rows
    size_t nrows_;

    // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::append( Matrix<T> _other )
{
    if ( nrows() == 0 && ncols() == 0 )
        {
            ncols_ = _other.ncols();
        }

    if ( _other.ncols() != ncols() )
        {
            throw std::invalid_argument(
                "The number of columns needs to match for append()!" );
        }

    data_.insert( data_.end(), _other.begin(), _other.end() );

    nrows_ += _other.nrows();
}

// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::clear()
{
    *this = Matrix<T>( 0, 0 );
}

// -------------------------------------------------------------------------

}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_MATRIX_HPP_

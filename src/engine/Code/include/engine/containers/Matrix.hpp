#ifndef ENGINE_CONTAINERS_MATRIX_HPP_
#define ENGINE_CONTAINERS_MATRIX_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class Matrix
{
   public:
    Matrix( const size_t _nrows, const size_t _ncols, T *_data_ptr )
        : colnames_( std::make_shared<std::vector<std::string>>( _ncols, "" ) ),
          data_ptr_( _data_ptr ),
          ncols_( _ncols ),
          ncols_long_( static_cast<ULong>( _ncols ) ),
          nrows_( _nrows ),
          nrows_long_( static_cast<ULong>( _nrows ) ),
          units_( std::make_shared<std::vector<std::string>>( _ncols, "" ) ),
          type_( "Matrix" )
    {
        static_assert(
            std::is_arithmetic<T>::value,
            "Only arithmetic types allowed for Matrix<T>(...)!" );
    }

    Matrix( const size_t _nrows = 0, const size_t _ncols = 0 )
        : Matrix( _nrows, _ncols, nullptr )
    {
        data_ = std::make_shared<std::vector<T>>( nrows_long_ * ncols_long_ );

        data_ptr_ = data_->data();
    }

    Matrix(
        const size_t _nrows,
        const size_t _ncols,
        const std::shared_ptr<std::vector<T>> &_data )
        : Matrix( _nrows, _ncols, nullptr )
    {
        data_ = _data;

        data_ptr_ = data_->data();

        assert( nrows_ * ncols_ == data_->size() );
    }

    ~Matrix() {}

    // -------------------------------

    /// Appends another matrix through rowbinding
    void append( const Matrix<T> &_other );

    /// Sets nrows_, ncols_ to zero and intialises
    /// data_ with an empty vector
    void clear();

    /// Returns a copy of the _j'th column
    template <class T2>
    Matrix<T> column( T2 _j ) const;

    /// Loads the matrix from binary format
    void load( const std::string &_fname );

    /// Returns a matrix where all rows for which _key is true
    /// are removed.
    Matrix<T> remove_by_key( const std::vector<bool> &_key );

    /// Saves the matrix in binary format
    void save( const std::string &_fname ) const;

    /// Sorts the rows of the matrix by the key provided
    Matrix<T> sort_by_key( const Matrix<Int> &_key ) const;

    /// Sorts the rows of the matrix by the key provided
    Matrix<T> sort_by_key( const std::vector<Int> &_key ) const;

    /// Returns a shallow copy of a subselection of rows
    template <class T2>
    const Matrix<T> subview( T2 _row_begin, T2 _row_end ) const;

    /// Transforms matrix to a std::vector
    std::vector<T> to_vector() const;

    /// Returns a transposed version of this matrix
    Matrix<T> transpose() const;

    // -------------------------------

    /// Iterator to beginning of data
    T *begin() { return data_ptr_; }

    /// Const iterator to beginning of data
    T *begin() const { return data_ptr_; }

    /// Trivial getter
    const std::string &colname( const size_t _i ) const
    {
        assert( colnames_ );
        assert( _i < colnames_->size() );
        return ( *colnames_ )[_i];
    }

    /// Trivial getter
    std::shared_ptr<std::vector<std::string>> &colnames() { return colnames_; }

    /// Trivial getter
    const std::shared_ptr<std::vector<std::string>> &colnames() const
    {
        return colnames_;
    }

    /// Trivial getter
    T *&data() { return data_ptr_; }

    /// Trivial getter
    T *data() const { return data_ptr_; }

    /// Trivial getter
    std::shared_ptr<std::vector<T>> &data_ptr() { return data_; }

    /// Trivial getter
    const std::shared_ptr<std::vector<T>> &data_ptr() const { return data_; }

    /// Iterator to end of data
    T *end() { return data_ptr_ + size(); }

    /// Iterator to end of data
    T *end() const { return data_ptr_ + size(); }

    /// Returns number of bytes occupied by the data
    const ULong nbytes() const { return size() * sizeof( T ); }

    /// Accessor to data
    template <class T2>
    T &operator[]( const T2 _i )
    {
        assert(
            ( nrows() == 1 || ncols() == 1 ) &&
            "It is dangerous to call the operator[] on matrices with more than "
            "one column or row!" );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < nrows() * ncols() );

        return data_ptr_[_i];
    }

    /// Accessor to data
    template <class T2>
    T operator[]( const T2 _i ) const
    {
        assert(
            ( nrows() == 1 || ncols() == 1 ) &&
            "It is dangerous to call the operator[] on matrices with more than "
            "one column or row!" );
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < nrows() * ncols() );

        return data_ptr_[_i];
    }

    /// Accessor to data
    template <
        typename T2,
        typename std::enable_if<std::is_same<T2, ULong>::value == false, int>::
            type = 0>
    T &operator()( const T2 _i, const T2 _j )
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < nrows() );
        assert( _j >= 0 );
        assert( static_cast<size_t>( _j ) < ncols() );

        return data_ptr_
            [ncols_long_ * static_cast<ULong>( _i ) + static_cast<ULong>( _j )];
    }

    /// Accessor to data
    template <
        typename T2,
        typename std::enable_if<std::is_same<T2, ULong>::value == false, int>::
            type = 0>
    T operator()( const T2 _i, const T2 _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<size_t>( _i ) < nrows() );
        assert( _j >= 0 );
        assert( static_cast<size_t>( _j ) < ncols() );

        return data_ptr_
            [ncols_long_ * static_cast<ULong>( _i ) + static_cast<ULong>( _j )];
    }

    /// Accessor to data - specialization for when _i and _j are
    /// already of type ULong
    T &operator()( const ULong _i, const ULong _j )
    {
        assert( _i < nrows_long_ );
        assert( _j < ncols_long_ );

        return data_ptr_[ncols_long_ * _i + _j];
    }

    /// Accessor to data - specialization for when _i and _j are
    /// already of type ULong
    T operator()( const ULong _i, const ULong _j ) const
    {
        assert( _i < nrows_long_ );
        assert( _j < ncols_long_ );

        return data_ptr_[ncols_long_ * _i + _j];
    }

    /// Trivial getter
    size_t ncols() const { return ncols_; }

    /// Trivial getter
    size_t nrows() const { return nrows_; }

    /// Returns a shallow copy of a row
    template <class T2>
    Matrix<T> row( T2 _i )
    {
        return subview( _i, _i + 1 );
    }

    /// Returns a shallow copy of a row
    template <class T2>
    const Matrix<T> row( T2 _i ) const
    {
        return subview( _i, _i + 1 );
    }

    /// Trivial setter
    void set_colnames( const std::vector<std::string> &_colnames )
    {
        if ( static_cast<size_t>( _colnames.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of colnames provided does not match number of "
                    "columns! Expected: " +
                    std::to_string( ncols_ ) + ", got " +
                    std::to_string( _colnames.size() ) + "!" );
            }

        assert( colnames_ );

        ( *colnames_ ) = _colnames;
    }

    /// Trivial setter
    void set_units( const std::vector<std::string> &_units )
    {
        if ( static_cast<size_t>( _units.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of units provided does not match number of "
                    "columns!" );
            }

        assert( units_ );

        ( *units_ ) = _units;
    }

    /// Returns size of data
    const ULong size() const { return nrows_long_ * ncols_long_; }

    /// Trivial getter
    const std::string type() const { return type_; }

    /// Trivial getter
    const std::string &unit( const size_t _i ) const
    {
        assert( units_ );
        assert( _i < units_->size() );
        return ( *units_ )[_i];
    }

    /// Trivial getter
    std::shared_ptr<std::vector<std::string>> &units() { return units_; }

    /// Trivial getter
    std::shared_ptr<std::vector<std::string>> const &units() const
    {
        return units_;
    }
    // -------------------------------

   private:
    /// Called by load(...) when the system's byte order is big endian or the
    /// underlying type is char.
    Matrix<T> load_big_endian( const std::string &_fname ) const;

    /// Called by load(...) when the system's byte order is little endian and
    /// the underlying type is not char.
    Matrix<T> load_little_endian( const std::string &_fname ) const;

    /// Called by save(...) when the system's byte order is big endian or the
    /// underlying type is char.
    void save_big_endian( const std::string &_fname ) const;

    /// Called by save(...) when the system's byte order is little endian and
    /// the underlying type is not char.
    void save_little_endian( const std::string &_fname ) const;

    // -------------------------------

   private:
    /// Names of the columns
    std::shared_ptr<std::vector<std::string>> colnames_;

    /// The actual data, unless containers is simply virtual
    std::shared_ptr<std::vector<T>> data_;

    /// Pointer to the actual data. This is used by all member
    /// functions, in order to account for virtual containers
    T *data_ptr_;

    /// Number of columns
    size_t ncols_;

    /// Number of columns - unsigned long version
    ULong ncols_long_;

    /// Number of rows
    size_t nrows_;

    /// Number of rows - unsigned long version
    ULong nrows_long_;

    /// Units of the columns
    std::shared_ptr<std::vector<std::string>> units_;

    /// Type of this containers (since it is a base class)
    std::string type_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::append( const Matrix<T> &_other )
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

    assert(
        ( data_ptr_ == data_.get()->data() || size() == 0 ) &&
        "append() does not work for externally managed data!" );

    data_.get()->insert( data_.get()->end(), _other.begin(), _other.end() );

    data_ptr_ = data_.get()->data();

    nrows_ += _other.nrows();

    nrows_long_ = static_cast<ULong>( nrows_ );
}

// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::clear()
{
    *this = Matrix<T>( 0, 0 );
}

// -------------------------------------------------------------------------

template <class T>
template <class T2>
Matrix<T> Matrix<T>::column( T2 _j ) const
{
    assert( ( _j >= 0 && _j < nrows_ ) && "Matrix::column: _j out of bounds!" );

    if ( ncols_ == 1 )
        {
            return *this;
        }
    else
        {
            auto mat = Matrix<T>( nrows_, 1 );

            for ( size_t i = 0; i < nrows_; ++i )
                {
                    mat[i] = ( *this )( i, _j );
                }

            return mat;
        }
}

// -----------------------------------------------------------------------------

template <class T>
void Matrix<T>::load( const std::string &_fname )
{
    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            *this = load_little_endian( _fname );
        }
    else
        {
            *this = load_big_endian( _fname );
        }
}

// -----------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::load_big_endian( const std::string &_fname ) const
{
    // -------------------------------------------------------------------------

    debug_log( "Matrix.load: Big endian..." );

    std::ifstream input( _fname, std::ios::binary );

    // -------------------------------------------------------------------------
    // Read nrows

    debug_log( "Matrix.load: Read nrows..." );

    size_t nrows = 0;

    input.read( reinterpret_cast<char *>( &nrows ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Read ncols

    debug_log( "Matrix.load: Read ncols..." );

    size_t ncols = 0;

    input.read( reinterpret_cast<char *>( &ncols ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Init matrix

    debug_log( "Matrix.load: Init matrix..." );

    auto mat = Matrix<T>( nrows, ncols );

    // -------------------------------------------------------------------------
    // Read data

    debug_log( "Matrix.load: Read data..." );

    input.read(
        reinterpret_cast<char *>( mat.data() ),
        mat.nrows() * mat.ncols() * sizeof( T ) );

    // -------------------------------------------------------------------------
    // Read colnames and units

    auto read_string = [&input]( std::string &_str ) {
        size_t str_size = 0;

        input.read( reinterpret_cast<char *>( &str_size ), sizeof( size_t ) );

        _str.resize( str_size );

        input.read( &_str[0], str_size );
    };

    debug_log( "Matrix.load: Read colnames..." );

    assert( static_cast<size_t>( mat.colnames()->size() ) == mat.ncols() );

    std::for_each(
        mat.colnames()->begin(), mat.colnames()->end(), read_string );

    debug_log( "Matrix.load: Read units..." );

    assert( static_cast<size_t>( mat.colnames()->size() ) == mat.ncols() );

    std::for_each( mat.units()->begin(), mat.units()->end(), read_string );

    // -------------------------------------------------------------------------

    return mat;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::load_little_endian( const std::string &_fname ) const
{
    // -------------------------------------------------------------------------

    std::ifstream input( _fname, std::ios::binary );

    // -------------------------------------------------------------------------
    // Read nrows

    debug_log( "Matrix.load: Read nrows..." );

    size_t nrows = 0;

    input.read( reinterpret_cast<char *>( &nrows ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Read ncols

    debug_log( "Matrix.load: Read ncols..." );

    size_t ncols = 0;

    input.read( reinterpret_cast<char *>( &ncols ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Reverse byte order.

    utils::Endianness::reverse_byte_order( &nrows );

    utils::Endianness::reverse_byte_order( &ncols );

    // -------------------------------------------------------------------------
    // Init matrix

    debug_log( "Matrix.load: Init matrix..." );

    auto mat = Matrix<T>( nrows, ncols );

    // -------------------------------------------------------------------------
    // Read data

    debug_log( "Matrix.load: Read data..." );

    input.read(
        reinterpret_cast<char *>( mat.data() ),
        mat.nrows() * mat.ncols() * sizeof( T ) );

    // -------------------------------------------------------------------------
    // Reverse byte order.

    debug_log( "Matrix.load: Reverse byte order of data..." );

    auto reverse_data = []( T &_val ) {
        utils::Endianness::reverse_byte_order( &_val );
    };

    std::for_each( mat.begin(), mat.end(), reverse_data );

    // -------------------------------------------------------------------------
    // Read colnames and units.

    auto read_string = [&input]( std::string &_str ) {
        size_t str_size = 0;

        input.read( reinterpret_cast<char *>( &str_size ), sizeof( size_t ) );

        utils::Endianness::reverse_byte_order( &str_size );

        _str.resize( str_size );

        input.read( &_str[0], str_size );
    };

    debug_log( "Matrix.load: Read colnames..." );

    assert( static_cast<size_t>( mat.colnames()->size() ) == mat.ncols() );

    std::for_each(
        mat.colnames()->begin(), mat.colnames()->end(), read_string );

    debug_log( "Matrix.load: Read units..." );

    assert( static_cast<size_t>( mat.units()->size() ) == mat.ncols() );

    std::for_each( mat.units()->begin(), mat.units()->end(), read_string );

    // -------------------------------------------------------------------------

    return mat;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::remove_by_key( const std::vector<bool> &_key )
{
    assert(
        static_cast<size_t>( _key.size() ) == nrows() &&
        "Matrix: Size of keys must be identical to nrows!" );

    auto op = []( size_t init, bool elem ) {
        return ( ( elem ) ? ( init ) : ( init + 1 ) );
    };

    size_t nrows_new = std::accumulate( _key.begin(), _key.end(), 0, op );

    Matrix<T> trimmed( nrows_new, this->ncols() );

    size_t k = 0;

    for ( size_t i = 0; i < nrows(); ++i )
        {
            if ( _key[i] == false )
                {
                    for ( size_t j = 0; j < this->ncols(); ++j )
                        {
                            trimmed.data()[this->ncols() * k + j] =
                                data()[this->ncols() * i + j];
                        }

                    k++;
                }
        }

    return trimmed;
}

// -----------------------------------------------------------------------------

template <class T>
void Matrix<T>::save( const std::string &_fname ) const
{
    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            save_little_endian( _fname );
        }
    else
        {
            save_big_endian( _fname );
        }
}

// -----------------------------------------------------------------------------

template <class T>
void Matrix<T>::save_big_endian( const std::string &_fname ) const
{
    // -----------------------------------------------------------------

    debug_log( "Matrix.save: Is big endian..." );

    std::ofstream output( _fname, std::ios::binary );

    // -----------------------------------------------------------------
    // Write nrows

    debug_log( "Matrix.save: Write nrows..." );

    output.write( reinterpret_cast<const char *>( &nrows_ ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write ncols

    debug_log( "Matrix.save: Write ncols..." );

    output.write( reinterpret_cast<const char *>( &ncols_ ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write data

    debug_log( "Matrix.save: Write data..." );

    output.write(
        reinterpret_cast<const char *>( data() ),
        nrows() * ncols() * sizeof( T ) );

    // -----------------------------------------------------------------
    // Write colnames and units

    debug_log( "Matrix.save: Write colnames and units..." );

    auto write_string = [&output]( std::string &_str ) {
        size_t str_size = _str.size();

        output.write(
            reinterpret_cast<const char *>( &str_size ), sizeof( size_t ) );

        output.write( &( _str[0] ), _str.size() );
    };

    std::for_each( colnames()->begin(), colnames()->end(), write_string );

    std::for_each( units()->begin(), units()->end(), write_string );

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
void Matrix<T>::save_little_endian( const std::string &_fname ) const
{
    // -----------------------------------------------------------------

    debug_log( "Matrix.save: Is little endian..." );

    std::ofstream output( _fname, std::ios::binary );

    // -----------------------------------------------------------------
    // Write nrows

    debug_log( "Matrix.save: Write nrows..." );

    auto nrows = nrows_;

    utils::Endianness::reverse_byte_order( &nrows );

    output.write( reinterpret_cast<const char *>( &nrows ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write ncols

    debug_log( "Matrix.save: Write ncols..." );

    auto ncols = ncols_;

    utils::Endianness::reverse_byte_order( &ncols );

    output.write( reinterpret_cast<const char *>( &ncols ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write data

    debug_log( "Matrix.save: Write data..." );

    assert( data_ );

    auto write_reversed_data = [&output]( T &_val ) {
        T val_reversed = _val;

        utils::Endianness::reverse_byte_order( &val_reversed );

        output.write(
            reinterpret_cast<const char *>( &val_reversed ), sizeof( T ) );
    };

    std::for_each( begin(), end(), write_reversed_data );

    // -----------------------------------------------------------------
    // Write colnames and units

    debug_log( "Matrix.save: Write colnames and units..." );

    auto write_string = [&output]( std::string &_str ) {
        size_t str_size = _str.size();

        utils::Endianness::reverse_byte_order( &str_size );

        output.write(
            reinterpret_cast<const char *>( &str_size ), sizeof( size_t ) );

        output.write( &( _str[0] ), _str.size() );
    };

    std::for_each( colnames()->begin(), colnames()->end(), write_string );

    std::for_each( units()->begin(), units()->end(), write_string );

    // -----------------------------------------------------------------
}

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::sort_by_key( const std::vector<Int> &_key ) const
{
    Matrix<Int> key( _key.size(), static_cast<Int>( 1 ), _key.data() );

    return sort_by_key( key );
}

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::sort_by_key( const Matrix<Int> &_key ) const
{
    assert(
        _key.nrows() == nrows() &&
        "Matrix: Size of keys must be identical to nrows!" );

    Matrix<T> sorted( nrows(), ncols() );

    for ( size_t i = 0; i < nrows(); ++i )
        {
            assert(
                _key[i] >= 0 && _key[i] < nrows() &&
                "Matrix: Key out of bounds!" );

            for ( size_t j = 0; j < ncols(); ++j )
                {
                    sorted.data()[ncols() * i + j] =
                        data()[ncols() * _key[i] + j];
                }
        }

    return sorted;
}

// -------------------------------------------------------------------------

template <class T>
template <class T2>
const Matrix<T> Matrix<T>::subview( T2 _row_begin, T2 _row_end ) const
{
    assert(
        ( _row_begin >= 0 && _row_begin < nrows_ ) &&
        "Matrix::subview: _row_begin out of bounds!" );

    assert(
        ( _row_end >= 0 && _row_end <= nrows_ ) &&
        "Matrix::subview: _row_end out of bounds!" );

    assert( ( _row_end >= _row_begin ) && "Matrix::subview!" );

    auto mat = Matrix<T>(
        static_cast<size_t>( _row_end - _row_begin ),
        ncols_,
        data_ptr_ + static_cast<ULong>( _row_begin ) * ncols_long_ );

    mat.set_colnames( *( colnames_.get() ) );

    mat.set_units( *( units_ ) );

    return mat;
}

// -------------------------------------------------------------------------

template <class T>
std::vector<T> Matrix<T>::to_vector() const
{
    std::vector<T> vec( size() );

    std::copy( begin(), end(), vec.begin() );

    return vec;
}

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::transpose() const
{
    Matrix<T> transposed( ncols_, nrows_ );

    for ( ULong i = 0; i < nrows_long_; ++i )
        {
            for ( ULong j = 0; j < ncols_long_; ++j )
                {
                    transposed( j, i ) = ( *this )( i, j );
                }
        }

    return transposed;
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_MATRIX_HPP_

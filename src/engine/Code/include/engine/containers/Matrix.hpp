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
        : batches_( std::make_shared<std::vector<size_t>>( 0 ) ),
          colnames_( std::make_shared<std::vector<std::string>>( _ncols, "" ) ),
          data_ptr_( _data_ptr ),
          name_( std::make_shared<std::string>( "" ) ),
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

        batches() = {0, nrows_};
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

    /// Trivial getter
    std::vector<size_t> &batches() { return batches_.get()[0]; }

    /// Trivial getter
    const std::vector<size_t> &batches() const { return batches_.get()[0]; }

    /// Trivial getter for a batch. When the user loads the data in a batchwise
    /// fashion, the engine will actually store the data in form of these
    /// original batches.
    template <class T2>
    Matrix<T> batch( const T2 _batch_num )
    {
        return subview( batches()[_batch_num], batches()[_batch_num + 1] );
    }

    /// Trivial getter for a batch. When the user loads the data in a batchwise
    /// fashion, the engine will actually store the data in form of these
    /// original batches.
    template <class T2>
    const Matrix<T> batch( const T2 _batch_num ) const
    {
        return subview( batches()[_batch_num], batches()[_batch_num + 1] );
    }

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
    std::string &name() const { return *( name_ ); }

    /// Trivial getter
    size_t ncols() const { return ncols_; }

    /// Trivial getter
    size_t nrows() const { return nrows_; }

    /// Trivial getter
    const size_t num_batches() const
    {
        assert( batches().size() > 0 );
        return batches().size() - 1;
    }

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
    /// Batches contain information on how data was loaded
    /// into the containers, so the original order can be reconstructed
    std::shared_ptr<std::vector<size_t>> batches_;

    /// Names of the columns
    std::shared_ptr<std::vector<std::string>> colnames_;

    /// The actual data, unless containers is simply virtual
    std::shared_ptr<std::vector<T>> data_;

    /// Pointer to the actual data. This is used by all member
    /// functions, in order to account for virtual containers
    T *data_ptr_;

    /// Name of this containers
    std::shared_ptr<std::string> name_;

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

    batches().push_back( nrows_ );
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

// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::load( const std::string &_fname )
{
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
    // Read num_batches

    debug_log( "Matrix.load: Read num_batches..." );

    size_t num_batches = 0;

    input.read( reinterpret_cast<char *>( &num_batches ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Reverse byte order, if necessary

    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            debug_log( "Matrix.load: Is little endian (1)..." );

            utils::Endianness::reverse_byte_order( &nrows );

            utils::Endianness::reverse_byte_order( &ncols );

            utils::Endianness::reverse_byte_order( &num_batches );
        }

    // -------------------------------------------------------------------------
    // Read batches

    debug_log( "Matrix.load: Reading batches..." );

    std::vector<size_t> batches( num_batches );

    input.read(
        reinterpret_cast<char *>( batches.data() ),
        num_batches * sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Reverse byte order, if necessary

    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            debug_log( "Matrix.load: Reverse byte order of batches..." );

            auto reverse_batches = []( size_t &_val ) {
                utils::Endianness::reverse_byte_order( &_val );
            };

            std::for_each( batches.begin(), batches.end(), reverse_batches );
        }

    // -------------------------------------------------------------------------
    // Init matrix

    debug_log( "Matrix.load: Init matrix..." );

    *this = Matrix<T>( 0, ncols );

    // -------------------------------------------------------------------------
    // Read data

    debug_log( "Matrix.load: Read data..." );

    assert( batches.size() > 1 );

    for ( size_t i = 0; i < batches.size() - 1; ++i )
        {
            Matrix<T> mat( batches[i + 1] - batches[i], ncols );

            input.read(
                reinterpret_cast<char *>( mat.data() ),
                mat.nrows() * mat.ncols() * sizeof( T ) );

            append( mat );
        }

    // Because declaring the matrix using zero entries will create
    // and extra batch containing nothing, we drop that extra zero.
    this->batches().erase( this->batches().begin() );

    // -------------------------------------------------------------------------

    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            debug_log( "Matrix.load: Is little endian (2)..." );

            // -------------------------------------------------------------------------
            // Reverse byte order of data

            debug_log( "Matrix.load: Reverse byte order of data..." );

            {
                auto reverse_data = []( T &_val ) {
                    utils::Endianness::reverse_byte_order( &_val );
                };

                std::for_each( begin(), end(), reverse_data );
            }

            // -------------------------------------------------------------------------
            // Read colnames and units

            {
                // ---------------------------------------------------------------------

                auto read_string = [&input]( std::string &_str ) {
                    size_t str_size = 0;

                    input.read(
                        reinterpret_cast<char *>( &str_size ),
                        sizeof( size_t ) );

                    utils::Endianness::reverse_byte_order( &str_size );

                    _str.resize( str_size );

                    input.read( &_str[0], str_size );
                };

                // ---------------------------------------------------------------------

                debug_log( "Matrix.load: Read colnames..." );

                assert(
                    static_cast<size_t>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each(
                    colnames()->begin(), colnames()->end(), read_string );

                // ---------------------------------------------------------------------

                debug_log( "Matrix.load: Read units..." );

                assert(
                    static_cast<size_t>( units()->size() ) == this->ncols() );

                std::for_each( units()->begin(), units()->end(), read_string );

                // ---------------------------------------------------------------------

                read_string( name() );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
        }
    else
        {
            // -------------------------------------------------------------------------
            // Read colnames and units

            {
                // ---------------------------------------------------------------------

                auto read_string = [&input]( std::string &_str ) {
                    size_t str_size = 0;

                    input.read(
                        reinterpret_cast<char *>( &str_size ),
                        sizeof( size_t ) );

                    _str.resize( str_size );

                    input.read( &_str[0], str_size );
                };

                // ---------------------------------------------------------------------

                debug_log( "Matrix.load: Read colnames..." );

                assert(
                    static_cast<size_t>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each(
                    colnames()->begin(), colnames()->end(), read_string );

                // ---------------------------------------------------------------------

                debug_log( "Matrix.load: Read units..." );

                assert(
                    static_cast<size_t>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each( units()->begin(), units()->end(), read_string );

                // ---------------------------------------------------------------------

                read_string( name() );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
        }
}

// -------------------------------------------------------------------------

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

// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::save( const std::string &_fname ) const
{
    std::ofstream output( _fname, std::ios::binary );

    if ( std::is_same<T, char>::value == false &&
         utils::Endianness::is_little_endian() )
        {
            debug_log( "Matrix.save: Is little endian..." );

            // -------------------------------------------------------------------------
            // Write nrows

            debug_log( "Matrix.save: Write nrows..." );

            {
                size_t nrows = nrows_;

                utils::Endianness::reverse_byte_order( &nrows );

                output.write(
                    reinterpret_cast<const char *>( &nrows ),
                    sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write ncols

            debug_log( "Matrix.save: Write ncols..." );

            {
                size_t ncols = ncols_;

                utils::Endianness::reverse_byte_order( &ncols );

                output.write(
                    reinterpret_cast<const char *>( &ncols ),
                    sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write num_batches

            debug_log( "Matrix.save: Write num_batches..." );

            assert( batches_ );

            {
                size_t num_batches = batches_->size();

                utils::Endianness::reverse_byte_order( &num_batches );

                output.write(
                    reinterpret_cast<const char *>( &num_batches ),
                    sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write batches

            debug_log( "Matrix.save: Write batches..." );

            assert( batches_ );

            auto batches = this->batches();

            {
                // ---------------------------------------------------------------------

                auto write_inverted_batches = [&output]( size_t &_val ) {
                    size_t val_reversed = _val;

                    utils::Endianness::reverse_byte_order( &val_reversed );

                    output.write(
                        reinterpret_cast<const char *>( &val_reversed ),
                        sizeof( size_t ) );
                };

                std::for_each(
                    batches.begin(), batches.end(), write_inverted_batches );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
            // Write data

            debug_log( "Matrix.save: Write data..." );

            assert( data_ );

            {
                // ---------------------------------------------------------------------

                auto write_reversed_data = [&output]( T &_val ) {
                    T val_reversed = _val;

                    utils::Endianness::reverse_byte_order( &val_reversed );

                    output.write(
                        reinterpret_cast<const char *>( &val_reversed ),
                        sizeof( T ) );
                };

                // ---------------------------------------------------------------------

                for ( size_t i = 0; i < batches.size() - 1; ++i )
                    {
                        auto mat = batch( i );

                        assert( mat.ncols() == this->ncols() );

                        std::for_each(
                            mat.begin(), mat.end(), write_reversed_data );
                    }

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
            // Write colnames, units and name

            debug_log( "Matrix.save: Write colnames and units..." );

            {
                // ---------------------------------------------------------------------

                auto write_string = [&output]( std::string &_str ) {
                    size_t str_size = _str.size();

                    utils::Endianness::reverse_byte_order( &str_size );

                    output.write(
                        reinterpret_cast<const char *>( &str_size ),
                        sizeof( size_t ) );

                    output.write( &( _str[0] ), _str.size() );
                };

                // ---------------------------------------------------------------------

                std::for_each(
                    colnames()->begin(), colnames()->end(), write_string );

                // ---------------------------------------------------------------------

                std::for_each( units()->begin(), units()->end(), write_string );

                // ---------------------------------------------------------------------

                write_string( this->name() );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
        }
    else
        {
            debug_log( "Matrix.save: Is big endian..." );

            // -------------------------------------------------------------------------
            // Write nrows

            {
                debug_log( "Matrix.save: Write nrows..." );

                size_t nrows = nrows_;

                output.write(
                    reinterpret_cast<const char *>( &nrows ),
                    sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write ncols

            debug_log( "Matrix.save: Write ncols..." );

            output.write(
                reinterpret_cast<const char *>( &( ncols_ ) ),
                sizeof( size_t ) );

            // -------------------------------------------------------------------------
            // Write num_batches

            debug_log( "Matrix.save: Write num_batches..." );

            {
                size_t num_batches = batches_->size();

                output.write(
                    reinterpret_cast<const char *>( &num_batches ),
                    sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write batches

            auto batches = this->batches();

            {
                debug_log( "Matrix.save: Write batches..." );

                output.write(
                    reinterpret_cast<const char *>( batches.data() ),
                    batches.size() * sizeof( size_t ) );
            }

            // -------------------------------------------------------------------------
            // Write data

            debug_log( "Matrix.save: Write data..." );

            for ( size_t i = 0; i < batches.size() - 1; ++i )
                {
                    auto mat = batch( i );

                    output.write(
                        reinterpret_cast<const char *>( mat.data() ),
                        mat.nrows() * mat.ncols() * sizeof( T ) );
                }

            // -------------------------------------------------------------------------
            // Write colnames and units

            debug_log( "Matrix.save: Write colnames and units..." );

            {
                // ---------------------------------------------------------------------

                auto write_string = [&output]( std::string &_str ) {
                    size_t str_size = _str.size();

                    output.write(
                        reinterpret_cast<const char *>( &str_size ),
                        sizeof( size_t ) );

                    output.write( &( _str[0] ), _str.size() );
                };

                // ---------------------------------------------------------------------

                std::for_each(
                    colnames()->begin(), colnames()->end(), write_string );

                // ---------------------------------------------------------------------

                std::for_each( units()->begin(), units()->end(), write_string );

                // ---------------------------------------------------------------------

                write_string( this->name() );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
        }
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

    mat.name() = name();

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

#ifndef ENGINE_CONTAINERS_COLUMN_HPP_
#define ENGINE_CONTAINERS_COLUMN_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class Column
{
   public:
    static constexpr const char *FLOAT_COLUMN = "FloatColumn";
    static constexpr const char *STRING_COLUMN = "StringColumn";

    static constexpr const char *FLOAT_COLUMN_VIEW = "FloatColumnView";
    static constexpr const char *STRING_COLUMN_VIEW = "StringColumnView";
    static constexpr const char *BOOLEAN_COLUMN_VIEW = "BooleanColumnView";

   public:
    typedef T value_type;

    Column( const size_t _nrows )
        : data_ptr_( std::make_shared<std::vector<T>>( _nrows ) ),
          name_( "" ),
          nrows_( _nrows ),
          unit_( "" )
    {
        static_assert(
            std::is_arithmetic<T>::value ||
                std::is_same<T, std::string>::value ||
                std::is_same<T, strings::String>::value,
            "Only arithmetic types or strings::String allowed for "
            "Column<T>(...)!" );
    }

    Column( const std::shared_ptr<std::vector<T>> &_data_ptr )
        : data_ptr_( _data_ptr ), name_( "" ), unit_( "" )
    {
        assert_true( data_ptr_ );
        nrows_ = data_ptr_->size();
    }

    Column(
        const std::shared_ptr<std::vector<T>> &_data_ptr,
        const std::string &_name )
        : Column( _data_ptr )
    {
        set_name( _name );
    }

    Column() : Column( 0 ) {}

    ~Column() {}

    // -------------------------------

    /// Appends another Column through rowbinding
    void append( const Column<T> &_other );

    /// Sets nrows_, ncols_ to zero and intialises
    /// data_ with an empty vector
    void clear();

    /// Generates a deep copy of the column itself.
    Column<T> clone() const;

    /// Loads the Column from binary format
    void load( const std::string &_fname );

    /// Saves the Column in binary format
    void save( const std::string &_fname ) const;

    /// Returns a copy of the column that has been sorted by the
    /// key provided.
    /// The resulting column does not have to be the same length
    /// as the original one, but will be of the same length as the
    /// _key.
    Column<T> sort_by_key( const std::vector<size_t> &_key ) const;

    /// Transforms Column to a std::vector
    std::vector<T> to_vector() const;

    /// Returns a Column containing all rows for which _key is true.
    Column<T> where( const std::vector<bool> &_condition ) const;

    // -------------------------------

    /// Boundary-checked accessor to data
    template <class T2>
    T &at( const T2 _i )
    {
        if ( !data_ptr_ || _i < 0 || static_cast<size_t>( _i ) >= nrows() )
            {
                throw std::invalid_argument(
                    "Out-of-bounds access to column '" + name_ + "'" );
            }

        return ( *data_ptr_ )[_i];
    }

    /// Boundary-checker accessor to data
    template <class T2>
    T at( const T2 _i ) const
    {
        if ( !data_ptr_ || _i < 0 || static_cast<size_t>( _i ) >= nrows() )
            {
                throw std::invalid_argument(
                    "Out-of-bounds access to column '" + name_ + "'" );
            }

        return ( *data_ptr_ )[_i];
    }

    /// Iterator to beginning of data
    T *begin() { return data_ptr_->data(); }

    /// Const iterator to beginning of data
    T *begin() const { return data_ptr_->data(); }

    /// Trivial getter
    T *data() { return data_ptr_->data(); }

    /// Trivial getter
    const T *data() const { return data_ptr_->data(); }

    /// Trivial getter
    std::shared_ptr<std::vector<T>> &data_ptr() { return data_ptr_; }

    /// Trivial getter
    const std::shared_ptr<std::vector<T>> &data_ptr() const
    {
        return data_ptr_;
    }

    /// Iterator to end of data
    T *end() { return data_ptr_->data() + nrows(); }

    /// Iterator to end of data
    T *end() const { return data_ptr_->data() + nrows(); }

    /// Trivial getter
    const std::string &name() const { return name_; }

    /// Returns number of bytes occupied by the data
    const ULong nbytes() const
    {
        if constexpr ( std::is_same<T, strings::String>() )
            {
                return std::accumulate(
                    begin(),
                    end(),
                    static_cast<size_t>( nrows() * ( sizeof( T ) + 1 ) ),
                    []( const size_t init, const T &_str ) {
                        return init + _str.size();
                    } );
            }
        else
            {
                return static_cast<ULong>( nrows() ) * sizeof( T );
            }
    }

    /// Accessor to data
    template <class T2>
    T &operator[]( const T2 _i )
    {
        assert_true( _i >= 0 );
        assert_true( static_cast<size_t>( _i ) < nrows() );

        return ( *data_ptr_ )[_i];
    }

    /// Accessor to data
    template <class T2>
    T operator[]( const T2 _i ) const
    {
        assert_true( _i >= 0 );
        assert_true( static_cast<size_t>( _i ) < nrows() );

        return ( *data_ptr_ )[_i];
    }

    /// Trivial getter
    size_t nrows() const { return nrows_; }

    /// Trivial setter
    void set_name( const std::string &_name ) { name_ = _name; }

    /// Trivial setter
    void set_unit( const std::string &_unit ) { unit_ = _unit; }

    /// Trivial getter.
    const size_t size() const { return nrows(); }

    /// For int types only: Transforms to a float column.
    template <
        typename T2 = T,
        typename std::enable_if<std::is_same<T2, Int>::value, int>::type = 0>
    Column<Float> to_float_column() const
    {
        auto float_col = Column<Float>( nrows() );

        for ( size_t i = 0; i < nrows(); ++i )
            {
                const auto val = ( *this )[i];
                if ( val == std::numeric_limits<Int>::lowest() )
                    {
                        float_col[i] = NAN;
                    }
                else
                    {
                        float_col[i] = static_cast<Float>( val );
                    }
            }

        return float_col;
    }

    /// For float types only: Transforms to a Int column.
    template <
        typename T2 = T,
        typename std::enable_if<std::is_same<T2, Float>::value, int>::type = 0>
    Column<Int> to_int_column() const
    {
        auto int_col = Column<Int>( nrows() );

        for ( size_t i = 0; i < nrows(); ++i )
            {
                const auto val = ( *this )[i];
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        int_col[i] = std::numeric_limits<Int>::lowest();
                    }
                else
                    {
                        int_col[i] = static_cast<Int>( val );
                    }
            }

        return int_col;
    }

    /// Trivial getter
    const std::string &unit() const { return unit_; }

    // -------------------------------

   private:
    /// Called by load(...) when the system's byte order is big endian or the
    /// underlying type is char.
    Column<T> load_big_endian( const std::string &_fname ) const;

    /// Called by load(...) when the system's byte order is little endian and
    /// the underlying type is not char.
    Column<T> load_little_endian( const std::string &_fname ) const;

    /// Called by save(...) when the system's byte order is big endian or the
    /// underlying type is char.
    void save_big_endian( const std::string &_fname ) const;

    /// Called by save(...) when the system's byte order is little endian and
    /// the underlying type is not char.
    void save_little_endian( const std::string &_fname ) const;

    // -------------------------------

   private:
    /// Called by load_big_endian(...).
    template <class StringType>
    void read_string_big_endian( StringType *_str, std::ifstream *_input ) const
    {
        size_t str_size = 0;

        _input->read( reinterpret_cast<char *>( &str_size ), sizeof( size_t ) );

        std::string str;

        // Using resize is important, otherwise
        // the result is weird behaviour!
        str.resize( str_size );

        _input->read( &( str[0] ), str_size );

        *_str = std::move( str );
    };

    /// Called by load_little_endian(...).
    template <class StringType>
    void read_string_little_endian(
        StringType *_str, std::ifstream *_input ) const
    {
        size_t str_size = 0;

        _input->read( reinterpret_cast<char *>( &str_size ), sizeof( size_t ) );

        utils::Endianness::reverse_byte_order( &str_size );

        std::string str;

        // Using resize is important, otherwise
        // the result is weird behaviour!
        str.resize( str_size );

        _input->read( &( str[0] ), str_size );

        *_str = std::move( str );
    };

    /// Called by save_big_endian(...).
    template <class StringType>
    void write_string_big_endian(
        const StringType &_str, std::ofstream *_output ) const
    {
        size_t str_size = _str.size();

        _output->write(
            reinterpret_cast<const char *>( &str_size ), sizeof( size_t ) );

        _output->write( _str.c_str(), _str.size() );
    };

    /// Called by save_little_endian(...).
    template <class StringType>
    void write_string_little_endian(
        const StringType &_str, std::ofstream *_output ) const
    {
        size_t str_size = _str.size();

        utils::Endianness::reverse_byte_order( &str_size );

        _output->write(
            reinterpret_cast<const char *>( &str_size ), sizeof( size_t ) );

        _output->write( _str.c_str(), _str.size() );
    };

    // -------------------------------

   private:
    /// The actual data.
    std::shared_ptr<std::vector<T>> data_ptr_;

    /// Name of the column.
    std::string name_;

    /// Number of rows.
    size_t nrows_;

    /// Unit of the column.
    std::string unit_;

    // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Column<T>::append( const Column<T> &_other )
{
    if ( !data_ptr_ )
        {
            throw std::invalid_argument(
                "Cannot append to column! It contains no data!" );
        }

    data_ptr_->insert( data_ptr_->end(), _other.begin(), _other.end() );

    nrows_ += _other.nrows();
}

// -------------------------------------------------------------------------

template <class T>
void Column<T>::clear()
{
    *this = Column<T>( 0, 0 );
}

// -------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::clone() const
{
    if ( !data_ptr_ )
        {
            throw std::invalid_argument(
                "Column cannot be cloned! It contains no data!" );
        }

    const auto vec = std::make_shared<std::vector<T>>(
        data_ptr_->begin(), data_ptr_->end() );

    auto col = Column<T>( vec );

    col.set_name( name_ );

    col.set_unit( unit_ );

    return col;
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::load( const std::string &_fname )
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
Column<T> Column<T>::load_big_endian( const std::string &_fname ) const
{
    // -------------------------------------------------------------------------

    debug_log( "Column.load: Big endian..." );

    std::ifstream input( _fname, std::ios::binary );

    // -------------------------------------------------------------------------
    // Read nrows

    debug_log( "Column.load: Read nrows..." );

    size_t nrows = 0;

    input.read( reinterpret_cast<char *>( &nrows ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Init matrix

    debug_log( "Column.load: Init Column..." );

    auto col = Column<T>( nrows );

    // -------------------------------------------------------------------------
    // Read data

    debug_log( "Column.load: Read data..." );

    if constexpr ( std::is_same<T, strings::String>() )
        {
            for ( auto &str : col ) read_string_big_endian( &str, &input );
        }
    else
        {
            input.read(
                reinterpret_cast<char *>( col.data() ),
                col.nrows() * sizeof( T ) );
        }

    // -------------------------------------------------------------------------
    // Read name and unit

    read_string_big_endian( &col.name_, &input );

    read_string_big_endian( &col.unit_, &input );

    // -------------------------------------------------------------------------

    return col;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::load_little_endian( const std::string &_fname ) const
{
    // -------------------------------------------------------------------------

    std::ifstream input( _fname, std::ios::binary );

    // -------------------------------------------------------------------------
    // Read nrows

    debug_log( "Column.load: Read nrows..." );

    size_t nrows = 0;

    input.read( reinterpret_cast<char *>( &nrows ), sizeof( size_t ) );

    // -------------------------------------------------------------------------
    // Reverse byte order.

    utils::Endianness::reverse_byte_order( &nrows );

    // -------------------------------------------------------------------------
    // Init Column

    debug_log( "Column.load: Init Column..." );

    auto col = Column<T>( nrows );

    // -------------------------------------------------------------------------
    // Read data

    debug_log( "Column.load: Read data..." );

    if constexpr ( std::is_same<T, strings::String>() )
        {
            for ( auto &str : col ) read_string_little_endian( &str, &input );
        }
    else
        {
            input.read(
                reinterpret_cast<char *>( col.data() ),
                col.nrows() * sizeof( T ) );
        }

    // -------------------------------------------------------------------------
    // Reverse byte order.

    if constexpr ( !std::is_same<T, strings::String>() )
        {
            debug_log( "Column.load: Reverse byte order of data..." );

            auto reverse_data = []( T &_val ) {
                utils::Endianness::reverse_byte_order( &_val );
            };

            std::for_each( col.begin(), col.end(), reverse_data );
        }

    // -------------------------------------------------------------------------
    // Read colnames and units.

    read_string_little_endian( &col.name_, &input );

    read_string_little_endian( &col.unit_, &input );

    // -------------------------------------------------------------------------

    return col;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::save( const std::string &_fname ) const
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
void Column<T>::save_big_endian( const std::string &_fname ) const
{
    // -----------------------------------------------------------------

    debug_log( "Column.save: Is big endian..." );

    std::ofstream output( _fname, std::ios::binary );

    // -----------------------------------------------------------------
    // Write nrows

    debug_log( "Column.save: Write nrows..." );

    output.write( reinterpret_cast<const char *>( &nrows_ ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write data

    debug_log( "Column.save: Write data..." );

    if constexpr ( std::is_same<T, strings::String>() )
        {
            for ( auto it = begin(); it != end(); ++it )
                write_string_big_endian( *it, &output );
        }
    else
        {
            output.write(
                reinterpret_cast<const char *>( data() ),
                nrows() * sizeof( T ) );
        }

    // -----------------------------------------------------------------
    // Write colnames and units

    debug_log( "Column.save: Write colnames and units..." );

    write_string_big_endian( name_, &output );

    write_string_big_endian( unit_, &output );

    // -----------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <class T>
void Column<T>::save_little_endian( const std::string &_fname ) const
{
    // -----------------------------------------------------------------

    debug_log( "Column.save: Is little endian..." );

    std::ofstream output( _fname, std::ios::binary );

    // -----------------------------------------------------------------
    // Write nrows

    debug_log( "Column.save: Write nrows..." );

    auto nrows = nrows_;

    utils::Endianness::reverse_byte_order( &nrows );

    output.write( reinterpret_cast<const char *>( &nrows ), sizeof( size_t ) );

    // -----------------------------------------------------------------
    // Write data

    debug_log( "Column.save: Write data..." );

    if constexpr ( std::is_same<T, strings::String>() )
        {
            for ( auto it = begin(); it != end(); ++it )
                write_string_little_endian( *it, &output );
        }
    else
        {
            auto write_reversed_data = [&output]( T &_val ) {
                T val_reversed = _val;

                utils::Endianness::reverse_byte_order( &val_reversed );

                output.write(
                    reinterpret_cast<const char *>( &val_reversed ),
                    sizeof( T ) );
            };

            std::for_each( begin(), end(), write_reversed_data );
        }

    // -----------------------------------------------------------------
    // Write name and unit

    debug_log( "Column.save: Write colname and unit..." );

    write_string_little_endian( name_, &output );

    write_string_little_endian( unit_, &output );

    // -----------------------------------------------------------------
}

// -------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::sort_by_key( const std::vector<size_t> &_key ) const
{
    Column<T> sorted( _key.size() );

    sorted.set_name( name() );

    sorted.set_unit( unit() );

    for ( size_t i = 0; i < _key.size(); ++i )
        {
            if ( _key[i] < nrows() )
                {
                    sorted[i] = ( *this )[_key[i]];
                }
            else
                {
                    if constexpr ( std::is_same<T, Float>() )
                        {
                            sorted[i] = static_cast<Float>( NAN );
                        }
                    else if constexpr ( std::is_same<T, strings::String>() )
                        {
                            sorted[i] = "";
                        }
                    else if ( std::is_same<T, Int>() )
                        {
                            sorted[i] = -1;
                        }
                    else
                        {
                            assert_true( false );
                        }
                }
        }

    return sorted;
}

// -------------------------------------------------------------------------

template <class T>
std::vector<T> Column<T>::to_vector() const
{
    std::vector<T> vec( size() );

    std::copy( begin(), end(), vec.begin() );

    return vec;
}

// -----------------------------------------------------------------------------

template <class T>
Column<T> Column<T>::where( const std::vector<bool> &_condition ) const
{
    if ( _condition.size() != nrows() )
        {
            throw std::invalid_argument(
                "Size of keys must be identical to number of rows!" );
        }

    auto op = []( size_t init, bool elem ) {
        return ( ( elem ) ? ( init + 1 ) : ( init ) );
    };

    size_t nrows_new =
        std::accumulate( _condition.begin(), _condition.end(), 0, op );

    Column<T> trimmed( nrows_new );

    trimmed.name_ = name_;

    trimmed.unit_ = unit_;

    size_t k = 0;

    for ( size_t i = 0; i < nrows(); ++i )
        {
            if ( _condition[i] )
                {
                    trimmed[k++] = data()[i];
                }
        }

    return trimmed;
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_COLUMN_HPP_

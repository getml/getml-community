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
    Matrix( SQLNET_INT _nrows, SQLNET_INT _ncols, T *_data_ptr )
        : batches_( std::make_shared<std::vector<SQLNET_INT>>( 0 ) ),
          colnames_( std::make_shared<std::vector<std::string>>( _ncols, "" ) ),
          data_ptr_( _data_ptr ),
          name_( std::make_shared<std::string>( "" ) ),
          ncols_( _ncols ),
          ncols_long_( static_cast<SQLNET_UNSIGNED_LONG>( _ncols ) ),
          nrows_( _nrows ),
          nrows_long_( static_cast<SQLNET_UNSIGNED_LONG>( _nrows ) ),
          units_( std::make_shared<std::vector<std::string>>( _ncols, "" ) ),
          type_( "Matrix" )
    {
        static_assert(
            std::is_arithmetic<T>::value,
            "Only arithmetic types allowed for Matrix<T>(...)!" );

        batches() = {0, nrows_};
    }

    Matrix( SQLNET_INT _nrows = 0, SQLNET_INT _ncols = 0 )
        : Matrix( _nrows, _ncols, nullptr )
    {
        data_ = std::make_shared<std::vector<T>>( nrows_long_ * ncols_long_ );

        data_ptr_ = data_.get()->data();
    }

    ~Matrix() {}

    // -------------------------------

    /// Appends another matrix through rowbinding
    void append( Matrix<T> _other );

    /// Sets nrows_, ncols_ to zero and intialises
    /// data_ with an empty vector
    void clear();

    /// Returns a copy of the _j'th column
    template <class T2>
    inline Matrix<T> column( T2 _j ) const;

#ifdef SQLNET_MULTINODE_MPI

    /// MPI version only: Gathers matrix at this point
    containers::Matrix<T> gather_root();

    /// MPI version only: Non-root equivalent to gather_root()
    void gather();

#endif  // SQLNET_MULTINODE_MPI

    /// Loads the matrix from binary format
    void load( std::string _fname );

#ifdef SQLNET_MULTINODE_MPI

    /// MPI version only: Non-root equivalent to load()
    void load();

#endif  // SQLNET_MULTINODE_MPI

    /// Returns a matrix where all rows for which _key is true
    /// are removed
    Matrix<T> remove_by_key( std::vector<bool> &_key );

    /// Saves the matrix in binary format
    void save( std::string _fname ) const;

#ifdef SQLNET_MULTINODE_MPI

    /// MPI version only: Non-root equivalent to save()
    void save();

    /// MPI version only: Scatters the matrix across several processes
    Matrix<T> scatter();

#endif  // SQLNET_MULTINODE_MPI

    /// Sorts the rows of the matrix by the key provided
    Matrix<T> sort_by_key( Matrix<SQLNET_INT> &_key );

    /// Sorts the rows of the matrix by the key provided
    Matrix<T> sort_by_key( std::vector<SQLNET_INT> &_key );

    /// Returns a shallow copy of a subselection of rows
    template <class T2>
    inline Matrix<T> subview( T2 _row_begin, T2 _row_end );

    /// Returns a shallow copy of a subselection of rows
    template <class T2>
    inline const Matrix<T> subview( T2 _row_begin, T2 _row_end ) const;

    /// Transforms matrix to a std::vector
    std::vector<T> to_vector() const;

    /// Returns a transposed version of this matrix
    Matrix<T> transpose();

    // -------------------------------

    /// Trivial getter
    inline std::vector<SQLNET_INT> &batches() { return batches_.get()[0]; }

    /// Trivial getter
    inline const std::vector<SQLNET_INT> &batches() const
    {
        return batches_.get()[0];
    }

    /// Trivial getter for a batch. When the user loads the data in a batchwise
    /// fashion, the engine will actually store the data in form of these
    /// original batches.
    template <class T2>
    inline Matrix<T> batch( const T2 _batch_num )
    {
        return subview( batches()[_batch_num], batches()[_batch_num + 1] );
    }

    /// Trivial getter for a batch. When the user loads the data in a batchwise
    /// fashion, the engine will actually store the data in form of these
    /// original batches.
    template <class T2>
    inline const Matrix<T> batch( const T2 _batch_num ) const
    {
        return subview( batches()[_batch_num], batches()[_batch_num + 1] );
    }

    /// Iterator to beginning of data
    inline T *begin() { return data_ptr_; }

    /// Const iterator to beginning of data
    inline T *begin() const { return data_ptr_; }

    /// Trivial getter
    inline const std::string &colname( const SQLNET_INT _i ) const
    {
        return colnames_.get()[0][_i];
    }

    /// Trivial getter
    inline std::shared_ptr<std::vector<std::string>> &colnames()
    {
        return colnames_;
    }

    /// Trivial getter
    inline const std::shared_ptr<std::vector<std::string>> &colnames() const
    {
        return colnames_;
    }

    /// Trivial getter
    inline T *&data() { return data_ptr_; }

    /// Trivial getter
    inline T *data() const { return data_ptr_; }

    /// Iterator to end of data
    inline T *end() { return data_ptr_ + size(); }

    /// Iterator to end of data
    inline T *end() const { return data_ptr_ + size(); }

    /// Returns number of bytes occupied by the data
    inline const SQLNET_UNSIGNED_LONG nbytes() const
    {
        return size() * sizeof( T );
    }

    /// Accessor to data
    template <class T2>
    inline T &operator[]( const T2 _i )
    {
        assert(
            ( nrows() == 1 || ncols() == 1 ) &&
            "It is dangerous to call the operator[] on matrices with more than "
            "one column or row!" );
        assert( _i >= 0 );
        assert( static_cast<SQLNET_INT>( _i ) < nrows() * ncols() );

        return data_ptr_[_i];
    }

    /// Accessor to data
    template <class T2>
    inline T operator[]( const T2 _i ) const
    {
        assert(
            ( nrows() == 1 || ncols() == 1 ) &&
            "It is dangerous to call the operator[] on matrices with more than "
            "one column or row!" );
        assert( _i >= 0 );
        assert( static_cast<SQLNET_INT>( _i ) < nrows() * ncols() );

        return data_ptr_[_i];
    }

    /// Accessor to data
    template <
        typename T2,
        typename std::enable_if<
            std::is_same<T2, SQLNET_UNSIGNED_LONG>::value == false,
            int>::type = 0>
    inline T &operator()( const T2 _i, const T2 _j )
    {
        assert( _i >= 0 );
        assert( static_cast<SQLNET_INT>( _i ) < nrows() );
        assert( _j >= 0 );
        assert( static_cast<SQLNET_INT>( _j ) < ncols() );

        return data_ptr_
            [ncols_long_ * static_cast<SQLNET_UNSIGNED_LONG>( _i ) +
             static_cast<SQLNET_UNSIGNED_LONG>( _j )];
    }

    /// Accessor to data
    template <
        typename T2,
        typename std::enable_if<
            std::is_same<T2, SQLNET_UNSIGNED_LONG>::value == false,
            int>::type = 0>
    inline T operator()( const T2 _i, const T2 _j ) const
    {
        assert( _i >= 0 );
        assert( static_cast<SQLNET_INT>( _i ) < nrows() );
        assert( _j >= 0 );
        assert( static_cast<SQLNET_INT>( _j ) < ncols() );

        return data_ptr_
            [ncols_long_ * static_cast<SQLNET_UNSIGNED_LONG>( _i ) +
             static_cast<SQLNET_UNSIGNED_LONG>( _j )];
    }

    /// Accessor to data - specialization for when _i and _j are
    /// already of type SQLNET_UNSIGNED_LONG
    inline T &operator()(
        const SQLNET_UNSIGNED_LONG _i, const SQLNET_UNSIGNED_LONG _j )
    {
        assert( _i < nrows_long_ );
        assert( _j < ncols_long_ );

        return data_ptr_[ncols_long_ * _i + _j];
    }

    /// Accessor to data - specialization for when _i and _j are
    /// already of type SQLNET_UNSIGNED_LONG
    inline T operator()(
        const SQLNET_UNSIGNED_LONG _i, const SQLNET_UNSIGNED_LONG _j ) const
    {
        assert( _i < nrows_long_ );
        assert( _j < ncols_long_ );

        return data_ptr_[ncols_long_ * _i + _j];
    }

    /// Trivial getter
    inline std::string &name() const { return *( name_ ); }

    /// Trivial getter
    inline SQLNET_INT ncols() const { return ncols_; }

    /// Trivial getter
    inline SQLNET_INT nrows() const { return nrows_; }

    /// Trivial getter
    inline const SQLNET_SIZE num_batches() const
    {
        return batches().size() - 1;
    }

    /// Returns a shallow copy of a row
    template <class T2>
    inline Matrix<T> row( T2 _i )
    {
        return subview( _i, _i + 1 );
    }

    /// Returns a shallow copy of a row
    template <class T2>
    inline const Matrix<T> row( T2 _i ) const
    {
        return subview( _i, _i + 1 );
    }

    /// Trivial setter
    inline void set_colnames( std::vector<std::string> &_colnames )
    {
        if ( static_cast<SQLNET_INT>( _colnames.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of colnames provided does not match number of "
                    "columns! Expected: " +
                    std::to_string( ncols_ ) + ", got " +
                    std::to_string( _colnames.size() ) + "!" );
            }

        colnames_.get()[0] = _colnames;
    }

    /// Trivial setter
    inline void set_units( std::vector<std::string> &_units )
    {
        if ( static_cast<SQLNET_INT>( _units.size() ) != ncols_ )
            {
                throw std::invalid_argument(
                    "Number of units provided does not match number of "
                    "columns!" );
            }

        units_.get()[0] = _units;
    }

    /// Returns size of data
    inline const SQLNET_UNSIGNED_LONG size() const
    {
        return nrows_long_ * ncols_long_;
    }

    /// Trivial getter
    inline const std::string type() const { return type_; }

    /// Trivial getter
    inline std::string &unit( SQLNET_INT _i )
    {
        assert( static_cast<SQLNET_INT>( units_->size() ) > _i && _i >= 0 );
        return units_.get()[0][_i];
    }

    /// Trivial getter
    inline const std::string &unit( SQLNET_INT _i ) const
    {
        assert( static_cast<SQLNET_INT>( units_->size() ) > _i && _i >= 0 );
        return units_.get()[0][_i];
    }

    /// Trivial getter
    inline std::shared_ptr<std::vector<std::string>> &units() { return units_; }

    /// Trivial getter
    inline std::shared_ptr<std::vector<std::string>> const &units() const
    {
        return units_;
    }

    // -------------------------------

   private:
    /// Batches contain information on how data was loaded
    /// into the containers, so the original order can be reconstructed
    std::shared_ptr<std::vector<SQLNET_INT>> batches_;

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
    SQLNET_INT ncols_;

    /// Number of columns - unsigned long version
    SQLNET_UNSIGNED_LONG ncols_long_;

    /// Number of rows
    SQLNET_INT nrows_;

    /// Number of rows - unsigned long version
    SQLNET_UNSIGNED_LONG nrows_long_;

    /// Units of the columns
    std::shared_ptr<std::vector<std::string>> units_;

    /// Type of this containers (since it is a base class)
    std::string type_;
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

    assert(
        ( data_ptr_ == data_.get()->data() || size() == 0 ) &&
        "append() does not work for externally managed data!" );

    data_.get()->insert( data_.get()->end(), _other.begin(), _other.end() );

    data_ptr_ = data_.get()->data();

    nrows_ += _other.nrows();

    nrows_long_ = static_cast<SQLNET_UNSIGNED_LONG>( nrows_ );

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
inline Matrix<T> Matrix<T>::column( T2 _j ) const
{
    assert( ( _j >= 0 && _j < nrows_ ) && "Matrix::column: _j out of bounds!" );

    if ( ncols_ == 1 )
        {
            return *this;
        }
    else
        {
            auto mat = Matrix<T>( nrows_, 1 );

            for ( SQLNET_INT i = 0; i < nrows_; ++i )
                {
                    mat[i] = ( *this )( i, _j );
                }

            return mat;
        }
}

    // -----------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

template <class T>
containers::Matrix<T> containers::Matrix<T>::gather_root()
{
    if ( num_batches() == 1 )
        {
            // ---------------------------------------------------------------------
            // Set up MPI communicator

            boost::mpi::communicator comm_world;

            auto num_processes = comm_world.size();

            auto process_rank = comm_world.rank();

            // ---------------------------------------------------------------------
            // Having zero columns will lead to a division
            // by zero, if unchecked.

            if ( ncols() == 0 )
                {
                    SQLNET_INT global_nrows = 0;

                    boost::mpi::reduce(
                        comm_world,               // comm
                        nrows(),                  // in_values
                        global_nrows,             // out_values
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();

                    return containers::Matrix<T>( global_nrows, 0 );
                }

            // ---------------------------------------------------------------------
            // Calculate sendcounts

            std::vector<SQLNET_INT> sendcounts( num_processes );

            boost::mpi::all_gather(
                comm_world, nrows() * ncols(), sendcounts.data() );

            comm_world.barrier();

            // ---------------------------------------------------------------------
            // Calculate displs

            std::vector<SQLNET_INT> displs( num_processes + 1 );

            for ( SQLNET_SIZE i = 0; i < sendcounts.size(); ++i )
                {
                    displs[i + 1] = displs[i] + sendcounts[i];
                }

            // ---------------------------------------------------------------------
            // Gather data

            // Division by zero is checked above
            containers::Matrix<T> global_matrix(
                displs[num_processes] / ncols(),  // nrows
                ncols()                           // ncols
            );

            boost::mpi::gatherv(
                comm_world,
                data(),
                sendcounts[process_rank],
                global_matrix.data(),
                sendcounts,
                displs,
                0 );

            comm_world.barrier();

            // ---------------------------------------------------------------------

            return global_matrix;
        }
    else
        {
            // ---------------------------------------------------------------------
            // This recursive definition of gather(...) works, because
            // the subview created by batch( i ) has one single
            // batch by definition.

            containers::Matrix<T> global_matrix(
                0,       // nrows
                ncols()  // ncols
            );

            for ( SQLNET_SIZE i = 0; i < num_batches(); ++i )
                {
                    containers::Matrix<T> mat = batch( i );

                    mat = mat.gather_root();

                    global_matrix.append( mat );
                }

            // Because declaring the matrix using zero entries will create
            // and extra batch containing nothing, we drop that extra zero.
            global_matrix.batches().erase( global_matrix.batches().begin() );

            return global_matrix;
        }
}

#endif  // SQLNET_MULTINODE_MPI

    // -------------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

template <class T>
void containers::Matrix<T>::gather()
{
    if ( num_batches() == 1 )
        {
            // ---------------------------------------------------------------------
            // Set up MPI communicator

            boost::mpi::communicator comm_world;

            auto num_processes = comm_world.size();

            auto process_rank = comm_world.rank();

            // ---------------------------------------------------------------------
            // Having 0 columns will lead to a division
            // by zero in the root process, if unchecked.

            if ( ncols() == 0 )
                {
                    SQLNET_INT global_nrows = 0;

                    boost::mpi::reduce(
                        comm_world,               // comm
                        nrows(),                  // in_values
                        global_nrows,             // out_values
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();

                    return;
                }

            // ---------------------------------------------------------------------
            // Calculate sendcounts

            std::vector<SQLNET_INT> sendcounts( num_processes );

            boost::mpi::all_gather(
                comm_world, nrows() * ncols(), sendcounts.data() );

            comm_world.barrier();

            // ---------------------------------------------------------------------
            // Calculate displs

            std::vector<SQLNET_INT> displs( num_processes + 1 );

            for ( SQLNET_SIZE i = 0; i < sendcounts.size(); ++i )
                {
                    displs[i + 1] = displs[i] + sendcounts[i];
                }

            // ---------------------------------------------------------------------
            // Gather data

            boost::mpi::gatherv(
                comm_world,
                data(),
                sendcounts[process_rank],
                data(),  // just a pivot
                sendcounts,
                displs,
                0 );

            comm_world.barrier();

            // ---------------------------------------------------------------------
        }
    else
        {
            // This recursive definition of gather(...) works, because
            // the subview created by batch( i ) has one single
            // batch by definition.
            for ( SQLNET_SIZE i = 0; i < num_batches(); ++i )
                {
                    containers::Matrix<T> mat = batch( i );

                    mat.gather();
                }
        }
}

#endif  // SQLNET_MULTINODE_MPI

// -------------------------------------------------------------------------

template <class T>
void Matrix<T>::load( std::string _fname )
{
    std::ifstream input( _fname, std::ios::binary );

    // -------------------------------------------------------------------------
    // Read nrows

    debug_message( "Matrix.load: Read nrows..." );

    SQLNET_INT nrows = 0;

    input.read( reinterpret_cast<char *>( &nrows ), sizeof( SQLNET_INT ) );

    // -------------------------------------------------------------------------
    // Read ncols

    debug_message( "Matrix.load: Read ncols..." );

    SQLNET_INT ncols = 0;

    input.read( reinterpret_cast<char *>( &ncols ), sizeof( SQLNET_INT ) );

    // -------------------------------------------------------------------------
    // Read num_batches

    debug_message( "Matrix.load: Read num_batches..." );

    SQLNET_SIZE num_batches = 0;

    input.read(
        reinterpret_cast<char *>( &num_batches ), sizeof( SQLNET_SIZE ) );

    // -------------------------------------------------------------------------
    // Reverse byte order, if necessary

    if ( std::is_same<T, char>::value == false &&
         autosql::Endianness::is_little_endian() )
        {
            debug_message( "Matrix.load: Is little endian (1)..." );

            autosql::Endianness::reverse_byte_order( nrows );

            autosql::Endianness::reverse_byte_order( ncols );

            autosql::Endianness::reverse_byte_order( num_batches );
        }

    // -------------------------------------------------------------------------
    // Broadcast, ncols_ and num_batches if necessary

    {
#ifdef SQLNET_MULTINODE_MPI

        boost::mpi::communicator comm_world;

        boost::mpi::broadcast( comm_world, ncols, 0 );

        comm_world.barrier();

        boost::mpi::broadcast( comm_world, num_batches, 0 );

        comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI
    }

    // -------------------------------------------------------------------------
    // Read batches

    debug_message( "Matrix.load: Reading batches..." );

    std::vector<SQLNET_INT> batches( num_batches );

    input.read(
        reinterpret_cast<char *>( batches.data() ),
        num_batches * sizeof( SQLNET_INT ) );

    // -------------------------------------------------------------------------
    // Reverse byte order, if necessary

    if ( std::is_same<T, char>::value == false &&
         autosql::Endianness::is_little_endian() )
        {
            debug_message( "Matrix.load: Reverse byte order of batches..." );

            auto reverse_batches = []( SQLNET_INT &_val ) {
                autosql::Endianness::reverse_byte_order( _val );
            };

            std::for_each( batches.begin(), batches.end(), reverse_batches );
        }

    // -------------------------------------------------------------------------
    // Init matrix

    debug_message( "Matrix.load: Init matrix..." );

    *this = Matrix<T>( 0, ncols );

    // -------------------------------------------------------------------------
    // Read data

    debug_message( "Matrix.load: Read data..." );

    assert( batches.size() > 1 );

    for ( SQLNET_SIZE i = 0; i < batches.size() - 1; ++i )
        {
            containers::Matrix<T> mat( batches[i + 1] - batches[i], ncols );

            input.read(
                reinterpret_cast<char *>( mat.data() ),
                mat.nrows() * mat.ncols() * sizeof( T ) );

#ifdef SQLNET_MULTINODE_MPI

            mat = mat.scatter();

#endif  // SQLNET_MULTINODE_MPI

            append( mat );
        }

    // Because declaring the matrix using zero entries will create
    // and extra batch containing nothing, we drop that extra zero.
    this->batches().erase( this->batches().begin() );

    // -------------------------------------------------------------------------

    if ( std::is_same<T, char>::value == false &&
         autosql::Endianness::is_little_endian() )
        {
            debug_message( "Matrix.load: Is little endian (2)..." );

            // -------------------------------------------------------------------------
            // Reverse byte order of data

            debug_message( "Matrix.load: Reverse byte order of data..." );

            {
                auto reverse_data = []( T &_val ) {
                    autosql::Endianness::reverse_byte_order( _val );
                };

                std::for_each( begin(), end(), reverse_data );
            }

            // -------------------------------------------------------------------------
            // Read colnames and units

            {
                // ---------------------------------------------------------------------

                auto read_string = [&input]( std::string &_str ) {

                    SQLNET_SIZE str_size = 0;

                    input.read(
                        reinterpret_cast<char *>( &str_size ),
                        sizeof( SQLNET_SIZE ) );

                    autosql::Endianness::reverse_byte_order( str_size );

                    _str.resize( str_size );

                    input.read( &_str[0], str_size );

                };

                // ---------------------------------------------------------------------

                debug_message( "Matrix.load: Read colnames..." );

                assert(
                    static_cast<SQLNET_INT>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each(
                    colnames()->begin(), colnames()->end(), read_string );

                // ---------------------------------------------------------------------

                debug_message( "Matrix.load: Read units..." );

                assert(
                    static_cast<SQLNET_INT>( units()->size() ) ==
                    this->ncols() );

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

                    SQLNET_SIZE str_size = 0;

                    input.read(
                        reinterpret_cast<char *>( &str_size ),
                        sizeof( SQLNET_SIZE ) );

                    _str.resize( str_size );

                    input.read( &_str[0], str_size );

                };

                // ---------------------------------------------------------------------

                debug_message( "Matrix.load: Read colnames..." );

                assert(
                    static_cast<SQLNET_INT>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each(
                    colnames()->begin(), colnames()->end(), read_string );

                // ---------------------------------------------------------------------

                debug_message( "Matrix.load: Read units..." );

                assert(
                    static_cast<SQLNET_INT>( colnames()->size() ) ==
                    this->ncols() );

                std::for_each( units()->begin(), units()->end(), read_string );

                // ---------------------------------------------------------------------

                read_string( name() );

                // ---------------------------------------------------------------------
            }
            // -------------------------------------------------------------------------
        }

    // -------------------------------------------------------------------------
    // Broadcast, colnames_ and units_, if necessary

    {
#ifdef SQLNET_MULTINODE_MPI

        boost::mpi::communicator comm_world;

        debug_message( "Matrix.load: Broadcast colnames..." );

        boost::mpi::broadcast( comm_world, colnames().get()[0], 0 );

        comm_world.barrier();

        debug_message( "Matrix.load: Broadcast units..." );

        boost::mpi::broadcast( comm_world, units().get()[0], 0 );

        comm_world.barrier();

#endif  // SQLNET_MULTINODE_MPI
    }
}

    // -------------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

template <class T>
void Matrix<T>::load()
{
    // -------------------------------------------------------------

    boost::mpi::communicator comm_world;

    // -------------------------------------------------------------
    // Receive ncols_

    SQLNET_INT ncols = 0;

    boost::mpi::broadcast( comm_world, ncols, 0 );

    comm_world.barrier();

    // -------------------------------------------------------------
    // Receive num_batches

    SQLNET_SIZE num_batches = 0;

    boost::mpi::broadcast( comm_world, num_batches, 0 );

    comm_world.barrier();

    // -------------------------------------------------------------
    // Init matrix

    *this = Matrix<T>( 0, ncols );

    // -------------------------------------------------------------
    // Receive actual data

    for ( SQLNET_SIZE i = 0; i < num_batches - 1; ++i )
        {
            auto mat = scatter();

            append( mat );
        }

    // Because declaring the matrix using zero entries will create
    // and extra batch containing nothing, we drop that extra zero.
    batches().erase( batches().begin() );

    // -------------------------------------------------------------------------
    // Reverse byte order of data, if necessary

    if ( std::is_same<T, char>::value == false &&
         autosql::Endianness::is_little_endian() )
        {
            debug_message( "Matrix.load: Is little endian (2)..." );

            // -------------------------------------------------------------------------
            // Reverse byte order of data

            debug_message( "Matrix.load: Reverse byte order of data..." );

            {
                auto reverse_data = []( T &_val ) {
                    autosql::Endianness::reverse_byte_order( _val );
                };

                std::for_each( begin(), end(), reverse_data );
            }
        }

    // -------------------------------------------------------------
    // Receive colnames and units

    boost::mpi::broadcast( comm_world, colnames().get()[0], 0 );

    comm_world.barrier();

    boost::mpi::broadcast( comm_world, units().get()[0], 0 );

    comm_world.barrier();

    // -------------------------------------------------------------
}

#endif  // SQLNET_MULTINODE_MPI

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::remove_by_key( std::vector<bool> &_key )
{
    assert(
        static_cast<SQLNET_INT>( _key.size() ) == nrows() &&
        "Matrix: Size of keys must be identical to nrows!" );

    auto op = []( SQLNET_INT init, bool elem ) {
        return ( ( elem ) ? ( init ) : ( init + 1 ) );
    };

    SQLNET_INT nrows_new = std::accumulate( _key.begin(), _key.end(), 0, op );

    Matrix<T> trimmed( nrows_new, this->ncols() );

    SQLNET_INT k = 0;

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            if ( _key[i] == false )
                {
                    for ( SQLNET_INT j = 0; j < this->ncols(); ++j )
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
void Matrix<T>::save( std::string _fname ) const
{
    std::ofstream output( _fname, std::ios::binary );

    if ( std::is_same<T, char>::value == false &&
         autosql::Endianness::is_little_endian() )
        {
            debug_message( "Matrix.save: Is little endian..." );

            // -------------------------------------------------------------------------
            // Write nrows

            debug_message( "Matrix.save: Write nrows..." );

            {
                SQLNET_INT nrows = nrows_;

#ifdef SQLNET_MULTINODE_MPI

                {
                    boost::mpi::communicator comm_world;

                    SQLNET_PARALLEL_LIB::reduce(
                        comm_world,               // comm
                        nrows_,                   // in_value
                        nrows,                    // out_value
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();
                }

#endif  // SQLNET_MULTINODE_MPI

                autosql::Endianness::reverse_byte_order( nrows );

                output.write(
                    reinterpret_cast<const char *>( &nrows ),
                    sizeof( SQLNET_INT ) );
            }

            // -------------------------------------------------------------------------
            // Write ncols

            debug_message( "Matrix.save: Write ncols..." );

            {
                SQLNET_INT ncols = ncols_;

                autosql::Endianness::reverse_byte_order( ncols );

                output.write(
                    reinterpret_cast<const char *>( &ncols ),
                    sizeof( SQLNET_INT ) );
            }

            // -------------------------------------------------------------------------
            // Write num_batches

            debug_message( "Matrix.save: Write num_batches..." );

            assert( batches_ );

            {
                SQLNET_SIZE num_batches = batches_->size();

                autosql::Endianness::reverse_byte_order( num_batches );

                output.write(
                    reinterpret_cast<const char *>( &num_batches ),
                    sizeof( SQLNET_SIZE ) );
            }

            // -------------------------------------------------------------------------
            // Write batches

            debug_message( "Matrix.save: Write batches..." );

            assert( batches_ );

            auto batches = this->batches();

            {
            // ---------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

                {
                    boost::mpi::communicator comm_world;

                    SQLNET_PARALLEL_LIB::reduce(
                        comm_world,        // comm
                        batches().data(),  // in_values
                        static_cast<SQLNET_INT>( batches().size() ),  // count,
                        batches.data(),           // out_values
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();
                }

#endif  // SQLNET_MULTINODE_MPI

                auto write_inverted_batches = [&output]( SQLNET_INT &_val ) {

                    SQLNET_INT val_reversed = _val;

                    autosql::Endianness::reverse_byte_order( val_reversed );

                    output.write(
                        reinterpret_cast<const char *>( &val_reversed ),
                        sizeof( SQLNET_INT ) );
                };

                std::for_each(
                    batches.begin(), batches.end(), write_inverted_batches );

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
            // Write data

            debug_message( "Matrix.save: Write data..." );

            assert( data_ );

            {
                // ---------------------------------------------------------------------

                auto write_reversed_data = [&output]( T &_val ) {

                    T val_reversed = _val;

                    autosql::Endianness::reverse_byte_order( val_reversed );

                    output.write(
                        reinterpret_cast<const char *>( &val_reversed ),
                        sizeof( T ) );
                };

                // ---------------------------------------------------------------------

                for ( SQLNET_SIZE i = 0; i < batches.size() - 1; ++i )
                    {
                        auto mat = batch( i );

                        assert( mat.ncols() == this->ncols() );

#ifdef SQLNET_MULTINODE_MPI

                        mat = mat.gather_root();

#endif  // SQLNET_MULTINODE_MPI

                        std::for_each(
                            mat.begin(), mat.end(), write_reversed_data );
                    }

                // ---------------------------------------------------------------------
            }

            // -------------------------------------------------------------------------
            // Write colnames, units and name

            debug_message( "Matrix.save: Write colnames and units..." );

            {
                // ---------------------------------------------------------------------

                auto write_string = [&output]( std::string &_str ) {

                    SQLNET_SIZE str_size = _str.size();

                    autosql::Endianness::reverse_byte_order( str_size );

                    output.write(
                        reinterpret_cast<const char *>( &str_size ),
                        sizeof( SQLNET_SIZE ) );

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
            debug_message( "Matrix.save: Is big endian..." );

            // -------------------------------------------------------------------------
            // Write nrows

            {
                debug_message( "Matrix.save: Write nrows..." );

                SQLNET_INT nrows = nrows_;

#ifdef SQLNET_MULTINODE_MPI

                {
                    boost::mpi::communicator comm_world;

                    SQLNET_PARALLEL_LIB::reduce(
                        comm_world,               // comm
                        nrows_,                   // in_value
                        nrows,                    // out_value
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();
                }

#endif  // SQLNET_MULTINODE_MPI

                output.write(
                    reinterpret_cast<const char *>( &nrows ),
                    sizeof( SQLNET_INT ) );
            }

            // -------------------------------------------------------------------------
            // Write ncols

            debug_message( "Matrix.save: Write ncols..." );

            output.write(
                reinterpret_cast<const char *>( &( ncols_ ) ),
                sizeof( SQLNET_INT ) );

            // -------------------------------------------------------------------------
            // Write num_batches

            debug_message( "Matrix.save: Write num_batches..." );

            {
                SQLNET_SIZE num_batches = batches_->size();

                output.write(
                    reinterpret_cast<const char *>( &num_batches ),
                    sizeof( SQLNET_SIZE ) );
            }

            // -------------------------------------------------------------------------
            // Write batches

            auto batches = this->batches();

            {
                debug_message( "Matrix.save: Write batches..." );

#ifdef SQLNET_MULTINODE_MPI

                {
                    boost::mpi::communicator comm_world;

                    SQLNET_PARALLEL_LIB::reduce(
                        comm_world,        // comm
                        batches().data(),  // in_values
                        static_cast<SQLNET_INT>( batches().size() ),  // count,
                        batches.data(),           // out_values
                        std::plus<SQLNET_INT>(),  // op
                        0                         // root
                    );

                    comm_world.barrier();
                }

#endif  // SQLNET_MULTINODE_MPI

                output.write(
                    reinterpret_cast<const char *>( batches.data() ),
                    batches.size() * sizeof( SQLNET_INT ) );
            }

            // -------------------------------------------------------------------------
            // Write data

            debug_message( "Matrix.save: Write data..." );

            for ( SQLNET_SIZE i = 0; i < batches.size() - 1; ++i )
                {
                    auto mat = batch( i );

#ifdef SQLNET_MULTINODE_MPI

                    mat = mat.gather_root();

#endif  // SQLNET_MULTINODE_MPI

                    output.write(
                        reinterpret_cast<const char *>( mat.data() ),
                        mat.nrows() * mat.ncols() * sizeof( T ) );
                }

            // -------------------------------------------------------------------------
            // Write colnames and units

            debug_message( "Matrix.save: Write colnames and units..." );

            {
                // ---------------------------------------------------------------------

                auto write_string = [&output]( std::string &_str ) {

                    SQLNET_SIZE str_size = _str.size();

                    output.write(
                        reinterpret_cast<const char *>( &str_size ),
                        sizeof( SQLNET_SIZE ) );

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

#ifdef SQLNET_MULTINODE_MPI

template <class T>
void Matrix<T>::save()
{
    boost::mpi::communicator comm_world;

    // -------------------------------------------------------------------------
    // Send nrows

    SQLNET_PARALLEL_LIB::reduce(
        comm_world,               // comm
        nrows_,                   // in_value
        std::plus<SQLNET_INT>(),  // op
        0                         // root
    );

    comm_world.barrier();

    // -------------------------------------------------------------------------
    // Send batches

    SQLNET_PARALLEL_LIB::reduce(
        comm_world,                                   // comm
        batches().data(),                             // in_values
        static_cast<SQLNET_INT>( batches().size() ),  // count,
        std::plus<SQLNET_INT>(),                      // op
        0                                             // root
    );

    comm_world.barrier();

    // -------------------------------------------------------------------------
    // Send data in batchwise fashion

    for ( SQLNET_SIZE i = 0; i < batches().size() - 1; ++i )
        {
            auto mat = batch( i );

            mat.gather();
        }
}

    // -------------------------------------------------------------------------

#endif  // SQLNET_MULTINODE_MPI

    // -------------------------------------------------------------------------

#ifdef SQLNET_MULTINODE_MPI

template <class T>
Matrix<T> Matrix<T>::scatter()
{
    // ---------------------------------------------------------------------
    // Set up MPI communicator

    boost::mpi::communicator comm_world;

    auto num_processes = comm_world.size();

    auto process_rank = comm_world.rank();

    // ---------------------------------------------------------------------
    // Set up shape

    std::vector<SQLNET_INT> shape( 2 );

    shape[0] = nrows();

    shape[1] = ncols();

    // ---------------------------------------------------------------------
    // Broadcast shape

    debug_message( "Broadcast shape... " );

    boost::mpi::broadcast( comm_world, shape, 0 );

    comm_world.barrier();

    // ---------------------------------------------------------------------
    // Calculate sendcounts and displs

    debug_message( "Calculate sendcounts and displs... " );

    std::vector<SQLNET_INT> sendcounts( num_processes );

    std::vector<SQLNET_INT> displs( num_processes + 1 );

    Sendcounts::calculate_sendcounts_and_displs(
        shape[0],    // nrows
        shape[1],    // ncols
        sendcounts,  // sendcounts
        displs       // displs
    );

    // ---------------------------------------------------------------------
    // calculate_sendcounts_and_displs...) will only
    // multiply sendcounts by ncols when ncols is
    // greater than 0. So this will lead to the
    // correct sizes of the local matrices.

    debug_message( "Scattering... " );

    containers::Matrix<T> local_matrix( sendcounts[process_rank], 0 );

    if ( shape[1] > 0 )
        {
            local_matrix = containers::Matrix<T>(
                sendcounts[process_rank] / shape[1], shape[1] );

            if ( process_rank == 0 )
                {
                    boost::mpi::scatterv(
                        comm_world,
                        data(),
                        sendcounts,
                        displs,
                        local_matrix.data(),
                        sendcounts[process_rank],
                        0 );
                }
            else
                {
                    boost::mpi::scatterv(
                        comm_world,
                        local_matrix.data(),
                        sendcounts[process_rank],
                        0 );
                }

            comm_world.barrier();
        }

    // ---------------------------------------------------------------------

    return local_matrix;

    // ---------------------------------------------------------------------
}

#endif  // SQLNET_MULTINODE_MPI

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::sort_by_key( std::vector<SQLNET_INT> &_key )
{
    Matrix<SQLNET_INT> key(
        static_cast<SQLNET_INT>( _key.size() ), 1, _key.data() );

    return sort_by_key( key );
}

// -------------------------------------------------------------------------

template <class T>
Matrix<T> Matrix<T>::sort_by_key( Matrix<SQLNET_INT> &_key )
{
    assert(
        _key.nrows() == nrows() &&
        "Matrix: Size of keys must be identical to nrows!" );

    Matrix<T> sorted( nrows(), ncols() );

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            assert(
                _key[i] >= 0 && _key[i] < nrows() &&
                "Matrix: Key out of bounds!" );

            for ( SQLNET_INT j = 0; j < ncols(); ++j )
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
inline Matrix<T> Matrix<T>::subview( T2 _row_begin, T2 _row_end )
{
    assert(
        ( _row_begin >= 0 && _row_begin < nrows_ ) &&
        "Matrix::subview: _row_begin out of bounds!" );

    assert(
        ( _row_end >= 0 && _row_end <= nrows_ ) &&
        "Matrix::subview: _row_end out of bounds!" );

    assert( ( _row_end >= _row_begin ) && "Matrix::subview!" );

    auto mat = Matrix<T>(
        static_cast<SQLNET_INT>( _row_end - _row_begin ),
        ncols_,
        data_ptr_ +
            static_cast<SQLNET_UNSIGNED_LONG>( _row_begin ) * ncols_long_ );

    mat.set_colnames( *( colnames_.get() ) );

    mat.set_units( *( units_ ) );

    mat.name() = name();

    return mat;
}

// -------------------------------------------------------------------------

template <class T>
template <class T2>
inline const Matrix<T> Matrix<T>::subview( T2 _row_begin, T2 _row_end ) const
{
    assert(
        ( _row_begin >= 0 && _row_begin < nrows_ ) &&
        "Matrix::subview: _row_begin out of bounds!" );

    assert(
        ( _row_end >= 0 && _row_end <= nrows_ ) &&
        "Matrix::subview: _row_end out of bounds!" );

    assert( ( _row_end >= _row_begin ) && "Matrix::subview!" );

    auto mat = Matrix<T>(
        static_cast<SQLNET_INT>( _row_end - _row_begin ),
        ncols_,
        data_ptr_ +
            static_cast<SQLNET_UNSIGNED_LONG>( _row_begin ) * ncols_long_ );

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
Matrix<T> Matrix<T>::transpose()
{
    Matrix<T> transposed( ncols_, nrows_ );

    for ( SQLNET_UNSIGNED_LONG i = 0; i < nrows_long_; ++i )
        {
            for ( SQLNET_UNSIGNED_LONG j = 0; j < ncols_long_; ++j )
                {
                    transposed( j, i ) = ( *this )( i, j );
                }
        }

    return transposed;
}

// -------------------------------------------------------------------------
}
}

#endif // AUTOSQL_CONTAINER_MATRIX_HPP_

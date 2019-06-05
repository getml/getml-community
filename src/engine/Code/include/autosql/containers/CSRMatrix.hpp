#ifndef AUTOSQL_CONTAINER_CSRMATRIX_HPP_
#define AUTOSQL_CONTAINER_CSRMATRIX_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class CSRMatrix : public Container<T>
{
   public:
    CSRMatrix(
        SQLNET_INT _nrows = 0,
        SQLNET_INT _ncols = 0,
        SQLNET_INT _num_non_zero = 0 )
        : Container<T>( _nrows, _ncols )
    {
        init( _nrows, _ncols, _num_non_zero );
    }

    CSRMatrix(
        SQLNET_INT _nrows,
        SQLNET_INT _ncols,
        SQLNET_INT _num_non_zero,
        T* _data_ptr,
        SQLNET_INT* _indices_ptr )
        : Container<T>( _nrows, _ncols )
    {
        Container<T>::init( _nrows, _ncols );

        num_non_zero_ = _num_non_zero;

        data_ptr_ = _data_ptr;

        indices_ptr_ = _indices_ptr;

        indptr_ = std::shared_ptr<std::vector<SQLNET_INT> >(
            new std::vector<SQLNET_INT>( nrows_ + 1 ) );

        indptr_ptr_ = indptr_.get()->data();

        type_ = "CSRMatrix";
    }

    // -------------------------------

    // Append a new CSR matrix
    void append( CSRMatrix<T>& _other );

    // Sets nrows_, ncols_, num_non_zero_ to zero and intialises
    // data_ with an empty vector
    void clear();

    // Loads the CSRMatrix from binary format
    void load( std::string _fname );

    // Returns a value from data_ or yero
    template <class T2>
    const T operator()( T2 i, T2 j );

    // Sorts the rows of the CSR-matrix by the key provided
    CSRMatrix<T> remove_by_key( std::vector<bool>& _key );

    // Saves the CSRMatrix in binary format
    void save( std::string _fname );

    // Sorts the rows of the CSR-matrix by the key provided
    CSRMatrix<T> sort_by_key( Matrix<SQLNET_INT>& _key );
    CSRMatrix<T> sort_by_key( std::vector<SQLNET_INT>& _key );

    // Sort the indices of the CSRMatrix
    CSRMatrix<T> sort_indices();

    // Returns a shallow copy of a subselection of rows.
    // (But there is a deep copy of indptr)
    template <class T2>
    inline CSRMatrix<T> subview( T2 _row_begin, T2 _row_end );

    // -------------------------------

    template <class T2>
    CSRMatrix<T> batch( const T2 _batch_num )
    {
        return subview(
            batches()[_batch_num], batches()[_batch_num + 1] );
    }

    inline T* begin() { return data_ptr_; }

    inline T* end() { return data_ptr_ + num_non_zero_; }

    inline SQLNET_INT* indices() { return indices_ptr_; }

    inline SQLNET_INT* indptr() { return indptr_ptr_; }

    inline SQLNET_INT num_non_zero() const { return num_non_zero_; }

    inline SQLNET_SIZE size() const
    {
        return static_cast<SQLNET_SIZE>( num_non_zero_ );
    }

    // -------------------------------

   private:
    void init( SQLNET_INT _nrows, SQLNET_INT _ncols, SQLNET_INT _num_non_zero );

    // -------------------------------

    std::shared_ptr<std::vector<SQLNET_INT> > indices_;

    SQLNET_INT* indices_ptr_;

    std::shared_ptr<std::vector<SQLNET_INT> > indptr_;

    SQLNET_INT* indptr_ptr_;

    SQLNET_INT num_non_zero_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void CSRMatrix<T>::append( CSRMatrix<T>& _other )
{
    assert(
        _other.ncols() >= ncols() &&
        "CSRMatrix: The number of columns needs be greater or equal for "
        "append()!" );

    assert(
        ( data_ptr_ == data_.get()->data() || size() == 0 ) &&
        "CSRMatrix: Append does not work for externally managed data()!" );

    // ----------------------------------------------------------------------
    // Insert data

    data_.get()->insert(
        data_.get()->end(), _other.begin(), _other.end() );

    // ----------------------------------------------------------------------
    // Insert indices

    indices_.get()->insert(
        indices_.get()->end(),
        _other.indices(),
        _other.indices() + _other.num_non_zero() );

    // ----------------------------------------------------------------------
    // Insert indptr - this is a bit more complicated, because of the nature of
    // inptr.

    std::vector<SQLNET_INT> indptr( _other.nrows() + 1 );

    std::copy(
        _other.indptr(), _other.indptr() + _other.nrows() + 1, indptr.begin() );

    std::for_each( indptr.begin(), indptr.end(), [this]( SQLNET_INT& i ) {
        i += indptr()[nrows()];  // indptr()[nrows()] =
                                             // last element
    } );

    // indptr_.get()->begin() + nrows() = iterator at last element
    indptr_.get()->erase( indptr_.get()->begin() + nrows() );

    indptr_.get()->insert(
        indptr_.get()->end(), indptr.begin(), indptr.end() );

    // ----------------------------------------------------------------------
    // Reassign pointers

    data_ptr_ = data_.get()->data();

    indices_ptr_ = indices_.get()->data();

    indptr_ptr_ = indptr_.get()->data();

    // ----------------------------------------------------------------------
    // Increase nrows and batches

    nrows_ += _other.nrows();

    batches().push_back( nrows_ );

    // ----------------------------------------------------------------------
    // For CSR Matrices, it is possible that the number
    // of columns increases with append (unlike normal matrices)

    ncols_ = _other.ncols();
}

// -------------------------------------------------------------------------

template <class T>
void CSRMatrix<T>::clear()
{
    init( 0, 0, 0 );
}

// -------------------------------------------------------------------------

template <class T>
void CSRMatrix<T>::init(
    SQLNET_INT _nrows, SQLNET_INT _ncols, SQLNET_INT _num_non_zero )
{
    Container<T>::init( _nrows, _ncols );

    num_non_zero_ = _num_non_zero;

    data_ = std::shared_ptr<std::vector<T> >(
        new std::vector<T>( num_non_zero_ ) );

    data_ptr_ = data_.get()->data();

    indices_ = std::shared_ptr<std::vector<SQLNET_INT> >(
        new std::vector<SQLNET_INT>( num_non_zero_ ) );

    indices_ptr_ = indices_.get()->data();

    indptr_ = std::shared_ptr<std::vector<SQLNET_INT> >(
        new std::vector<SQLNET_INT>( nrows_ + 1 ) );

    indptr_ptr_ = indptr_.get()->data();

    type_ = "CSRMatrix";
}

// -------------------------------------------------------------------------

template <class T>
void CSRMatrix<T>::load( std::string _fname )
{
    std::ifstream input( _fname, std::ios::binary );

    input.read(
        reinterpret_cast<char*>( &( nrows_ ) ), sizeof( SQLNET_INT ) );

    input.read(
        reinterpret_cast<char*>( &( ncols_ ) ), sizeof( SQLNET_INT ) );

    input.read(
        reinterpret_cast<char*>( &( num_non_zero_ ) ),
        sizeof( SQLNET_INT ) );

    init( nrows_, ncols_, num_non_zero_ );

    input.read(
        reinterpret_cast<char*>( data() ),
        num_non_zero_ * sizeof( T ) );

    input.read(
        reinterpret_cast<char*>( indices() ),
        num_non_zero_ * sizeof( SQLNET_INT ) );

    input.read(
        reinterpret_cast<char*>( indptr() ),
        ( nrows_ + 1 ) * sizeof( SQLNET_INT ) );
}

// -------------------------------------------------------------------------

template <class T>
template <class T2>
const T CSRMatrix<T>::operator()( T2 _i, T2 _j )
{
    assert( _i >= 0 && static_cast<SQLNET_INT>( _i ) < nrows() );
    assert( _j >= 0 && static_cast<SQLNET_INT>( _j ) < ncols() );

    for ( SQLNET_INT k = indptr()[_i]; k < indptr()[_i + 1]; ++k )
        {
            if ( indices()[k] == _j )
                {
                    return data()[k];
                }
            else if ( indices()[k] > _j )
                {
                    return static_cast<T>( 0 );
                }
        }

    return static_cast<T>( 0 );
    ;
}

// -------------------------------------------------------------------------

template <class T>
CSRMatrix<T> CSRMatrix<T>::remove_by_key( std::vector<bool>& _key )
{
    if ( static_cast<SQLNET_INT>( _key.size() ) != nrows() )
        {
            throw std::invalid_argument(
                "CSRMatrix: Size of keys must be identical to nrows!" );
        }

    auto op = []( SQLNET_INT init, bool elem ) {
        return ( ( elem ) ? ( init ) : ( init + 1 ) );
    };

    SQLNET_INT nrows_new = std::accumulate( _key.begin(), _key.end(), 0, op );

    SQLNET_INT num_non_zero_new = 0;

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            if ( _key[i] == false )
                {
                    num_non_zero_new +=
                        indptr()[i + 1] - indptr()[i];
                }
        }

    CSRMatrix<T> trimmed( nrows_new, ncols(), num_non_zero_new );

    SQLNET_INT k = 0;

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            if ( _key[i] == false )
                {
                    // Transfer indptr
                    trimmed.indptr()[k + 1] = trimmed.indptr()[k] +
                                              indptr()[i + 1] -
                                              indptr()[i];

                    // Transfer data
                    for ( SQLNET_INT j = 0;
                          j < indptr()[i + 1] - indptr()[i];
                          ++j )
                        {
                            trimmed.data()[trimmed.indptr()[k] + j] =
                                data()[indptr()[i] + j];
                        }

                    // Transfer indices
                    for ( SQLNET_INT j = 0;
                          j < indptr()[i + 1] - indptr()[i];
                          ++j )
                        {
                            trimmed.indices()[trimmed.indptr()[k] + j] =
                                indices()[indptr()[i] + j];
                        }

                    k++;
                }
        }

    return trimmed;
}

// -------------------------------------------------------------------------

template <class T>
void CSRMatrix<T>::save( std::string _fname )
{
    std::ofstream output( _fname, std::ios::binary );

    output.write(
        reinterpret_cast<const char*>( &( nrows_ ) ),
        sizeof( SQLNET_INT ) );

    output.write(
        reinterpret_cast<const char*>( &( ncols_ ) ),
        sizeof( SQLNET_INT ) );

    output.write(
        reinterpret_cast<const char*>( &( num_non_zero_ ) ),
        sizeof( SQLNET_INT ) );

    output.write(
        reinterpret_cast<const char*>( data() ),
        num_non_zero_ * sizeof( T ) );

    output.write(
        reinterpret_cast<const char*>( indices() ),
        num_non_zero_ * sizeof( SQLNET_INT ) );

    output.write(
        reinterpret_cast<const char*>( indptr() ),
        ( nrows_ + 1 ) * sizeof( SQLNET_INT ) );
}

// -------------------------------------------------------------------------

template <class T>
CSRMatrix<T> CSRMatrix<T>::sort_by_key( std::vector<SQLNET_INT>& _key )
{
    Matrix<SQLNET_INT> key(
        static_cast<SQLNET_INT>( _key.size() ), 1, _key.data() );

    return sort_by_key( key );
}

// -------------------------------------------------------------------------

template <class T>
CSRMatrix<T> CSRMatrix<T>::sort_by_key( Matrix<SQLNET_INT>& _key )
{
    if ( _key.nrows() != nrows() )
        {
            throw std::invalid_argument(
                "Matrix: Size of keys must be identical to nrows!" );
        }

    // ------------------------------------------------------------------
    // Since it is not impossible that some keys are duplicates, we first
    // calculate num_non_zero_new.

    SQLNET_INT num_non_zero_new = 0;

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            num_non_zero_new +=
                indptr()[_key[i] + 1] - indptr()[_key[i]];
        }

    CSRMatrix<T> sorted( nrows(), ncols(), num_non_zero_new );

    // ------------------------------------------------------------------
    // Calculate number of elements in each row

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            if ( _key[i] < 0 || _key[i] >= nrows() )
                {
                    throw std::invalid_argument( "Matrix: Key out of bounds!" );
                }

            sorted.indptr()[i + 1] =
                indptr()[_key[i] + 1] - indptr()[_key[i]];
        }

    // --------------------------------------------------------------------
    // Accumulate number of elements in each row - result is proper indptr

    std::partial_sum(
        sorted.indptr(), sorted.indptr() + sorted.nrows(), sorted.indptr() );

    // --------------------------------------------------------------------
    // Now that we have a proper indptr we can transfer data() and indices()!

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            // Transfer data
            for ( SQLNET_INT j = 0;
                  j < sorted.indptr()[i + 1] - sorted.indptr()[i];
                  ++j )
                {
                    sorted.data()[sorted.indptr()[i] + j] =
                        data()[indptr()[_key[i]] + j];
                }

            // Transfer indices
            for ( SQLNET_INT j = 0;
                  j < indptr()[i + 1] - indptr()[i];
                  ++j )
                {
                    sorted.indices()[sorted.indptr()[i] + j] =
                        indices()[indptr()[_key[i]] + j];
                }
        }

    // --------------------------------------------------------------------

    return sorted;
}

// -------------------------------------------------------------------------

template <class T>
CSRMatrix<T> CSRMatrix<T>::sort_indices()
{
    std::vector<SQLNET_INT> indptr( 1 );
    std::vector<SQLNET_INT> indices;
    std::vector<T> data;

    // --------------------------------------------------------------------
    // We sort the indices by copying and the associated value into tuples
    // and sorting the tuples.

    for ( SQLNET_INT i = 0; i < nrows(); ++i )
        {
            std::vector<std::tuple<SQLNET_INT, T> > tuples;

            for ( SQLNET_INT j = indptr()[i]; j < indptr()[i + 1];
                  ++j )
                {
                    tuples.push_back( std::make_tuple(
                        indices()[j], data()[j] ) );
                }

            auto compare_op =
                []( const std::tuple<SQLNET_INT, T>& elem1,
                    const std::tuple<SQLNET_INT, T>& elem2 ) -> bool {
                return ( std::get<0>( elem1 ) < std::get<0>( elem2 ) );
            };

            std::sort( tuples.begin(), tuples.end(), compare_op );

            // --------------------------------------------------------------------
            // It is possible that there are indices which are duplicates. In
            // this case, we accumulate the data values.

            // Signifies that this is the first tuple in this row, so we do not
            // add data to the previous row.
            bool not_first = false;

            for ( auto& tup : tuples )
                {
                    if ( not_first && indices.back() == std::get<0>( tup ) )
                        {
                            data.back() += std::get<1>( tup );
                        }
                    else
                        {
                            indices.push_back( std::get<0>( tup ) );
                            data.push_back( std::get<1>( tup ) );
                            not_first = true;
                        }
                }

            indptr.push_back( static_cast<SQLNET_INT>( data.size() ) );
        }

    // --------------------------------------------------------------------
    // The rest is straight-forward - build a CSRMatrix and copy the results.

    auto sorted = CSRMatrix<T>( nrows(), ncols(), indptr.back() );

    std::copy( indices.begin(), indices.end(), sorted.indices() );

    std::copy( data.begin(), data.end(), sorted.data() );

    std::copy( indptr.begin(), indptr.end(), sorted.indptr() );

    // --------------------------------------------------------------------

    return sorted;
}

// -------------------------------------------------------------------------

template <class T>
template <class T2>
CSRMatrix<T> CSRMatrix<T>::subview( T2 _row_begin, T2 _row_end )
{
    assert(
        ( _row_begin >= 0 && _row_begin < nrows_ ) &&
        "CSRMatrix::subview: _row_begin out of bounds!" );

    assert(
        ( _row_end >= 0 && _row_end <= nrows_ ) &&
        "CSRMatrix::subview: _row_end out of bounds!" );

    assert( ( _row_end >= _row_begin ) && "CSRMatrix::subview!" );

    // --------------------------------------------------------------------
    // Shallow of data and indices.

    CSRMatrix<T> mat(
        _row_end - _row_begin,                                  // _nrows
        ncols(),                                          // _ncols
        indptr()[_row_end] - indptr()[_row_begin],  // _num_non_zero
        data() + indptr()[_row_begin],              // _data_ptr
        indices() + indptr()[_row_begin]            // _indices
    );

    // --------------------------------------------------------------------
    // We need to create a deep copy of indptr.

    std::copy(
        indptr() + _row_begin,
        indptr() + _row_end + 1,
        mat.indptr() );

    SQLNET_INT begin = mat.indptr()[0];

    std::for_each(
        mat.indptr(),
        mat.indptr() + mat.nrows() + 1,
        [begin]( SQLNET_INT& _i ) { _i -= begin; } );

    // --------------------------------------------------------------------
    // Don't forget colnames and units!

    mat.set_colnames( *( colnames_.get() ) );

    mat.set_units( *( units_.get() ) );

    // --------------------------------------------------------------------

    return mat;
}

// -------------------------------------------------------------------------
}
}

#endif // AUTOSQL_CONTAINER_CSRMATRIX_HPP_

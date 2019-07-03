#ifndef PREDICTORS_CONTAINERS_CSRMATRIX_HPP_
#define PREDICTORS_CONTAINERS_CSRMATRIX_HPP_

namespace predictors
{
// -----------------------------------------------------------------------------

template <
    typename DataType = Float,
    typename IndicesType = size_t,
    typename IndptrType = size_t>
class CSRMatrix
{
    // -----------------------------------------------------------

   public:
    /// Constructs an empty CSRMatrix.
    CSRMatrix() : indptr_( std::vector<IndptrType>( 1 ) ), ncols_( 0 ) {}

    /// Constructs a CSRMatrix from a discrete or numerical column.
    CSRMatrix( const CFloatColumn& _col );

    /// Constructs a CSRMatrix from a categorical column.
    CSRMatrix( const CIntColumn& _col, const size_t _n_unique );

    // -----------------------------------------------------------

   public:
    /// Adds a discrete or numerical column.
    void add( const CFloatColumn& _col );

    /// Adds a categorical column.
    void add( const CIntColumn& _col, const size_t _n_unique );

    // -----------------------------------------------------------

   public:
    /// Deletes all data in the CSRMatrix.
    void clear() { *this = CSRMatrix(); }

    /// Trivial accessor.
    DataType* data() { return data_.data(); }

    /// Trivial (const) accessor.
    const DataType* data() const { return data_.data(); }

    /// Trivial (const) accessor.
    const IndptrType* indptr() const { return indptr_.data(); }

    /// Trivial (const) accessor.
    const IndicesType* indices() const { return indices_.data(); }

    /// Trivial (const) accessor.
    const size_t ncols() const { return ncols_; }

    /// Trivial (const) accessor.
    const size_t nrows() const
    {
        assert( indptr_.size() != 0 );
        return indptr_.size() - 1;
    }

    /// Number of non-zero entries.
    const size_t size() const
    {
        assert( data_.size() == indices_.size() );
        return data_.size();
    }

    // -----------------------------------------------------------

   private:
    /// Contains the actual data.
    std::vector<DataType> data_;

    /// Pointers to where columns begin and end.
    std::vector<IndptrType> indptr_;

    /// Indicate the rows.
    std::vector<IndicesType> indices_;

    /// The number of columns in the CSRMatrix.
    size_t ncols_;

    // -----------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

namespace predictors
{
// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType>::CSRMatrix(
    const CFloatColumn& _col )
{
    assert( _col );

    data_ = std::vector<DataType>( _col->size() );

    for ( size_t i = 0; i < data_.size(); ++i )
        {
            data_[i] = static_cast<DataType>( ( *_col )[i] );
        }

    indices_ = std::vector<IndicesType>( _col->size() );

    indptr_ = std::vector<IndptrType>( _col->size() + 1 );

    for ( size_t i = 0; i < indptr_.size(); ++i )
        {
            indptr_[i] = static_cast<IndptrType>( i );
        }

    ncols_ = 1;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType>::CSRMatrix(
    const CIntColumn& _col, const size_t _n_unique )
{
    assert( _col );

    indptr_ = std::vector<IndptrType>( _col->size() + 1 );

    for ( size_t i = 0; i < _col->size(); ++i )
        {
            if ( ( *_col )[i] >= 0 )
                {
                    indices_.push_back(
                        static_cast<IndicesType>( ( *_col )[i] ) );

                    indptr_[i + 1] = indptr_[i] + 1;
                }
            else
                {
                    indptr_[i + 1] = indptr_[i];
                }
        }

    data_ = std::vector<DataType>( indices_.size(), 1.0 );

    ncols_ = _n_unique;
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
void CSRMatrix<DataType, IndicesType, IndptrType>::add(
    const CFloatColumn& _col )
{
    // -------------------------------------------------------------------------

    assert( _col );

    // -------------------------------------------------------------------------
    // If the CSRMatrix is empty, simply construct it from the column.

    if ( ncols() == 0 )
        {
            *this = CSRMatrix<DataType, IndicesType, IndptrType>( _col );
            return;
        }

    assert( _col->size() == nrows() );

    // -------------------------------------------------------------------------
    // Adapt data_.

    auto data_temp = std::vector<DataType>( data_.size() + _col->size() );

    for ( size_t i = 0; i < nrows(); ++i )
        {
            std::copy(
                data_.begin() + indptr_[i],
                data_.begin() + indptr_[i + 1],
                data_temp.begin() + indptr_[i] + i );

            data_temp[indptr_[i + 1] + i] =
                static_cast<DataType>( ( *_col )[i] );
        }

    data_ = std::move( data_temp );

    // -------------------------------------------------------------------------
    // Adapt indices_.

    auto indices_temp =
        std::vector<IndicesType>( indices_.size() + _col->size() );

    for ( size_t i = 0; i < nrows(); ++i )
        {
            std::copy(
                indices_.begin() + indptr_[i],
                indices_.begin() + indptr_[i + 1],
                indices_temp.begin() + indptr_[i] + i );

            indices_temp[indptr_[i + 1] + i] =
                static_cast<IndicesType>( ncols_ );
        }

    indices_ = std::move( indices_temp );

    // -------------------------------------------------------------------------
    // Adapt indptr_.

    for ( size_t i = 1; i <= nrows(); ++i )
        {
            indptr_[i] += i;
        }

    // -------------------------------------------------------------------------
    // Finally, we must increment ncols_.

    ++ncols_;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
void CSRMatrix<DataType, IndicesType, IndptrType>::add(
    const CIntColumn& _col, const size_t _n_unique )
{
    // -------------------------------------------------------------------------

    assert( _col );

    // -------------------------------------------------------------------------
    // If the CSRMatrix is empty, simply construct it from the column.

    if ( ncols() == 0 )
        {
            *this = CSRMatrix( _col, _n_unique );
            return;
        }

    assert( _col->size() == nrows() );

    // -------------------------------------------------------------------------
    // Count the number of non-negative entries in _col.

    auto is_non_negative = []( int val ) { return ( val >= 0 ); };

    const size_t num_non_negative =
        std::count_if( _col->begin(), _col->end(), is_non_negative );

    // -------------------------------------------------------------------------
    // Adapt data_ and indices_

    auto data_temp = std::vector<DataType>( data_.size() + num_non_negative );

    auto indices_temp =
        std::vector<IndicesType>( indices_.size() + num_non_negative );

    IndptrType num_added = 0;

    for ( size_t i = 0; i < nrows(); ++i )
        {
            std::copy(
                data_.begin() + indptr_[i],
                data_.begin() + indptr_[i + 1],
                data_temp.begin() + indptr_[i] + num_added );

            std::copy(
                indices_.begin() + indptr_[i],
                indices_.begin() + indptr_[i + 1],
                indices_temp.begin() + indptr_[i] + num_added );

            if ( ( *_col )[i] >= 0 )
                {
                    data_temp[indptr_[i + 1] + num_added] = 1.0;

                    indices_temp[indptr_[i + 1] + num_added] =
                        static_cast<IndicesType>( ( *_col )[i] ) + static_cast<IndicesType>( ncols() );

                    ++num_added;
                }
        }

    assert( num_added == num_non_negative );

    data_ = std::move( data_temp );

    indices_ = std::move( indices_temp );

    // -------------------------------------------------------------------------
    // Adapt indptr_.

    num_added = 0;

    for ( size_t i = 0; i < _col->size(); ++i )
        {
            if ( ( *_col )[i] >= 0 )
                {
                    ++num_added;
                }

            indptr_[i + 1] += num_added;
        }

    assert( num_added == num_non_negative );

    // -------------------------------------------------------------------------
    // Finally, we must adapt ncols_.

    ncols_ += _n_unique;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_CONTAINERS_CSRMATRIX_HPP_

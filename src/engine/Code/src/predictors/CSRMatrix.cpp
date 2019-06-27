#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

CSRMatrix::CSRMatrix( const CFloatColumn& _col )
{
    assert( _col );

    data_ = *_col;

    indices_ = std::vector<size_t>( _col->size() );

    indptr_ = std::vector<size_t>( _col->size() + 1 );

    for ( size_t i = 0; i < indptr_.size(); ++i )
        {
            indptr_[i] = i;
        }

    ncols_ = 1;
}

// -----------------------------------------------------------------------------

CSRMatrix::CSRMatrix( const CIntColumn& _col, const size_t _n_unique )
{
    assert( _col );

    indptr_ = std::vector<size_t>( _col->size() + 1 );

    for ( size_t i = 0; i < _col->size(); ++i )
        {
            if ( ( *_col )[i] >= 0 )
                {
                    indices_.push_back( static_cast<size_t>( ( *_col )[i] ) );

                    indptr_[i + 1] = indptr_[i] + 1;
                }
            else
                {
                    indptr_[i + 1] = indptr_[i];
                }
        }

    data_ = std::vector<Float>( indices_.size(), 1.0 );

    ncols_ += _n_unique;
}

// -----------------------------------------------------------------------------

void CSRMatrix::add( const CFloatColumn& _col )
{
    // -------------------------------------------------------------------------

    assert( _col );

    // -------------------------------------------------------------------------
    // If the CSRMatrix is empty, simply construct it from the column.

    if ( ncols() == 0 )
        {
            *this = CSRMatrix( _col );
            return;
        }

    assert( _col->size() == nrows() );

    // -------------------------------------------------------------------------
    // Adapt data_.

    auto data_temp = std::vector<Float>( data_.size() + _col->size() );

    for ( size_t i = 0; i < nrows(); ++i )
        {
            std::copy(
                data_.begin() + indptr_[i],
                data_.begin() + indptr_[i + 1],
                data_temp.begin() + indptr_[i] + i );

            data_temp[indptr_[i + 1] + i] = ( *_col )[i];
        }

    data_ = std::move( data_temp );

    // -------------------------------------------------------------------------
    // Adapt indices_.

    auto indices_temp = std::vector<size_t>( indices_.size() + _col->size() );

    for ( size_t i = 0; i < nrows(); ++i )
        {
            std::copy(
                indices_.begin() + indptr_[i],
                indices_.begin() + indptr_[i + 1],
                indices_temp.begin() + indptr_[i] + i );

            indices_temp[indptr_[i + 1] + i] = ncols_;
        }

    indices_ = std::move( indices_temp );

    // -------------------------------------------------------------------------
    // Adapt indptr_.

    std::for_each(
        indptr_.begin() + 1, indptr_.end(), []( size_t& _val ) { _val += 1; } );

    // -------------------------------------------------------------------------
    // Finally, we must increment ncols_.

    ++ncols_;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

void CSRMatrix::add( const CIntColumn& _col, const size_t _n_unique )
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

    auto data_temp = std::vector<Float>( data_.size() + num_non_negative );

    auto indices_temp =
        std::vector<size_t>( indices_.size() + num_non_negative );

    size_t num_added = 0;

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
                        static_cast<size_t>( ( *_col )[i] ) + ncols();

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

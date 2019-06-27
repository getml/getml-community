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

CSRMatrix::CSRMatrix( const CIntColumn& _col )
{
    assert( _col );

    data_ = std::vector<Float>( _col->size(), 1.0 );

    indices_ = std::vector<size_t>( _col->size() );

    for ( size_t i = 0; i < indices_.size(); ++i )
        {
            assert( ( *_col )[i] >= 0 );
            indices_[i] = static_cast<size_t>( ( *_col )[i] );
        }

    indptr_ = std::vector<size_t>( _col->size() + 1 );

    for ( size_t i = 0; i < indptr_.size(); ++i )
        {
            indptr_[i] = i;
        }

    ncols_ = 1;
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

void CSRMatrix::add( const CIntColumn& _col )
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

            data_temp[indptr_[i + 1] + i] = 1.0;
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

            assert( ( *_col )[i] >= 0 );

            indices_temp[indptr_[i + 1] + i] =
                static_cast<size_t>( ( *_col )[i] ) + ncols();
        }

    indices_ = std::move( indices_temp );

    // -------------------------------------------------------------------------
    // Adapt indptr_.

    std::for_each(
        indptr_.begin() + 1, indptr_.end(), []( size_t& _val ) { _val += 1; } );

    // -------------------------------------------------------------------------
    // Finally, we must adapt ncols_.

    const auto it = std::max_element( indices_.begin(), indices_.end() );

    ncols_ = *it + 1;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace predictors

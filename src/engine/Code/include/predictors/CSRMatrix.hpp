#ifndef PREDICTORS_CONTAINERS_CSRMATRIX_HPP_
#define PREDICTORS_CONTAINERS_CSRMATRIX_HPP_

namespace predictors
{
// -----------------------------------------------------------------------------

class CSRMatrix
{
    // -----------------------------------------------------------

   public:
    /// Constructs an empty CSRMatrix.
    CSRMatrix() : indptr_( std::vector<size_t>( 1 ) ), ncols_( 0 ) {}

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

    /// Trivial (const) accessor.
    const Float* data() const { return data_.data(); }

    /// Trivial (const) accessor.
    const size_t* indptr() const { return indptr_.data(); }

    /// Trivial (const) accessor.
    const size_t* indices() const { return indices_.data(); }

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
    std::vector<Float> data_;

    /// Pointers to where columns begin and end.
    std::vector<size_t> indptr_;

    /// Indicate the rows.
    std::vector<size_t> indices_;

    /// The number of columns in the CSRMatrix.
    size_t ncols_;

    // -----------------------------------------------------------
};

// -----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_CONTAINERS_CSRMATRIX_HPP_

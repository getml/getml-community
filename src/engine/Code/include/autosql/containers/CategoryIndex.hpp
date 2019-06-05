#ifndef AUTOSQL_CONTAINER_CATEGORYINDEX_HPP_
#define AUTOSQL_CONTAINER_CATEGORYINDEX_HPP_

namespace autosql
{
namespace containers
{
// -------------------------------------------------------------------------

/// Allows us to find categories quickly.
class CategoryIndex
{
   public:
    CategoryIndex(
        const std::vector<AUTOSQL_INT>& _categories,
        const AUTOSQL_SAMPLE_ITERATOR _begin,
        const AUTOSQL_SAMPLE_ITERATOR _end );

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor.
    AUTOSQL_SAMPLE_ITERATOR begin() const { return begin_; }

    /// Returns iterator to the beginning of a set of categories.
    AUTOSQL_SAMPLE_ITERATOR begin( const AUTOSQL_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<AUTOSQL_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    AUTOSQL_SAMPLE_ITERATOR end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    AUTOSQL_SAMPLE_ITERATOR end( const AUTOSQL_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<AUTOSQL_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_ + 1];
    }

    // -------------------------------

   private:
    /// Builds the indptr during construction of the CategoryIndex.
    static std::vector<AUTOSQL_INT> build_indptr(
        const std::vector<AUTOSQL_INT>& _categories,
        const AUTOSQL_SAMPLE_ITERATOR _begin,
        const AUTOSQL_SAMPLE_ITERATOR _end );

    // -------------------------------

   private:
    /// Points to the first sample.
    const AUTOSQL_SAMPLE_ITERATOR begin_;

    /// Points to the first sample.
    const AUTOSQL_SAMPLE_ITERATOR end_;

    /// Contains all categories that have been included.
    const std::vector<AUTOSQL_INT> indptr_;

    /// Minimum values of the samples.
    const AUTOSQL_INT minimum_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_CATEGORYINDEX_HPP_

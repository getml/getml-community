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
        const containers::MatchPtrs::iterator _begin,
        const containers::MatchPtrs::iterator _end );

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor.
    containers::MatchPtrs::iterator begin() const { return begin_; }

    /// Returns iterator to the beginning of a set of categories.
    containers::MatchPtrs::iterator begin( const AUTOSQL_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<AUTOSQL_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    containers::MatchPtrs::iterator end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    containers::MatchPtrs::iterator end( const AUTOSQL_INT _category ) const
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
        const containers::MatchPtrs::iterator _begin,
        const containers::MatchPtrs::iterator _end );

    // -------------------------------

   private:
    /// Points to the first sample.
    const containers::MatchPtrs::iterator begin_;

    /// Points to the first sample.
    const containers::MatchPtrs::iterator end_;

    /// Contains all categories that have been included.
    const std::vector<AUTOSQL_INT> indptr_;

    /// Minimum values of the samples.
    const AUTOSQL_INT minimum_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_CATEGORYINDEX_HPP_

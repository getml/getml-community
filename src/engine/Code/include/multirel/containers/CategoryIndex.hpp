#ifndef MULTIREL_CONTAINER_CATEGORYINDEX_HPP_
#define MULTIREL_CONTAINER_CATEGORYINDEX_HPP_

namespace multirel
{
namespace containers
{
// -------------------------------------------------------------------------

/// Allows us to find categories quickly.
class CategoryIndex
{
   public:
    CategoryIndex(
        const std::vector<Int>& _categories,
        const containers::MatchPtrs::iterator _begin,
        const containers::MatchPtrs::iterator _end );

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor.
    containers::MatchPtrs::iterator begin() const { return begin_; }

    /// Returns iterator to the beginning of a set of categories.
    containers::MatchPtrs::iterator begin( const Int _category ) const
    {
        assert_true( _category - minimum_ >= 0 );
        assert_true(
            _category - minimum_ + 1 < static_cast<Int>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    containers::MatchPtrs::iterator end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    containers::MatchPtrs::iterator end( const Int _category ) const
    {
        assert_true( _category - minimum_ >= 0 );
        assert_true(
            _category - minimum_ + 1 < static_cast<Int>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_ + 1];
    }

    // -------------------------------

   private:
    /// Builds the indptr during construction of the CategoryIndex.
    static std::vector<Int> build_indptr(
        const std::vector<Int>& _categories,
        const containers::MatchPtrs::iterator _begin,
        const containers::MatchPtrs::iterator _end );

    // -------------------------------

   private:
    /// Points to the first sample.
    const containers::MatchPtrs::iterator begin_;

    /// Points to the first sample.
    const containers::MatchPtrs::iterator end_;

    /// Contains all categories that have been included.
    const std::vector<Int> indptr_;

    /// Minimum values of the samples.
    const Int minimum_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINER_CATEGORYINDEX_HPP_

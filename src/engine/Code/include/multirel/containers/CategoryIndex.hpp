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
        containers::MatchPtrs&& _bins,
        std::vector<size_t>&& _indptr,
        const Int _minimum )
        : bins_( _bins ), indptr_( _indptr ), minimum_( _minimum )
    {
    }

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor.
    containers::MatchPtrs::const_iterator begin() const
    {
        return bins_.cbegin();
    }

    /// Returns iterator to the beginning of a set of categories.
    containers::MatchPtrs::const_iterator begin( const Int _cat ) const
    {
        assert_true( _cat >= minimum_ );
        const auto ix = static_cast<size_t>( _cat - minimum_ );
        assert_true( ix < indptr_.size() );
        assert_true( indptr_[ix] <= bins_.size() );
        return bins_.cbegin() + indptr_[ix];
    }

    /// Trivial accessor.
    containers::MatchPtrs::const_iterator end() const { return bins_.cend(); }

    /// Returns iterator to the end of a set of categories.
    containers::MatchPtrs::const_iterator end( const Int _cat ) const
    {
        assert_true( _cat >= minimum_ );
        const auto ix = static_cast<size_t>( _cat - minimum_ );
        assert_true( ix + 1 < indptr_.size() );
        assert_true( indptr_[ix + 1] >= indptr_[ix] );
        assert_true( indptr_[ix + 1] <= bins_.size() );
        return bins_.cbegin() + indptr_[ix + 1];
    }

    /// Returns the size of the underlying indptr.
    size_t size() const { return indptr_.size(); }

    // -------------------------------

   private:
    /// The bins themselves.
    const containers::MatchPtrs bins_;

    /// Indptr to the bins.
    const std::vector<size_t> indptr_;

    /// Minimum values of the categories.
    const Int minimum_;

    // -------------------------------
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace multirel

#endif  // MULTIREL_CONTAINER_CATEGORYINDEX_HPP_

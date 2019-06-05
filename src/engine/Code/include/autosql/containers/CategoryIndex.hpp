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
        const std::vector<SQLNET_INT>& _categories,
        const SQLNET_SAMPLE_ITERATOR _begin,
        const SQLNET_SAMPLE_ITERATOR _end );

    ~CategoryIndex() = default;

    // -------------------------------

   public:
    /// Trivial accessor.
    SQLNET_SAMPLE_ITERATOR begin() const { return begin_; }

    /// Returns iterator to the beginning of a set of categories.
    SQLNET_SAMPLE_ITERATOR begin( const SQLNET_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<SQLNET_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_];
    }

    /// Trivial accessor.
    SQLNET_SAMPLE_ITERATOR end() const { return end_; }

    /// Returns iterator to the end of a set of categories.
    SQLNET_SAMPLE_ITERATOR end( const SQLNET_INT _category ) const
    {
        assert( _category - minimum_ >= 0 );
        assert(
            _category - minimum_ + 1 <
            static_cast<SQLNET_INT>( indptr_.size() ) );
        return begin_ + indptr_[_category - minimum_ + 1];
    }

    // -------------------------------

   private:
    /// Builds the indptr during construction of the CategoryIndex.
    static std::vector<SQLNET_INT> build_indptr(
        const std::vector<SQLNET_INT>& _categories,
        const SQLNET_SAMPLE_ITERATOR _begin,
        const SQLNET_SAMPLE_ITERATOR _end );

    // -------------------------------

   private:
    /// Points to the first sample.
    const SQLNET_SAMPLE_ITERATOR begin_;

    /// Points to the first sample.
    const SQLNET_SAMPLE_ITERATOR end_;

    /// Contains all categories that have been included.
    const std::vector<SQLNET_INT> indptr_;

    /// Minimum values of the samples.
    const SQLNET_INT minimum_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

#endif  // AUTOSQL_CONTAINER_CATEGORYINDEX_HPP_

#ifndef BINNING_ROWNUMBINNER_HPP_
#define BINNING_ROWNUMBINNER_HPP_

// ----------------------------------------------------------------------------

namespace binning
{
// ----------------------------------------------------------------------------

template <class MatchType, class GetRownumType>
class RownumBinner
{
   public:
    static std::vector<size_t> bin(
        const size_t _nrows,
        const GetRownumType& _get_rownum,
        const std::vector<MatchType>::const_iterator _begin,
        const std::vector<MatchType>::const_iterator _end,
        std::vector<MatchType>* _bins );

   private:
    /// Generates the indptr, which indicates the beginning and end of
    /// each bin.
    static std::vector<size_t> make_indptr(
        const size_t _nrows,
        const GetRownumType& _get_rownum,
        const std::vector<MatchType>::const_iterator _begin,
        const std::vector<MatchType>::const_iterator _end );
};

// ----------------------------------------------------------------------------

template <class MatchType, class GetRownumType>
std::vector<size_t> RownumBinner<MatchType, GetRownumType>::bin(
    const size_t _nrows,
    const GetRownumType& _get_rownum,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _end,
    std::vector<MatchType>* _bins )
{
    // ---------------------------------------------------------------------------

    assert_true( _end >= _begin );

    assert_true(
        _bins->size() >= static_cast<size_t>( std::distance( _begin, _end ) ) );

    // ---------------------------------------------------------------------------

    const auto indptr = make_indptr( _nrows, _get_rownum, _begin, _end );

    // ---------------------------------------------------------------------------

    assert_true( indptr.size() == _nrows + 1 );

    std::vector<size_t> counts( _nrows + 1 );

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto ix = _get_rownum( *it );

            assert_true( ix >= 0 );
            assert_true( ix < _nrows );

            assert_true( indptr[ix] + counts[ix] < indptr[ix + 1] );

            *( _bins->begin() + indptr[ix] + counts[ix] ) = *it;

            ++counts[ix];
        }

    // ---------------------------------------------------------------------------

    return indptr;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class MatchType, class GetRownumType>
std::vector<size_t> RownumBinner<MatchType, GetRownumType>::make_indptr(
    const size_t _nrows,
    const GetRownumType& _get_rownum,
    const typename std::vector<MatchType>::const_iterator _begin,
    const typename std::vector<MatchType>::const_iterator _end )
{
    std::vector<size_t> indptr( _nrows + 1 );

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto ix = _get_rownum( *it );

            assert_true( ix >= 0 );
            assert_true( ix < _nrows );

            ++indptr[ix + 1];
        }

    std::partial_sum( indptr.begin(), indptr.end(), indptr.begin() );

    assert_true( indptr.front() == 0 );

    assert_true(
        indptr.back() == static_cast<size_t>( std::distance( _begin, _end ) ) );

    return indptr;
}

// ----------------------------------------------------------------------------
}  // namespace binning

#endif  // BINNING_ROWNUMBINNER_HPP_

#ifndef RELBOOST_UTILS_DISCRETEBINNER_HPP_
#define RELBOOST_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
class DiscreteBinner
{
   public:
    /// Bins the matches into _num_bins equal-width bins.
    /// The bins will be written into _bins_begin and the
    /// method returns and indptr to them.
    /// This assumes that min and max are known.
    static std::pair<std::vector<size_t>, Float> bin(
        const Float _min,
        const Float _max,
        const GetValueType& _get_value,
        const size_t _num_bins_numerical,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _nan_begin,
        const std::vector<containers::Match>::const_iterator _end,
        std::vector<containers::Match>* _bins );

   private:
    /// Generates the indptr, which indicates the beginning and end of
    /// each bin.
    static std::vector<size_t> make_indptr(
        const Float _min,
        const Float _max,
        const GetValueType& _get_value,
        const size_t _actual_num_bins,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _nan_begin );
};

// ----------------------------------------------------------------------------

template <class GetValueType>
std::pair<std::vector<size_t>, Float> DiscreteBinner<GetValueType>::bin(
    const Float _min,
    const Float _max,
    const GetValueType& _get_value,
    const size_t _num_bins_numerical,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _nan_begin,
    const std::vector<containers::Match>::const_iterator _end,
    std::vector<containers::Match>* _bins )
{
    // ---------------------------------------------------------------------------

    assert_true( _nan_begin >= _begin );
    assert_true( _end >= _nan_begin );

    assert_true(
        _bins->size() >= static_cast<size_t>( std::distance( _begin, _end ) ) );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( _min >= _max )
        {
            return std::make_pair( std::vector<size_t>( 0 ), 0.0 );
        }

    // ---------------------------------------------------------------------------
    // If the number of bins is too large, then use numerical binning.

    assert_true( _max >= _min );

    const auto actual_num_bins = static_cast<size_t>( _max - _min ) + 1;

    if ( actual_num_bins > _num_bins_numerical )
        {
            return NumericalBinner<GetValueType>::bin(
                _min,
                _max,
                _get_value,
                _num_bins_numerical,
                _begin,
                _nan_begin,
                _end,
                _bins );
        }

    // ---------------------------------------------------------------------------

    const auto indptr = make_indptr(
        _min, _max, _get_value, actual_num_bins, _begin, _nan_begin );

    // ---------------------------------------------------------------------------

    assert_true( indptr.size() == actual_num_bins + 1 );

    std::vector<size_t> counts( actual_num_bins );

    for ( auto it = _begin; it != _nan_begin; ++it )
        {
            const auto val = _get_value( *it );

            assert_true( !std::isnan( val ) && !std::isinf( val ) );

            assert_true( val <= _max );

            assert_true( val >= _min );

            const auto ix = static_cast<size_t>( _max - val );

            assert_true( ix < actual_num_bins );

            assert_true( indptr[ix] + counts[ix] < indptr[ix + 1] );

            *( _bins->begin() + indptr[ix] + counts[ix] ) = *it;

            ++counts[ix];
        }

    // ---------------------------------------------------------------------------

    assert_true( indptr.size() > 0 );

    std::copy( _nan_begin, _end, _bins->begin() + indptr.back() );

    // ---------------------------------------------------------------------------

    return std::make_pair( indptr, 1.0 );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class GetValueType>
std::vector<size_t> DiscreteBinner<GetValueType>::make_indptr(
    const Float _min,
    const Float _max,
    const GetValueType& _get_value,
    const size_t _actual_num_bins,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _nan_begin )
{
    std::vector<size_t> indptr( _actual_num_bins + 1 );

    for ( auto it = _begin; it != _nan_begin; ++it )
        {
            const auto val = _get_value( *it );

            assert_true( !std::isnan( val ) && !std::isinf( val ) );

            assert_true( val <= _max );

            assert_true( val >= _min );

            const auto ix = static_cast<size_t>( _max - val );

            assert_true( ix < _actual_num_bins );

            ++indptr[ix + 1];
        }

    std::partial_sum( indptr.begin(), indptr.end(), indptr.begin() );

    assert_true( indptr.front() == 0 );

    assert_true(
        indptr.back() ==
        static_cast<size_t>( std::distance( _begin, _nan_begin ) ) );

    return indptr;
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_DISCRETEBINNER_HPP_

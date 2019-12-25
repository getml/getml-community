#ifndef RELBOOST_UTILS_NUMERICALBINNER_HPP_
#define RELBOOST_UTILS_NUMERICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
class NumericalBinner
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
        const size_t _num_bins,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _end,
        std::vector<containers::Match>::iterator _bins_begin );

    /// Finds the minimum and the maximum value that is produced by
    /// _get_value.
    static std::pair<Float, Float> find_min_max(
        const GetValueType& _get_value,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _end,
        multithreading::Communicator* _comm );

   private:
    /// Generates the indptr, which indicates the beginning and end of
    /// each bin.
    static std::pair<std::vector<size_t>, Float> make_indptr(
        const Float _min,
        const Float _max,
        const GetValueType& _get_value,
        const size_t _num_bins,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _end );
};

// ----------------------------------------------------------------------------

template <class GetValueType>
std::pair<std::vector<size_t>, Float> NumericalBinner<GetValueType>::bin(
    const Float _min,
    const Float _max,
    const GetValueType& _get_value,
    const size_t _num_bins,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _end,
    std::vector<containers::Match>::iterator _bins_begin )
{
    // ---------------------------------------------------------------------------

    assert_true( _end >= _begin );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( _min >= _max )
        {
            return std::make_pair( std::vector<size_t>( 0 ), 0.0 );
        }

    // ---------------------------------------------------------------------------

    const auto [indptr, step_size] =
        make_indptr( _min, _max, _get_value, _num_bins, _begin, _end );

    // ---------------------------------------------------------------------------

    std::vector<size_t> counts( _num_bins );

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto val = _get_value( *it );

            const auto ix = static_cast<size_t>( ( _max - val ) / step_size );

            assert_true( ix < _num_bins );

            assert_true( indptr[ix] + counts[ix] < indptr[ix + 1] );

            *( _bins_begin + indptr[ix] + counts[ix] ) = *it;

            ++counts[ix];
        }

    // ---------------------------------------------------------------------------

    return std::make_pair( indptr, step_size );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

template <class GetValueType>
std::pair<Float, Float> NumericalBinner<GetValueType>::find_min_max(
    const GetValueType& _get_value,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _end,
    multithreading::Communicator* _comm )
{
    assert_true( _end >= _begin );

    Float min = std::numeric_limits<Float>::max();

    Float max = std::numeric_limits<Float>::lowest();

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto val = _get_value( *it );

            if ( val < min )
                {
                    min = val;
                }

            if ( val > max )
                {
                    max = val;
                }
        }

    utils::Reducer::reduce( multithreading::minimum<Float>(), &min, _comm );

    utils::Reducer::reduce( multithreading::maximum<Float>(), &max, _comm );

    return std::make_pair( min, max );
}

// ----------------------------------------------------------------------------

template <class GetValueType>
std::pair<std::vector<size_t>, Float>
NumericalBinner<GetValueType>::make_indptr(
    const Float _min,
    const Float _max,
    const GetValueType& _get_value,
    const size_t _num_bins,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _end )
{
    assert_true( _num_bins > 0 );

    std::vector<size_t> indptr( _num_bins + 1 );

    // Note: The 0.001 is substracted to make sure that min value
    // does not lead to an overflow.
    const Float step_size =
        ( _max - _min ) / ( static_cast<Float>( _num_bins ) - 0.001 );

    for ( auto it = _begin; it != _end; ++it )
        {
            const auto val = _get_value( *it );

            const auto ix = static_cast<size_t>( ( _max - val ) / step_size );

            assert_true( ix < _num_bins );

            ++indptr[ix + 1];
        }

    std::partial_sum( indptr.begin(), indptr.end(), indptr.begin() );

    assert_true( indptr.front() == 0 );

    assert_true(
        indptr.back() == static_cast<size_t>( std::distance( _begin, _end ) ) );

    return std::make_pair( indptr, step_size );
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

#endif  // RELBOOST_UTILS_NUMERICALBINNER_HPP_

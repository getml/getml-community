#ifndef RELMT_UTILS_MINMAXFINDER_HPP_
#define RELMT_UTILS_MINMAXFINDER_HPP_

// ----------------------------------------------------------------------------

namespace relmt
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType, class VType>
struct MinMaxFinder
{
    /// Finds the minimum and the maximum value that is produced by
    /// _get_value.
    static std::pair<VType, VType> find_min_max(
        const GetValueType& _get_value,
        const std::vector<containers::Match>::const_iterator _begin,
        const std::vector<containers::Match>::const_iterator _end,
        multithreading::Communicator* _comm );
};

// ----------------------------------------------------------------------------

template <class GetValueType, class VType>
std::pair<VType, VType> MinMaxFinder<GetValueType, VType>::find_min_max(
    const GetValueType& _get_value,
    const std::vector<containers::Match>::const_iterator _begin,
    const std::vector<containers::Match>::const_iterator _end,
    multithreading::Communicator* _comm )
{
    assert_true( _end >= _begin );

    Float min = std::numeric_limits<VType>::max();

    Float max = std::numeric_limits<VType>::lowest();

    for ( auto it = _begin; it != _end; ++it )
        {
            const VType val = _get_value( *it );

            assert_true( !std::isnan( val ) && !std::isinf( val ) );

            if ( val < min )
                {
                    min = val;
                }

            if ( val > max )
                {
                    max = val;
                }
        }

    utils::Reducer::reduce( multithreading::minimum<VType>(), &min, _comm );

    utils::Reducer::reduce( multithreading::maximum<VType>(), &max, _comm );

    return std::make_pair( min, max );
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relmt

#endif  // RELMT_UTILS_MINMAXFINDER_HPP_

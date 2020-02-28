#ifndef MULTIREL_UTILS_MINMAXFINDER_HPP_
#define MULTIREL_UTILS_MINMAXFINDER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class VType>
struct MinMaxFinder
{
    /// Finds the minimum and the maximum value that is produced by
    /// _get_value.
    static std::pair<VType, VType> find_min_max(
        const std::vector<containers::Match*>::iterator _begin,
        const std::vector<containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <class VType>
std::pair<VType, VType> MinMaxFinder<VType>::find_min_max(
    const std::vector<containers::Match*>::iterator _begin,
    const std::vector<containers::Match*>::iterator _end,
    multithreading::Communicator* _comm )
{
    assert_true( _end >= _begin );

    Float min = std::numeric_limits<VType>::max();

    Float max = std::numeric_limits<VType>::lowest();

    for ( auto it = _begin; it != _end; ++it )
        {
            auto val = static_cast<VType>( 0 );

            if constexpr ( std::is_same<VType, Float>() )
                {
                    val = ( *it )->numerical_value;
                }
            else if ( std::is_same<VType, Int>() )
                {
                    val = ( *it )->categorical_value;
                }
            else
                {
                    assert_true( false && "Unknown VType!" );
                }

            if constexpr ( std::is_same<VType, Float>() )
                {
                    assert_true( !std::isnan( val ) && !std::isinf( val ) );
                }

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
}  // namespace multirel

#endif  // MULTIREL_UTILS_MINMAXFINDER_HPP_

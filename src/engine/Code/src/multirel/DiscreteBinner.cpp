#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

std::pair<std::vector<size_t>, Float> DiscreteBinner::bin(
    const Float _min,
    const Float _max,
    const size_t _num_bins_numerical,
    const std::vector<containers::Match*>::const_iterator _begin,
    const std::vector<containers::Match*>::const_iterator _nan_begin,
    const std::vector<containers::Match*>::const_iterator _end,
    std::vector<containers::Match*>* _bins )
{
    // ---------------------------------------------------------------------------

    assert_true( !std::isnan( _max ) );
    assert_true( !std::isnan( _max ) );
    assert_true( !std::isinf( _min ) );
    assert_true( !std::isinf( _min ) );

    assert_true( _nan_begin >= _begin );
    assert_true( _end >= _nan_begin );

    assert_true(
        _bins->size() >= static_cast<size_t>( std::distance( _begin, _end ) ) );

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( _min >= _max || _num_bins_numerical == 0 )
        {
            return std::make_pair( std::vector<size_t>( 0 ), 0.0 );
        }

    // ---------------------------------------------------------------------------

    assert_true( _max >= _min );

    const auto step_size = std::ceil(
        ( _max - _min ) / static_cast<Float>( _num_bins_numerical ) );

    const auto indptr = NumericalBinner::bin_given_step_size(
        _min, _max, step_size, _begin, _nan_begin, _end, _bins );

    // ---------------------------------------------------------------------------

    return std::make_pair( indptr, step_size );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

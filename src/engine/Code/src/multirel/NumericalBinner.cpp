#include "multirel/utils/utils.hpp"

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

std::pair<std::vector<size_t>, Float> NumericalBinner::bin(
    const Float _min,
    const Float _max,
    const size_t _num_bins,
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

    // ---------------------------------------------------------------------------
    // There is a possibility that all critical values are NAN in all processes.
    // This accounts for this edge case.

    if ( _min >= _max || _num_bins == 0 )
        {
            return std::make_pair( std::vector<size_t>( 0 ), 0.0 );
        }

    // ---------------------------------------------------------------------------

    const auto step_size = ( _max - _min ) / static_cast<size_t>( _num_bins );

    // ---------------------------------------------------------------------------

    const auto indptr = bin_given_step_size(
        _min, _max, step_size, _begin, _nan_begin, _end, _bins );

    // ---------------------------------------------------------------------------

    return std::make_pair( indptr, step_size );

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<size_t> NumericalBinner::bin_given_step_size(
    const Float _min,
    const Float _max,
    const Float _step_size,
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

    if ( _min >= _max || _step_size <= 0.0 )
        {
            return std::vector<size_t>( 0 );
        }

    // ---------------------------------------------------------------------------

    const auto indptr =
        make_indptr( _min, _max, _step_size, _begin, _nan_begin );

    // ---------------------------------------------------------------------------

    assert_true( indptr.size() > 0 );

    std::vector<size_t> counts( indptr.size() - 1 );

    for ( auto it = _begin; it != _nan_begin; ++it )
        {
            const auto val = ( *it )->numerical_value;

            assert_true( !std::isnan( val ) && !std::isinf( val ) );

            assert_true( val <= _max );

            assert_true( val >= _min );

            const auto ix = static_cast<size_t>( ( _max - val ) / _step_size );

            assert_true( ix < counts.size() );

            assert_true( indptr[ix] + counts[ix] < indptr[ix + 1] );

            *( _bins->begin() + indptr[ix] + counts[ix] ) = *it;

            ++counts[ix];
        }

        // ---------------------------------------------------------------------------

#ifndef NDEBUG

    for ( size_t i = 0; i < counts.size(); ++i )
        {
            assert_true( indptr[i] + counts[i] == indptr[i + 1] );
        }

#endif  // NDEBUG

    // ---------------------------------------------------------------------------

    assert_true( indptr.size() > 0 );

    std::copy( _nan_begin, _end, _bins->begin() + indptr.back() );

    // ---------------------------------------------------------------------------

    return indptr;

    // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<size_t> NumericalBinner::make_indptr(
    const Float _min,
    const Float _max,
    const Float _step_size,
    const std::vector<containers::Match*>::const_iterator _begin,
    const std::vector<containers::Match*>::const_iterator _nan_begin )
{
    assert_true( _max >= _min );

    assert_true( _step_size > 0.0 );

    const auto num_bins =
        static_cast<size_t>( ( _max - _min ) / _step_size ) + 1;

    std::vector<size_t> indptr( num_bins + 1 );

    for ( auto it = _begin; it != _nan_begin; ++it )
        {
            const auto val = ( *it )->numerical_value;

            assert_true( !std::isnan( val ) && !std::isinf( val ) );

            assert_true( val <= _max );

            assert_true( val >= _min );

            const auto ix = static_cast<size_t>( ( _max - val ) / _step_size );

            assert_true( ix < num_bins );

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
}  // namespace multirel

#include "relboost/utils/utils.hpp"

namespace relboost
{
namespace utils
{
// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<RELBOOST_INT>> CriticalValueSorter::sort(
    const std::vector<containers::CandidateSplit>::iterator _begin,
    const std::vector<containers::CandidateSplit>::iterator _end )
{
    // ------------------------------------------------------------------------
    // Create tuples

    auto tuples = std::vector<std::tuple<RELBOOST_FLOAT, RELBOOST_INT>>( 0 );

    assert( _end >= _begin );

    for ( auto it = _begin; it != _end; ++it )
        {
            if ( std::isnan( std::get<1>( it->weights_ ) ) ||
                 std::isnan( std::get<2>( it->weights_ ) ) )
                {
                    continue;
                }

            const auto weight = std::get<1>( it->weights_ );

            assert(
                std::distance(
                    it->split_.categories_used_begin_,
                    it->split_.categories_used_end_ ) == 1 );

            const auto cv = *it->split_.categories_used_begin_;

            tuples.push_back( std::make_tuple( weight, cv ) );
        }

    // ------------------------------------------------------------------------
    // Sort the tuples by descending order of the
    // weights.

    const auto sort_tuples =
        []( const std::tuple<RELBOOST_FLOAT, RELBOOST_INT>& t1,
            const std::tuple<RELBOOST_FLOAT, RELBOOST_INT>& t2 ) {
            return std::get<0>( t1 ) > std::get<0>( t2 );
        };

    std::sort( tuples.begin(), tuples.end(), sort_tuples );

    // ------------------------------------------------------------------------
    // Copy into critical values.

    const auto sorted =
        std::make_shared<std::vector<RELBOOST_INT>>( tuples.size() );

    for ( size_t i = 0; i < tuples.size(); ++i )
        {
            ( *sorted )[i] = std::get<1>( tuples[i] );
        }

    // ------------------------------------------------------------------------

    return sorted;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost
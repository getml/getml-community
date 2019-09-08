#include "autosql/containers/containers.hpp"

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

CategoryIndex::CategoryIndex(
    const std::vector<Int>& _categories,
    const containers::MatchPtrs::iterator _begin,
    const containers::MatchPtrs::iterator _end )
    : begin_( _begin ),
      end_( _end ),
      indptr_( CategoryIndex::build_indptr( _categories, _begin, _end ) ),
      minimum_( _categories.size() > 0 ? _categories.front() : 0 )
{
#ifndef NDEBUG

    if ( indptr_.size() > 0 )
        {
            for ( auto cat = _categories.front(); cat <= _categories.back();
                  ++cat )
                {
                    for ( auto it = begin( cat ); it < end( cat ); ++it )
                        {
                            assert_true( ( *it )->categorical_value == cat );
                        }
                }
        }

#endif  // NDEBUG
}

// ----------------------------------------------------------------------------

std::vector<Int> CategoryIndex::build_indptr(
    const std::vector<Int>& _categories,
    const containers::MatchPtrs::iterator _begin,
    const containers::MatchPtrs::iterator _end )
{
    // ------------------------------------------------------------------------

    assert_true( _end >= _begin );

    if ( std::distance( _begin, _end ) == 0 || _categories.size() == 0 )
        {
            return std::vector<Int>( 0 );
        }

    // ------------------------------------------------------------------------

    const auto minimum = _categories.front();
    const auto maximum = _categories.back();

    assert_true( maximum >= minimum );

    const auto dist = static_cast<Int>( std::distance( _begin, _end ) );

    auto indptr = std::vector<Int>( maximum - minimum + 2 );

    // ------------------------------------------------------------------------

    Int i = 0;

    for ( auto cat = minimum; cat <= maximum + 1; ++cat )
        {
            while ( i < dist )
                {
                    assert_true(
                        i == 0 || _begin[i]->categorical_value >=
                                      _begin[i - 1]->categorical_value );

                    if ( _begin[i]->categorical_value < cat )
                        {
                            ++i;
                        }
                    else  // ( _begin[i]->categorical_value >= cat )
                        {
                            indptr[cat - minimum] = i;
                            break;
                        }
                }

            if ( i == dist )
                {
                    indptr[cat - minimum] = i;
                }
        }

        // ------------------------------------------------------------------------

#ifndef NDEBUG

    for ( auto val : indptr )
        {
            assert_true( val >= 0 );
            assert_true( val <= dist );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    return indptr;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

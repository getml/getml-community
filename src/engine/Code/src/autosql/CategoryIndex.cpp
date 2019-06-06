#include "autosql/containers/containers.hpp"

namespace autosql
{
namespace containers
{
// ----------------------------------------------------------------------------

CategoryIndex::CategoryIndex(
    const std::vector<AUTOSQL_INT>& _categories,
    const AUTOSQL_SAMPLE_ITERATOR _begin,
    const AUTOSQL_SAMPLE_ITERATOR _end )
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
                            assert( ( *it )->categorical_value == cat );
                        }
                }
        }

#endif  // NDEBUG
}

// ----------------------------------------------------------------------------

std::vector<AUTOSQL_INT> CategoryIndex::build_indptr(
    const std::vector<AUTOSQL_INT>& _categories,
    const AUTOSQL_SAMPLE_ITERATOR _begin,
    const AUTOSQL_SAMPLE_ITERATOR _end )
{
    // ------------------------------------------------------------------------

    assert( _end >= _begin );

    if ( std::distance( _begin, _end ) == 0 || _categories.size() == 0 )
        {
            return std::vector<AUTOSQL_INT>( 0 );
        }

    // ------------------------------------------------------------------------

    const auto minimum = _categories.front();
    const auto maximum = _categories.back();

    assert( maximum >= minimum );

    const auto dist = static_cast<AUTOSQL_INT>( std::distance( _begin, _end ) );

    auto indptr = std::vector<AUTOSQL_INT>( maximum - minimum + 2 );

    // ------------------------------------------------------------------------

    AUTOSQL_INT i = 0;

    for ( auto cat = minimum; cat <= maximum + 1; ++cat )
        {
            while ( i < dist )
                {
                    assert(
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
            assert( val >= 0 );
            assert( val <= dist );
        }

#endif  // NDEBUG

    // ------------------------------------------------------------------------

    return indptr;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace autosql

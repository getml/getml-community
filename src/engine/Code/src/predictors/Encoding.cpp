#include "predictors/predictors.hpp"

namespace predictors
{
// -----------------------------------------------------------------------------

CIntColumn Encoding::fit_transform( const CIntColumn& _col )
{
    assert( _col );

    auto output = std::make_shared<std::vector<Int>>( _col->size() );

    for ( size_t i = 0; i < _col->size(); ++i )
        {
            const auto val = ( *_col )[i];

            const auto it = map_.find( val );

            if ( it == map_.end() )
                {
                    ( *output )[i] = static_cast<Int>( size() );
                    map_[val] = ( *output )[i];
                    vector_.push_back( ( *output )[i] );
                }
            else
                {
                    ( *output )[i] = it->second;
                }
        }

    return output;
}

// -----------------------------------------------------------------------------

CIntColumn Encoding::transform( const CIntColumn& _col ) const
{
    assert( _col );

    auto output = std::make_shared<std::vector<Int>>( _col->size() );

    for ( size_t i = 0; i < _col->size(); ++i )
        {
            const auto val = ( *_col )[i];

            const auto it = map_.find( val );

            if ( it == map_.end() )
                {
                    ( *output )[i] = -1;
                }
            else
                {
                    ( *output )[i] = it->second;
                }
        }

    return output;
}
// -----------------------------------------------------------------------------
}  // namespace predictors

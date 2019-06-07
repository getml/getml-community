#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

void DataFrameIndex::calculate( const Matrix<ENGINE_INT> &_join_key )
{
    if ( _join_key.size() < begin_ )
        {
            map_->clear();
            begin_ = 0;
        }

    for ( size_t i = begin_; i < _join_key.nrows(); ++i )
        {
            if ( _join_key[i] >= 0 )
                {
                    auto it = map_->find( _join_key[i] );

                    if ( it == map_->end() )
                        {
                            ( *map_ )[_join_key[i]] = {i};
                        }
                    else
                        {
                            it->second.push_back( i );
                        }
                }
        }

    begin_ = _join_key.nrows();
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

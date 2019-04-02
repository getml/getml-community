#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

void Encoding::append( const Encoding& _other, bool _include_subencoding )
{
    for ( auto& elem : _other.vector_ )
        {
            ( *this )[elem];
        }

    if ( _include_subencoding && _other.subencoding_ )
        {
            append( *_other.subencoding_, true );
        }
}

// ----------------------------------------------------------------------------

size_t Encoding::insert( const std::string& _val )
{
    assert( map_.find( _val ) == map_.end() );

    const auto ix = static_cast<size_t>( vector_.size() + subsize_ );

    map_[_val] = ix;

    vector_.push_back( _val );

    return ix;
}

// ----------------------------------------------------------------------------

size_t Encoding::operator[]( const std::string& _val )
{
    // -----------------------------------
    // If this is a NULL value, return -1.

    if ( _val == "" || _val == "nan" || _val == "NaN" || _val == "NA" ||
         _val == "NULL" )
        {
            return -1;
        }

    // -----------------------------------
    // Note that the subencoding is const
    // - it cannot be updated.

    if ( subencoding_ )
        {
            const auto result = ( *subencoding_ )[_val];

            if ( result != -1 )
                {
                    return result;
                }
        }

    // -----------------------------------
    // If it cannot be found in the subencoding,
    // check/update your own values.

    const auto it = map_.find( _val );

    if ( it == map_.end() )
        {
            return insert( _val );
        }
    else
        {
            return it->second;
        }
}

// ----------------------------------------------------------------------------

size_t Encoding::operator[]( const std::string& _val ) const
{
    // -----------------------------------
    // If this is a NULL value, return -1.

    if ( _val == "" || _val == "nan" || _val == "NaN" || _val == "NA" ||
         _val == "NULL" )
        {
            return -1;
        }

    // -----------------------------------
    // Note that the subencoding is const
    // - it cannot be updated.

    if ( subencoding_ )
        {
            const auto result = ( *subencoding_ )[_val];

            if ( result != -1 )
                {
                    return result;
                }
        }

    // -----------------------------------
    // If it cannot be found in the subencoding,
    // check your own values.

    const auto it = map_.find( _val );

    if ( it == map_.end() )
        {
            return -1;
        }
    else
        {
            return it->second;
        }
}

// ----------------------------------------------------------------------------

Encoding& Encoding::operator=( std::vector<std::string>&& _vector ) noexcept
{
    assert( !subencoding_ );

    vector_ = _vector;

    map_.clear();

    for ( size_t ix = 0; ix < static_cast<size_t>( vector_.size() ); ++ix )
        {
            auto& val = vector_[ix];

            assert( map_.find( val ) == map_.end() );

            map_[val] = ix;
        }

    return *this;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

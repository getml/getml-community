#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

void Encoding::append( const Encoding& _other, bool _include_subencoding )
{
    for ( auto& elem : *_other.vector_ )
        {
            ( *this )[elem.str()];
        }

    if ( _include_subencoding && _other.subencoding_ )
        {
            append( *_other.subencoding_, true );
        }
}

// ----------------------------------------------------------------------------

Int Encoding::insert( const std::string& _val )
{
    assert_true( map_.find( _val ) == map_.end() );

    const auto ix = static_cast<Int>( vector_->size() + subsize_ );

    const auto str = strings::String( _val );

    map_[str] = ix;

    vector_->push_back( str );

    return ix;
}

// ----------------------------------------------------------------------------

Int Encoding::operator[]( const std::string& _val )
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

    const auto str = strings::String( _val );

    const auto it = map_.find( str );

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

Int Encoding::operator[]( const std::string& _val ) const
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

    const auto str = strings::String( _val );

    const auto it = map_.find( str );

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
    assert_true( !subencoding_ );

    clear();

    for ( const auto& val : _vector )
        {
            insert( val );
        }

    return *this;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

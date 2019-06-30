#include "predictors/predictors.hpp"

namespace predictors
{
// ------------------------------------------------------------------------

/// Gets an array from a JSON object or throws.
Poco::JSON::Array::Ptr JSON::get_array(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    auto arr = _obj.getArray( _key );

    if ( !arr )
        {
            throw std::runtime_error( "Array named '" + _key + "' not found!" );
        }

    return arr;
}

// ------------------------------------------------------------------------

Poco::JSON::Object::Ptr JSON::get_object(
    const Poco::JSON::Object& _obj, const std::string& _key )
{
    auto ptr = _obj.getObject( _key );

    if ( !ptr )
        {
            throw std::runtime_error(
                "Object named '" + _key + "' not found!" );
        }

    return ptr;
}

// ------------------------------------------------------------------------

std::string JSON::stringify( const Poco::JSON::Object& _obj )
{
    std::stringstream json;

    _obj.stringify( json );

    return json.str();
}

// ------------------------------------------------------------------------
}  // namespace predictors

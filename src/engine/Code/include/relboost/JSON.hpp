#ifndef RELBOOST_JSON_HPP_
#define RELBOOST_JSON_HPP_

// ------------------------------------------------------------------------
// Dependencies

#include <memory>
#include <vector>

#include <Poco/JSON/Parser.h>

#include "debug/debug.hpp"

#include "relboost/enums/enums.hpp"

// ------------------------------------------------------------------------

namespace relboost
{
// ------------------------------------------------------------------------

struct JSON
{
    // ------------------------------------------------------------------------

    /// Parses the correct DataUsed from a string.
    static enums::DataUsed destringify( const std::string& _data_used );

    /// Gets an array from a JSON object or throws.
    static Poco::JSON::Array::Ptr get_array(
        const Poco::JSON::Object& _obj, const std::string& _key );

    /// Gets an array from a JSON object or throws.
    static Poco::JSON::Object::Ptr get_object(
        const Poco::JSON::Object& _obj, const std::string& _key );

    /// Expresses JSON object as JSON string
    static std::string stringify( const Poco::JSON::Object& _obj );

    /// Expresses DataUsed  as JSON string
    static std::string stringify( const enums::DataUsed& _data_used );

    // ------------------------------------------------------------------------

    /// Transforms a Poco array to a vector
    template <typename T>
    static std::vector<T> array_to_vector( const Poco::JSON::Array::Ptr _array )
    {
        if ( _array.isNull() )
            {
                std::runtime_error(
                    "Error in JSON: Array does not exist or is not an array!" );
            }

        std::vector<T> vec;

        for ( auto& val : *_array )
            {
                vec.push_back( val );
            }

        return vec;
    }

    /// Gets a value from a JSON object or throws.
    template <typename T>
    static T get_value(
        const Poco::JSON::Object& _obj, const std::string& _key )
    {
        if ( !_obj.has( _key ) )
            {
                throw std::runtime_error(
                    "Value named '" + _key + "' not found!" );
            }

        return _obj.get( _key ).convert<T>();
    }

    /// Transforms a vector to a Poco array
    template <typename T>
    static Poco::JSON::Array vector_to_array( const std::vector<T>& _vector )
    {
        Poco::JSON::Array arr;

        for ( const auto& elem : _vector )
            {
                arr.add( elem );
            }

        return arr;
    }

    /// Transforms a vector to a Poco array
    template <typename T>
    static Poco::JSON::Array::Ptr vector_to_array_ptr(
        const std::vector<T>& _vector )
    {
        return Poco::JSON::Array::Ptr(
            new Poco::JSON::Array( JSON::vector_to_array( _vector ) ) );
    }

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace relboost

#endif  // RELBOOST_JSON_HPP_

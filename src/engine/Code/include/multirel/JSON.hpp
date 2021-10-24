#ifndef MULTIREL_JSON_HPP_
#define MULTIREL_JSON_HPP_

// ------------------------------------------------------------------------
// Dependencies

#include <memory>
#include <vector>

#include <Poco/JSON/Parser.h>

#include "debug/debug.hpp"

#include "jsonutils/jsonutils.hpp"

#include "multirel/enums/enums.hpp"

// ------------------------------------------------------------------------

namespace multirel
{
// ------------------------------------------------------------------------

struct JSON
{
    // ------------------------------------------------------------------------

    /// Transforms data used to an integer.
    static size_t data_used_to_int( const enums::DataUsed& _data_used );

    /// Expresses DataUsed  as JSON string
    static enums::DataUsed int_to_data_used( const size_t& _val );

    // ------------------------------------------------------------------------

    /// Transforms a Poco array to a vector
    template <typename T>
    static std::vector<T> array_to_vector( const Poco::JSON::Array::Ptr _array )
    {
        return jsonutils::JSON::array_to_vector<T>( _array );
    }

    /// Gets an array from a JSON object or throws.
    static Poco::JSON::Array::Ptr get_array(
        const Poco::JSON::Object& _obj, const std::string& _key )
    {
        return jsonutils::JSON::get_array( _obj, _key );
    };

    /// Gets an array from a JSON object or throws.
    static Poco::JSON::Object::Ptr get_object(
        const Poco::JSON::Object& _obj, const std::string& _key )
    {
        return jsonutils::JSON::get_object( _obj, _key );
    }

    /// Gets a value from a JSON object or throws.
    template <typename T>
    static T get_value(
        const Poco::JSON::Object& _obj, const std::string& _key )
    {
        return jsonutils::JSON::get_value<T>( _obj, _key );
    }

    /// Transforms a vector to a Poco array
    template <typename T>
    static Poco::JSON::Array::Ptr vector_to_array_ptr(
        const std::vector<T>& _vector )
    {
        return jsonutils::JSON::vector_to_array_ptr<T>( _vector );
    }

    /// Expresses JSON object as JSON string
    static std::string stringify( const Poco::JSON::Object& _obj )
    {
        return jsonutils::JSON::stringify( _obj );
    }

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace multirel

#endif  // MULTIREL_JSON_HPP_

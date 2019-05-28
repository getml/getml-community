#ifndef METRICS_JSON_HPP_
#define METRICS_JSON_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

struct JSON
{
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

    /// Gets an array from a JSON object or throws.
    static Poco::JSON::Array::Ptr get_array(
        const Poco::JSON::Object& _obj, const std::string& _key )
    {
        auto arr = _obj.getArray( _key );

        if ( !arr )
            {
                throw std::runtime_error(
                    "Array named '" + _key + "' not found!" );
            }

        return arr;
    }

    /// Transforms a vector to a Poco array
    template <typename T>
    static Poco::JSON::Array vector_to_array( const std::vector<T>& _vector )
    {
        Poco::JSON::Array arr;

        for ( auto& elem : _vector )
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
};

// ----------------------------------------------------------------------------
}  // namespace metrics

#endif  // METRICS_JSON_HPP_

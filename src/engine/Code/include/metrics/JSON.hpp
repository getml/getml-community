#ifndef METRICS_JSON_HPP_
#define METRICS_JSON_HPP_

namespace metrics
{
// ----------------------------------------------------------------------------

struct JSON
{
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

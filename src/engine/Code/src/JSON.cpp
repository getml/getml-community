#include "JSON.hpp"

namespace relboost
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

std::string JSON::stringify( const enums::DataUsed& _data_used )
{
    switch ( _data_used )
        {
            case enums::DataUsed::categorical_input:
                return "categorical_input";

            case enums::DataUsed::categorical_output:
                return "categorical_output";

            case enums::DataUsed::discrete_input:
                return "discrete_input";

            case enums::DataUsed::discrete_input_is_nan:
                return "discrete_input_is_nan";

            case enums::DataUsed::discrete_output:
                return "discrete_output";

            case enums::DataUsed::discrete_output_is_nan:
                return "discrete_output_is_nan";

            case enums::DataUsed::numerical_input:
                return "numerical_input";

            case enums::DataUsed::numerical_input_is_nan:
                return "numerical_input_is_nan";

            case enums::DataUsed::numerical_output:
                return "numerical_output";

            case enums::DataUsed::numerical_output_is_nan:
                return "numerical_output_is_nan";

            case enums::DataUsed::same_units_categorical:
                return "same_units_categorical";

            case enums::DataUsed::same_units_discrete:
                return "same_units_discrete";

            case enums::DataUsed::same_units_discrete_is_nan:
                return "same_units_discrete_is_nan";

            case enums::DataUsed::same_units_numerical:
                return "same_units_numerical";

            case enums::DataUsed::same_units_numerical_is_nan:
                return "same_units_numerical_is_nan";

            case enums::DataUsed::time_stamps_diff:
                return "time_stamps_diff";

            default:
                assert( false && "Unknown data_used_" );
                return "";
        }
}

// ------------------------------------------------------------------------
}  // namespace relboost

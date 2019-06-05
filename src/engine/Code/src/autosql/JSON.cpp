#include "autosql/JSON.hpp"

namespace autosql
{
// ------------------------------------------------------------------------
/*
enums::DataUsed JSON::destringify( const std::string& _data_used )
{
    if ( _data_used == "categorical_input" )
        {
            return enums::DataUsed::categorical_input;
        }
    else if ( _data_used == "categorical_output" )
        {
            return enums::DataUsed::categorical_output;
        }
    else if ( _data_used == "discrete_input" )
        {
            return enums::DataUsed::discrete_input;
        }
    else if ( _data_used == "discrete_input_is_nan" )
        {
            return enums::DataUsed::discrete_input_is_nan;
        }
    else if ( _data_used == "discrete_output" )
        {
            return enums::DataUsed::discrete_output;
        }
    else if ( _data_used == "discrete_output_is_nan" )
        {
            return enums::DataUsed::discrete_output_is_nan;
        }
    else if ( _data_used == "numerical_input" )
        {
            return enums::DataUsed::numerical_input;
        }
    else if ( _data_used == "numerical_input_is_nan" )
        {
            return enums::DataUsed::numerical_input_is_nan;
        }
    else if ( _data_used == "numerical_output" )
        {
            return enums::DataUsed::numerical_output;
        }
    else if ( _data_used == "numerical_output_is_nan" )
        {
            return enums::DataUsed::numerical_output_is_nan;
        }
    else if ( _data_used == "same_units_categorical" )
        {
            return enums::DataUsed::same_units_categorical;
        }
    else if ( _data_used == "same_units_discrete" )
        {
            return enums::DataUsed::same_units_discrete;
        }
    else if ( _data_used == "same_units_discrete_is_nan" )
        {
            return enums::DataUsed::same_units_discrete_is_nan;
        }
    else if ( _data_used == "same_units_numerical" )
        {
            return enums::DataUsed::same_units_numerical;
        }
    else if ( _data_used == "same_units_numerical_is_nan" )
        {
            return enums::DataUsed::same_units_numerical_is_nan;
        }
    else if ( _data_used == "time_stamps_diff" )
        {
            return enums::DataUsed::time_stamps_diff;
        }
    else
        {
            throw std::runtime_error(
                "Unknown data used: '" + _data_used + "'!" );
        }
}*/

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
/*
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
}*/

// ------------------------------------------------------------------------
}  // namespace autosql

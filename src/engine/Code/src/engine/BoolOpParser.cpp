#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::binary_operation(
    const Poco::JSON::Object& _col ) const
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    const auto operand_type = JSON::get_value<std::string>(
        *JSON::get_object( _col, "operand1_" ), "type_" );

    const auto is_boolean = ( operand_type == "BooleanValue" ) ||
                            ( operand_type == "VirtualBooleanColumn" );

    const auto is_categorical = ( operand_type == "StringColumn" ) ||
                                ( operand_type == "CategoricalValue" ) ||
                                ( operand_type == "VirtualStringColumn" );

    const auto is_numerical = ( operand_type == "FloatColumn" ) ||
                              ( operand_type == "Value" ) ||
                              ( operand_type == "VirtualFloatColumn" );

    if ( op == "and" )
        {
            return bin_op( _col, std::logical_and<bool>() );
        }
    else if ( op == "contains" )
        {
            const auto contains = []( const std::string& str1,
                                      const std::string& str2 ) {
                return ( str1.find( str2 ) != std::string::npos );
            };

            return cat_bin_op( _col, contains );
        }
    else if ( is_boolean && op == "equal_to" )
        {
            return bin_op( _col, std::equal_to<bool>() );
        }
    else if ( is_categorical && op == "equal_to" )
        {
            return cat_bin_op( _col, std::equal_to<std::string>() );
        }
    else if ( is_numerical && op == "equal_to" )
        {
            return num_bin_op( _col, std::equal_to<Float>() );
        }
    else if ( op == "greater" )
        {
            return num_bin_op( _col, std::greater<Float>() );
        }
    else if ( op == "greater_equal" )
        {
            return num_bin_op( _col, std::greater_equal<Float>() );
        }
    else if ( op == "less" )
        {
            return num_bin_op( _col, std::less<Float>() );
        }
    else if ( op == "less_equal" )
        {
            return num_bin_op( _col, std::less_equal<Float>() );
        }
    else if ( is_boolean && op == "not_equal_to" )
        {
            return bin_op( _col, std::not_equal_to<bool>() );
        }
    else if ( is_categorical && op == "not_equal_to" )
        {
            return cat_bin_op( _col, std::not_equal_to<std::string>() );
        }
    else if ( is_numerical && op == "not_equal_to" )
        {
            return num_bin_op( _col, std::not_equal_to<Float>() );
        }
    else if ( op == "or" )
        {
            return bin_op( _col, std::logical_or<bool>() );
        }
    else if ( op == "xor" )
        {
            // logical_xor for boolean is the same thing as not_equal_to.
            return bin_op( _col, std::not_equal_to<bool>() );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for boolean columns." );

            return bin_op( _col, std::logical_and<bool>() );
        }
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::parse(
    const Poco::JSON::Object& _col ) const
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "BooleanValue" )
        {
            const auto value = JSON::get_value<bool>( _col, "value_" );

            return containers::ColumnView<bool>::from_value( value );
        }
    else if ( type == "VirtualBooleanColumn" )
        {
            if ( _col.has( "operand2_" ) )
                {
                    return binary_operation( _col );
                }
            else
                {
                    return unary_operation( _col );
                }
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type +
                "' not recognized for boolean columns." );

            return unary_operation( _col );
        }
}

// ----------------------------------------------------------------------------

containers::ColumnView<bool> BoolOpParser::unary_operation(
    const Poco::JSON::Object& _col ) const
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "is_inf" )
        {
            const auto is_inf = []( const Float val ) {
                return std::isinf( val );
            };

            return num_un_op( _col, is_inf );
        }
    else if ( op == "is_nan" )
        {
            const auto is_nan = []( const Float val ) {
                return std::isnan( val );
            };

            return num_un_op( _col, is_nan );
        }
    else if ( op == "not" )
        {
            return un_op( _col, std::logical_not<bool>() );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for boolen columns." );

            return un_op( _col, std::logical_not<bool>() );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

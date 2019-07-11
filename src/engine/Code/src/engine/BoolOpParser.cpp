#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

std::vector<bool> BoolOpParser::binary_operation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    const auto operand_type = JSON::get_value<std::string>(
        *JSON::get_object( _col, "operand1_" ), "type_" );

    const auto is_boolean = ( operand_type == "BooleanValue" ) ||
                            ( operand_type == "VirtualBooleanColumn" );

    const auto is_categorical = ( operand_type == "CategoricalColumn" ) ||
                                ( operand_type == "CategoricalValue" ) ||
                                ( operand_type == "VirtualCategoricalColumn" );

    const auto is_numerical = ( operand_type == "Column" ) ||
                              ( operand_type == "Value" ) ||
                              ( operand_type == "VirtualColumn" );

    if ( op == "and" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::logical_and<bool>() );
        }
    else if ( is_boolean && op == "equal_to" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::equal_to<bool>() );
        }
    else if ( is_categorical && op == "equal_to" )
        {
            return cat_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::equal_to<std::string>() );
        }
    else if ( is_numerical && op == "equal_to" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::equal_to<Float>() );
        }
    else if ( op == "greater" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::greater<Float>() );
        }
    else if ( op == "greater_equal" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::greater_equal<Float>() );
        }
    else if ( op == "less" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::less<Float>() );
        }
    else if ( op == "less_equal" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::less_equal<Float>() );
        }
    else if ( is_boolean && op == "not_equal_to" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::not_equal_to<bool>() );
        }
    else if ( is_categorical && op == "not_equal_to" )
        {
            return cat_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::not_equal_to<std::string>() );
        }
    else if ( is_numerical && op == "not_equal_to" )
        {
            return num_bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::not_equal_to<Float>() );
        }
    else if ( op == "or" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::logical_or<bool>() );
        }
    else if ( op == "xor" )
        {
            // logical_xor for boolean is the same thing as not_equal_to.
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::not_equal_to<bool>() );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for boolean columns." );

            return std::vector<bool>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<bool> BoolOpParser::parse(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "BooleanValue" )
        {
            const auto val = JSON::get_value<bool>( _col, "value_" );

            auto vec = std::vector<bool>( _df.nrows() );

            std::fill( vec.begin(), vec.end(), val );

            return vec;
        }
    else if ( type == "VirtualBooleanColumn" )
        {
            if ( _col.has( "operand2_" ) )
                {
                    return binary_operation(
                        _categories, _join_keys_encoding, _df, _col );
                }
            else
                {
                    return unary_operation(
                        _categories, _join_keys_encoding, _df, _col );
                }
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type +
                "' not recognized for boolean columns." );

            return std::vector<bool>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<bool> BoolOpParser::unary_operation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "is_inf" )
        {
            const auto is_inf = []( const Float val ) {
                return std::isinf( val );
            };

            return num_un_op(
                _categories, _join_keys_encoding, _df, _col, is_inf );
        }
    else if ( op == "is_nan" )
        {
            const auto is_nan = []( const Float val ) {
                return std::isnan( val );
            };

            return num_un_op(
                _categories, _join_keys_encoding, _df, _col, is_nan );
        }
    else if ( op == "not" )
        {
            return un_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::logical_not<bool>() );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for boolen columns." );

            return std::vector<bool>( 0 );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

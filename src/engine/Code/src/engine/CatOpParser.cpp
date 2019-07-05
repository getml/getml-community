#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::parse(
    const containers::Encoding& categories_,
    const containers::Encoding& join_keys_encoding_,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "CategoricalColumn" )
        {
            const auto name = JSON::get_value<std::string>( _col, "name_" );

            const auto role = JSON::get_value<std::string>( _col, "role_" );

            if ( role == "categorical" )
                {
                    return to_vec( categories_, _df.int_column( name, role ) );
                }
            else
                {
                    return to_vec(
                        join_keys_encoding_, _df.int_column( name, role ) );
                }
        }
    else if ( type == "Value" )
        {
            const auto val = JSON::get_value<std::string>( _col, "value_" );

            auto vec = std::vector<std::string>( _df.nrows() );

            std::fill( vec.begin(), vec.end(), val );

            return vec;
        }
    else if ( type == "VirtualCategoricalColumn" )
        {
            if ( _col.has( "operand2_" ) )
                {
                    return binary_operation(
                        categories_, join_keys_encoding_, _df, _col );
                }
            else
                {
                    return unary_operation(
                        categories_, join_keys_encoding_, _df, _col );
                }
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type + "' not recognized." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::binary_operation(
    const containers::Encoding& categories_,
    const containers::Encoding& join_keys_encoding_,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "concat" )
        {
            return bin_op(
                categories_,
                join_keys_encoding_,
                _df,
                _col,
                std::plus<std::string>() );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::unary_operation(
    const containers::Encoding& categories_,
    const containers::Encoding& join_keys_encoding_,
    const containers::DataFrame& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "substr" )
        {
            const auto begin = JSON::get_value<size_t>( _col, "begin_" );

            const auto len = JSON::get_value<size_t>( _col, "len_" );

            const auto substr = [begin, len]( const std::string& val ) {
                return val.substr( begin, len );
            };

            return un_op( categories_, join_keys_encoding_, _df, _col, substr );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

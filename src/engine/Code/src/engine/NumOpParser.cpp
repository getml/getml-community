#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::parse(
    const containers::DataFrame& _df, const Poco::JSON::Object& _col )
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "Column" )
        {
            const auto name = JSON::get_value<std::string>( _col, "name_" );

            const auto role = JSON::get_value<std::string>( _col, "role_" );

            return _df.float_column( name, role );
        }
    else if ( type == "Value" )
        {
            const auto val = JSON::get_value<Float>( _col, "value_" );

            auto col = containers::Column<Float>( _df.nrows() );

            std::fill( col.begin(), col.end(), val );

            return col;
        }
    else if ( type == "VirtualColumn" && _col.has( "operand2_" ) )
        {
            const auto op = JSON::get_value<std::string>( _col, "operator_" );

            const auto operand1 =
                parse( _df, *JSON::get_object( _col, "operand1_" ) );

            const auto operand2 =
                parse( _df, *JSON::get_object( _col, "operand2_" ) );

            return binary_operation( op, operand1, operand2 );
        }
    else if ( type == "VirtualColumn" && !_col.has( "operand2_" ) )
        {
            const auto op = JSON::get_value<std::string>( _col, "operator_" );

            const auto operand1 =
                parse( _df, *JSON::get_object( _col, "operand1_" ) );

            return unary_operation( op, operand1 );
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type + "' not recognized." );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::binary_operation(
    const std::string& _operator,
    const containers::Column<Float>& _operand1,
    const containers::Column<Float>& _operand2 )
{
    assert( _operand1.nrows() == _operand2.nrows() );

    if ( _operator == "divides" )
        {
            return bin_op( _operand1, _operand2, std::divides<Float>() );
        }
    else if ( _operator == "minus" )
        {
            return bin_op( _operand1, _operand2, std::minus<Float>() );
        }
    else if ( _operator == "multiplies" )
        {
            return bin_op( _operand1, _operand2, std::multiplies<Float>() );
        }
    else if ( _operator == "plus" )
        {
            return bin_op( _operand1, _operand2, std::plus<Float>() );
        }
    else if ( _operator == "pow" )
        {
            const auto pow = []( const Float val1, const Float val2 ) {
                return std::pow( val1, val2 );
            };
            return bin_op( _operand1, _operand2, pow );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + _operator + "' not recognized." );

            return containers::Column<Float>( 0 );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::unary_operation(
    const std::string& _operator, const containers::Column<Float>& _operand1 )
{
    auto result = containers::Column<Float>( _operand1.nrows() );

    if ( _operator == "exp" )
        {
            const auto exp = []( const Float val ) { return std::exp( val ); };
            return un_op( _operand1, exp );
        }
    else if ( _operator == "log" )
        {
            const auto log = []( const Float val ) { return std::log( val ); };
            return un_op( _operand1, log );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + _operator + "' not recognized." );
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

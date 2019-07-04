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
    else if ( _operator == "fmod" )
        {
            const auto fmod = []( const Float val1, const Float val2 ) {
                return std::fmod( val1, val2 );
            };
            return bin_op( _operand1, _operand2, fmod );
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

    if ( _operator == "abs" )
        {
            const auto abs = []( const Float val ) { return std::abs( val ); };
            return un_op( _operand1, abs );
        }
    else if ( _operator == "acos" )
        {
            const auto acos = []( const Float val ) {
                return std::acos( val );
            };
            return un_op( _operand1, acos );
        }
    else if ( _operator == "asin" )
        {
            const auto asin = []( const Float val ) {
                return std::asin( val );
            };
            return un_op( _operand1, asin );
        }
    else if ( _operator == "atan" )
        {
            const auto atan = []( const Float val ) {
                return std::atan( val );
            };
            return un_op( _operand1, atan );
        }
    else if ( _operator == "cbrt" )
        {
            const auto cbrt = []( const Float val ) {
                return std::cbrt( val );
            };
            return un_op( _operand1, cbrt );
        }
    else if ( _operator == "ceil" )
        {
            const auto ceil = []( const Float val ) {
                return std::ceil( val );
            };
            return un_op( _operand1, ceil );
        }
    else if ( _operator == "cos" )
        {
            const auto cos = []( const Float val ) { return std::cos( val ); };
            return un_op( _operand1, cos );
        }
    else if ( _operator == "exp" )
        {
            const auto exp = []( const Float val ) { return std::exp( val ); };
            return un_op( _operand1, exp );
        }
    else if ( _operator == "floor" )
        {
            const auto floor = []( const Float val ) {
                return std::floor( val );
            };
            return un_op( _operand1, floor );
        }
    else if ( _operator == "log" )
        {
            const auto log = []( const Float val ) { return std::log( val ); };
            return un_op( _operand1, log );
        }
    else if ( _operator == "round" )
        {
            const auto round = []( const Float val ) {
                return std::round( val );
            };
            return un_op( _operand1, round );
        }
    else if ( _operator == "sin" )
        {
            const auto sin = []( const Float val ) { return std::sin( val ); };
            return un_op( _operand1, sin );
        }
    else if ( _operator == "sqrt" )
        {
            const auto sqrt = []( const Float val ) {
                return std::sqrt( val );
            };
            return un_op( _operand1, sqrt );
        }
    else if ( _operator == "tan" )
        {
            const auto tan = []( const Float val ) { return std::tan( val ); };
            return un_op( _operand1, tan );
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

#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::binary_operation(
    const containers::DataFrame& _df, const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "divides" )
        {
            return bin_op( _df, _col, std::divides<Float>() );
        }
    else if ( op == "fmod" )
        {
            const auto fmod = []( const Float val1, const Float val2 ) {
                return std::fmod( val1, val2 );
            };
            return bin_op( _df, _col, fmod );
        }
    else if ( op == "minus" )
        {
            return bin_op( _df, _col, std::minus<Float>() );
        }
    else if ( op == "multiplies" )
        {
            return bin_op( _df, _col, std::multiplies<Float>() );
        }
    else if ( op == "plus" )
        {
            return bin_op( _df, _col, std::plus<Float>() );
        }
    else if ( op == "pow" )
        {
            const auto pow = []( const Float val1, const Float val2 ) {
                return std::pow( val1, val2 );
            };
            return bin_op( _df, _col, pow );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized." );

            return containers::Column<Float>( 0 );
        }
}
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
            return binary_operation( _df, _col );
        }
    else if ( type == "VirtualColumn" && !_col.has( "operand2_" ) )
        {
            return unary_operation( _df, _col );
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type +
                "' not recognized for numerical columns." );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::unary_operation(
    const containers::DataFrame& _df, const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "abs" )
        {
            const auto abs = []( const Float val ) { return std::abs( val ); };
            return un_op( _df, _col, abs );
        }
    else if ( op == "acos" )
        {
            const auto acos = []( const Float val ) {
                return std::acos( val );
            };
            return un_op( _df, _col, acos );
        }
    else if ( op == "asin" )
        {
            const auto asin = []( const Float val ) {
                return std::asin( val );
            };
            return un_op( _df, _col, asin );
        }
    else if ( op == "atan" )
        {
            const auto atan = []( const Float val ) {
                return std::atan( val );
            };
            return un_op( _df, _col, atan );
        }
    else if ( op == "cbrt" )
        {
            const auto cbrt = []( const Float val ) {
                return std::cbrt( val );
            };
            return un_op( _df, _col, cbrt );
        }
    else if ( op == "ceil" )
        {
            const auto ceil = []( const Float val ) {
                return std::ceil( val );
            };
            return un_op( _df, _col, ceil );
        }
    else if ( op == "cos" )
        {
            const auto cos = []( const Float val ) { return std::cos( val ); };
            return un_op( _df, _col, cos );
        }
    else if ( op == "day" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto day = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_mday );
            };
            return un_op( _df, _col, day );
        }
    else if ( op == "erf" )
        {
            const auto erf = []( const Float val ) { return std::erf( val ); };
            return un_op( _df, _col, erf );
        }
    else if ( op == "exp" )
        {
            const auto exp = []( const Float val ) { return std::exp( val ); };
            return un_op( _df, _col, exp );
        }
    else if ( op == "floor" )
        {
            const auto floor = []( const Float val ) {
                return std::floor( val );
            };
            return un_op( _df, _col, floor );
        }
    else if ( op == "hour" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto hour = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_hour );
            };
            return un_op( _df, _col, hour );
        }
    else if ( op == "lgamma" )
        {
            const auto lgamma = []( const Float val ) {
                return std::lgamma( val );
            };
            return un_op( _df, _col, lgamma );
        }
    else if ( op == "log" )
        {
            const auto log = []( const Float val ) { return std::log( val ); };
            return un_op( _df, _col, log );
        }
    else if ( op == "minute" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto minute = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>( std::gmtime( &time_stamp )->tm_min );
            };
            return un_op( _df, _col, minute );
        }
    else if ( op == "month" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto month = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_mon + 1 );
            };
            return un_op( _df, _col, month );
        }
    else if ( op == "random" )
        {
            return random( _df, _col );
        }
    else if ( op == "round" )
        {
            const auto round = []( const Float val ) {
                return std::round( val );
            };
            return un_op( _df, _col, round );
        }
    else if ( op == "rowid" )
        {
            return rowid( _df );
        }
    else if ( op == "second" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto second = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>( std::gmtime( &time_stamp )->tm_sec );
            };
            return un_op( _df, _col, second );
        }
    else if ( op == "sin" )
        {
            const auto sin = []( const Float val ) { return std::sin( val ); };
            return un_op( _df, _col, sin );
        }
    else if ( op == "sqrt" )
        {
            const auto sqrt = []( const Float val ) {
                return std::sqrt( val );
            };
            return un_op( _df, _col, sqrt );
        }
    else if ( op == "tan" )
        {
            const auto tan = []( const Float val ) { return std::tan( val ); };
            return un_op( _df, _col, tan );
        }
    else if ( op == "tgamma" )
        {
            const auto tgamma = []( const Float val ) {
                return std::tgamma( val );
            };
            return un_op( _df, _col, tgamma );
        }
    else if ( op == "weekday" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto weekday = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_wday );
            };
            return un_op( _df, _col, weekday );
        }
    else if ( op == "year" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto year = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_year + 1900 );
            };
            return un_op( _df, _col, year );
        }
    else if ( op == "yearday" )
        {
            const std::chrono::time_point<std::chrono::system_clock>
                epoch_point;

            const auto yearday = [epoch_point]( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return static_cast<Float>( NAN );
                    }

                const auto seconds_since_epoch =
                    static_cast<std::time_t>( 86400.0 * val );

                const auto time_stamp = std::chrono::system_clock::to_time_t(
                    epoch_point + std::chrono::seconds( seconds_since_epoch ) );

                return static_cast<Float>(
                    std::gmtime( &time_stamp )->tm_yday + 1 );
            };
            return un_op( _df, _col, yearday );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for numerical columns." );

            return containers::Column<Float>( 0 );
        }
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

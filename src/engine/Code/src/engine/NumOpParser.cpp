#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::binary_operation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "divides" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::divides<Float>() );
        }
    else if ( op == "fmod" )
        {
            const auto fmod = []( const Float val1, const Float val2 ) {
                return std::fmod( val1, val2 );
            };
            return bin_op( _categories, _join_keys_encoding, _df, _col, fmod );
        }
    else if ( op == "minus" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::minus<Float>() );
        }
    else if ( op == "multiplies" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::multiplies<Float>() );
        }
    else if ( op == "plus" )
        {
            return bin_op(
                _categories,
                _join_keys_encoding,
                _df,
                _col,
                std::plus<Float>() );
        }
    else if ( op == "pow" )
        {
            const auto pow = []( const Float val1, const Float val2 ) {
                return std::pow( val1, val2 );
            };
            return bin_op( _categories, _join_keys_encoding, _df, _col, pow );
        }
    else if ( op == "update" )
        {
            return update( _categories, _join_keys_encoding, _df, _col );
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
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "Column" )
        {
            const auto name = JSON::get_value<std::string>( _col, "name_" );

            const auto role = JSON::get_value<std::string>( _col, "role_" );

            const auto df_name =
                JSON::get_value<std::string>( _col, "df_name_" );

            const auto has_df_name =
                [df_name]( const containers::DataFrame& df ) {
                    return df.name() == df_name;
                };

            const auto it = std::find_if( _df.begin(), _df.end(), has_df_name );

            if ( it == _df.end() )
                {
                    throw std::invalid_argument(
                        "Column '" + name + "' is from DataFrame '" + df_name +
                        "'." );
                }

            if ( role == "undefined_integer" )
                {
                    return to_float_column( it->int_column( name, role ) );
                }
            else
                {
                    return it->float_column( name, role );
                }
        }
    else if ( type == "Value" )
        {
            const auto val = JSON::get_value<Float>( _col, "value_" );

            assert_true( _df.size() > 0 );

            auto col = containers::Column<Float>( _df[0].nrows() );

            std::fill( col.begin(), col.end(), val );

            return col;
        }
    else if ( type == "VirtualColumn" && _col.has( "operand2_" ) )
        {
            return binary_operation(
                _categories, _join_keys_encoding, _df, _col );
        }
    else if ( type == "VirtualColumn" && !_col.has( "operand2_" ) )
        {
            return unary_operation(
                _categories, _join_keys_encoding, _df, _col );
        }
    else
        {
            throw std::invalid_argument(
                "Column of type '" + type +
                "' not recognized for numerical columns." );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::to_num(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto operand1 = CatOpParser::parse(
        _categories,
        _join_keys_encoding,
        _df,
        *JSON::get_object( _col, "operand1_" ) );

    auto result = containers::Column<Float>( operand1.size() );

    const auto to_double = []( const std::string& _str ) {
        const auto [val, success] = csv::Parser::to_double( _str );
        if ( success )
            {
                return val;
            }
        else
            {
                return static_cast<Float>( NAN );
            }
    };

    std::transform(
        operand1.begin(), operand1.end(), result.begin(), to_double );

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::to_ts(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _col, "time_formats_" ) );

    const auto operand1 = CatOpParser::parse(
        _categories,
        _join_keys_encoding,
        _df,
        *JSON::get_object( _col, "operand1_" ) );

    auto result = containers::Column<Float>( operand1.size() );

    const auto to_time_stamp = [time_formats]( const std::string& _str ) {
        auto [val, success] = csv::Parser::to_time_stamp( _str, time_formats );

        if ( success )
            {
                return val;
            }

        std::tie( val, success ) = csv::Parser::to_double( _str );

        if ( success )
            {
                return val;
            }

        return static_cast<Float>( NAN );
    };

    std::transform(
        operand1.begin(), operand1.end(), result.begin(), to_time_stamp );

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::unary_operation(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "abs" )
        {
            const auto abs = []( const Float val ) { return std::abs( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, abs );
        }
    else if ( op == "acos" )
        {
            const auto acos = []( const Float val ) {
                return std::acos( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, acos );
        }
    else if ( op == "asin" )
        {
            const auto asin = []( const Float val ) {
                return std::asin( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, asin );
        }
    else if ( op == "atan" )
        {
            const auto atan = []( const Float val ) {
                return std::atan( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, atan );
        }
    else if ( op == "cbrt" )
        {
            const auto cbrt = []( const Float val ) {
                return std::cbrt( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, cbrt );
        }
    else if ( op == "ceil" )
        {
            const auto ceil = []( const Float val ) {
                return std::ceil( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, ceil );
        }
    else if ( op == "cos" )
        {
            const auto cos = []( const Float val ) { return std::cos( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, cos );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, day );
        }
    else if ( op == "erf" )
        {
            const auto erf = []( const Float val ) { return std::erf( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, erf );
        }
    else if ( op == "exp" )
        {
            const auto exp = []( const Float val ) { return std::exp( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, exp );
        }
    else if ( op == "floor" )
        {
            const auto floor = []( const Float val ) {
                return std::floor( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, floor );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, hour );
        }
    else if ( op == "lgamma" )
        {
            const auto lgamma = []( const Float val ) {
                return std::lgamma( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, lgamma );
        }
    else if ( op == "log" )
        {
            const auto log = []( const Float val ) { return std::log( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, log );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, minute );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, month );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, round );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, second );
        }
    else if ( op == "sin" )
        {
            const auto sin = []( const Float val ) { return std::sin( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, sin );
        }
    else if ( op == "sqrt" )
        {
            const auto sqrt = []( const Float val ) {
                return std::sqrt( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, sqrt );
        }
    else if ( op == "tan" )
        {
            const auto tan = []( const Float val ) { return std::tan( val ); };
            return un_op( _categories, _join_keys_encoding, _df, _col, tan );
        }
    else if ( op == "tgamma" )
        {
            const auto tgamma = []( const Float val ) {
                return std::tgamma( val );
            };
            return un_op( _categories, _join_keys_encoding, _df, _col, tgamma );
        }
    else if ( op == "to_num" )
        {
            return to_num( _categories, _join_keys_encoding, _df, _col );
        }
    else if ( op == "to_ts" )
        {
            return to_ts( _categories, _join_keys_encoding, _df, _col );
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
            return un_op(
                _categories, _join_keys_encoding, _df, _col, weekday );
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
            return un_op( _categories, _join_keys_encoding, _df, _col, year );
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
            return un_op(
                _categories, _join_keys_encoding, _df, _col, yearday );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized for numerical columns." );

            return containers::Column<Float>( 0 );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::update(
    const containers::Encoding& _categories,
    const containers::Encoding& _join_keys_encoding,
    const std::vector<containers::DataFrame>& _df,
    const Poco::JSON::Object& _col )
{
    const auto operand1 = parse(
        _categories,
        _join_keys_encoding,
        _df,
        *JSON::get_object( _col, "operand1_" ) );

    const auto operand2 = parse(
        _categories,
        _join_keys_encoding,
        _df,
        *JSON::get_object( _col, "operand2_" ) );

    const auto condition = BoolOpParser::parse(
        _categories,
        _join_keys_encoding,
        _df,
        *JSON::get_object( _col, "condition_" ) );

    assert_true( operand1.size() == operand2.size() );

    assert_true( operand1.size() == condition.size() );

    auto result = containers::Column<Float>( operand1.size() );

    for ( size_t i = 0; i < operand1.size(); ++i )
        {
            result[i] = ( condition[i] ? operand2[i] : operand1[i] );
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

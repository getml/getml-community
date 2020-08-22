#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::as_num(
    const Poco::JSON::Object& _col ) const
{
    const auto operand1 = CatOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand1_" ) );

    auto result = containers::Column<Float>( operand1.size() );

    const auto to_double = []( const std::string& _str ) {
        const auto [val, success] = io::Parser::to_double( _str );
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

containers::Column<Float> NumOpParser::as_ts(
    const Poco::JSON::Object& _col ) const
{
    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _col, "time_formats_" ) );

    const auto operand1 = CatOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( *JSON::get_object( _col, "operand1_" ) );

    auto result = containers::Column<Float>( operand1.size() );

    const auto to_time_stamp = [time_formats]( const std::string& _str ) {
        auto [val, success] = io::Parser::to_time_stamp( _str, time_formats );

        if ( success )
            {
                return val;
            }

        std::tie( val, success ) = io::Parser::to_double( _str );

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

void NumOpParser::check(
    const containers::Column<Float>& _col,
    const std::shared_ptr<const communication::Logger>& _logger,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------------

    communication::Warner warner;

    // --------------------------------------------------------------------------

    if ( _col.size() == 0 )
        {
            warner.send( _socket );
            return;
        }

    // --------------------------------------------------------------------------

    const Float length = static_cast<Float>( _col.size() );

    const Float num_non_null =
        utils::ColumnOperators::count( _col.begin(), _col.end() );

    const auto share_null = 1.0 - num_non_null / length;

    if ( share_null > 0.9 )
        {
            warner.add(
                std::to_string( share_null * 100.0 ) +
                "\% of all entries of column '" + _col.name() +
                "' are NULL values." );
        }

    // --------------------------------------------------------------------------

    if ( _logger )
        {
            for ( const auto& warning : warner.warnings() )
                {
                    _logger->log( "WARNING: " + warning );
                }
        }

    // --------------------------------------------------------------------------

    warner.send( _socket );

    // --------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::binary_operation(
    const Poco::JSON::Object& _col ) const
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "divides" )
        {
            return bin_op( _col, std::divides<Float>() );
        }
    else if ( op == "fmod" )
        {
            const auto fmod = []( const Float val1, const Float val2 ) {
                return std::fmod( val1, val2 );
            };
            return bin_op( _col, fmod );
        }
    else if ( op == "minus" )
        {
            return bin_op( _col, std::minus<Float>() );
        }
    else if ( op == "multiplies" )
        {
            return bin_op( _col, std::multiplies<Float>() );
        }
    else if ( op == "plus" )
        {
            return bin_op( _col, std::plus<Float>() );
        }
    else if ( op == "pow" )
        {
            const auto pow = []( const Float val1, const Float val2 ) {
                return std::pow( val1, val2 );
            };
            return bin_op( _col, pow );
        }
    else if ( op == "update" )
        {
            return update( _col );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op + "' not recognized." );

            return containers::Column<Float>( 0 );
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::boolean_as_num(
    const Poco::JSON::Object& _col ) const
{
    const auto obj = *JSON::get_object( _col, "operand1_" );

    const auto operand1 = BoolOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              begin_,
                              length_,
                              subselection_ )
                              .parse( obj );

    auto result = containers::Column<Float>( operand1.size() );

    const auto as_num = []( const bool val ) {
        if ( val )
            {
                return 1.0;
            }
        else
            {
                return 0.0;
            }
    };

    std::transform( operand1.begin(), operand1.end(), result.begin(), as_num );

    return result;
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::get_column(
    const Poco::JSON::Object& _col ) const
{
    const auto name = JSON::get_value<std::string>( _col, "name_" );

    const auto role = JSON::get_value<std::string>( _col, "role_" );

    const auto df_name = JSON::get_value<std::string>( _col, "df_name_" );

    const auto it = data_frames_->find( df_name );

    if ( it == data_frames_->end() )
        {
            throw std::invalid_argument(
                "Column '" + name + "' is from DataFrame '" + df_name +
                "', but no such DataFrame is known." );
        }

    const bool wrong_length =
        ( !subselection_ && it->second.nrows() != length_ ) ||
        it->second.nrows() < begin_ + length_;

    if ( wrong_length )
        {
            throw std::invalid_argument(
                "Columns must have the same length for binary operations "
                "to be possible!" );

            return containers::Column<Float>( 0 );
        }

    if ( it->second.nrows() == length_ )
        {
            return it->second.float_column( name, role );
        }
    else
        {
            const auto long_col = it->second.float_column( name, role );

            auto col = containers::Column<Float>( length_ );

            std::copy(
                long_col.begin() + begin_,
                long_col.begin() + begin_ + length_,
                col.begin() );

            return col;
        }
}

// ----------------------------------------------------------------------------

containers::Column<Float> NumOpParser::parse(
    const Poco::JSON::Object& _col ) const
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "FloatColumn" )
        {
            return get_column( _col );
        }
    else if ( type == "Value" )
        {
            const auto val = JSON::get_value<Float>( _col, "value_" );

            auto col = containers::Column<Float>( length_ );

            std::fill( col.begin(), col.end(), val );

            return col;
        }
    else if ( type == "VirtualFloatColumn" && _col.has( "operand2_" ) )
        {
            return binary_operation( _col );
        }
    else if ( type == "VirtualFloatColumn" && !_col.has( "operand2_" ) )
        {
            return unary_operation( _col );
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
    const Poco::JSON::Object& _col ) const
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "abs" )
        {
            const auto abs = []( const Float val ) { return std::abs( val ); };
            return un_op( _col, abs );
        }
    else if ( op == "acos" )
        {
            const auto acos = []( const Float val ) {
                return std::acos( val );
            };
            return un_op( _col, acos );
        }
    else if ( op == "as_num" )
        {
            return as_num( _col );
        }
    else if ( op == "as_ts" )
        {
            return as_ts( _col );
        }
    else if ( op == "asin" )
        {
            const auto asin = []( const Float val ) {
                return std::asin( val );
            };
            return un_op( _col, asin );
        }
    else if ( op == "atan" )
        {
            const auto atan = []( const Float val ) {
                return std::atan( val );
            };
            return un_op( _col, atan );
        }
    else if ( op == "boolean_as_num" )
        {
            return boolean_as_num( _col );
        }
    else if ( op == "cbrt" )
        {
            const auto cbrt = []( const Float val ) {
                return std::cbrt( val );
            };
            return un_op( _col, cbrt );
        }
    else if ( op == "ceil" )
        {
            const auto ceil = []( const Float val ) {
                return std::ceil( val );
            };
            return un_op( _col, ceil );
        }
    else if ( op == "cos" )
        {
            const auto cos = []( const Float val ) { return std::cos( val ); };
            return un_op( _col, cos );
        }
    else if ( op == "day" )
        {
            return un_op( _col, utils::Time::day );
        }
    else if ( op == "erf" )
        {
            const auto erf = []( const Float val ) { return std::erf( val ); };
            return un_op( _col, erf );
        }
    else if ( op == "exp" )
        {
            const auto exp = []( const Float val ) { return std::exp( val ); };
            return un_op( _col, exp );
        }
    else if ( op == "floor" )
        {
            const auto floor = []( const Float val ) {
                return std::floor( val );
            };
            return un_op( _col, floor );
        }
    else if ( op == "hour" )
        {
            return un_op( _col, utils::Time::hour );
        }
    else if ( op == "lgamma" )
        {
            const auto lgamma = []( const Float val ) {
                return std::lgamma( val );
            };
            return un_op( _col, lgamma );
        }
    else if ( op == "log" )
        {
            const auto log = []( const Float val ) { return std::log( val ); };
            return un_op( _col, log );
        }
    else if ( op == "minute" )
        {
            return un_op( _col, utils::Time::minute );
        }
    else if ( op == "month" )
        {
            return un_op( _col, utils::Time::month );
        }
    else if ( op == "random" )
        {
            return random( _col );
        }
    else if ( op == "round" )
        {
            const auto round = []( const Float val ) {
                return std::round( val );
            };
            return un_op( _col, round );
        }
    else if ( op == "rowid" )
        {
            return rowid();
        }
    else if ( op == "second" )
        {
            return un_op( _col, utils::Time::second );
        }
    else if ( op == "sin" )
        {
            const auto sin = []( const Float val ) { return std::sin( val ); };
            return un_op( _col, sin );
        }
    else if ( op == "sqrt" )
        {
            const auto sqrt = []( const Float val ) {
                return std::sqrt( val );
            };
            return un_op( _col, sqrt );
        }
    else if ( op == "tan" )
        {
            const auto tan = []( const Float val ) { return std::tan( val ); };
            return un_op( _col, tan );
        }
    else if ( op == "tgamma" )
        {
            const auto tgamma = []( const Float val ) {
                return std::tgamma( val );
            };
            return un_op( _col, tgamma );
        }
    else if ( op == "value" )
        {
            return parse( *JSON::get_object( _col, "operand1_" ) );
        }
    else if ( op == "weekday" )
        {
            return un_op( _col, utils::Time::weekday );
        }
    else if ( op == "year" )
        {
            return un_op( _col, utils::Time::year );
        }
    else if ( op == "yearday" )
        {
            return un_op( _col, utils::Time::yearday );
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
    const Poco::JSON::Object& _col ) const
{
    const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

    const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

    const auto condition =
        BoolOpParser(
            categories_,
            join_keys_encoding_,
            data_frames_,
            begin_,
            length_,
            subselection_ )
            .parse( *JSON::get_object( _col, "condition_" ) );

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

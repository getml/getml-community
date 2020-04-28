#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::binary_operation(
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    if ( op == "concat" )
        {
            return bin_op( _col, std::plus<std::string>() );
        }
    else if ( op == "update" )
        {
            return update( _col );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op +
                "' not recognized for categorical columns." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::boolean_as_string(
    const Poco::JSON::Object& _col )
{
    const auto obj = *JSON::get_object( _col, "operand1_" );

    const auto operand1 = BoolOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              num_elem_,
                              subselection_ )
                              .parse( obj );

    auto result = std::vector<std::string>( operand1.size() );

    const auto to_str = []( const bool val ) {
        if ( val )
            {
                return "true";
            }
        else
            {
                return "false";
            }
    };

    std::transform( operand1.begin(), operand1.end(), result.begin(), to_str );

    return result;
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::numerical_as_string(
    const Poco::JSON::Object& _col )
{
    const auto obj = *JSON::get_object( _col, "operand1_" );

    const auto operand1 = NumOpParser(
                              categories_,
                              join_keys_encoding_,
                              data_frames_,
                              num_elem_,
                              subselection_ )
                              .parse( obj );

    const auto role = obj.has( "role_" )
                          ? JSON::get_value<std::string>( obj, "role_" )
                          : std::string( "" );

    auto result = std::vector<std::string>( operand1.nrows() );

    if ( role == "time_stamp" ||
         operand1.unit().find( "time stamp" ) != std::string::npos )
        {
            const auto to_str = []( const Float val ) {
                if ( std::isnan( val ) || std::isinf( val ) )
                    {
                        return std::string( "NULL" );
                    }

                const auto microseconds_since_epoch =
                    static_cast<Poco::Timestamp::TimeVal>( 1.0e06 * val );

                const auto time_stamp =
                    Poco::Timestamp( microseconds_since_epoch );

                return Poco::DateTimeFormatter::format(
                    time_stamp, Poco::DateTimeFormat::ISO8601_FRAC_FORMAT );
            };

            std::transform(
                operand1.begin(), operand1.end(), result.begin(), to_str );
        }
    else
        {
            const auto to_str = []( const Float val ) {
                std::ostringstream stream;
                stream << val;
                return stream.str();
            };

            std::transform(
                operand1.begin(), operand1.end(), result.begin(), to_str );
        }

    return result;
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::parse( const Poco::JSON::Object& _col )
{
    const auto type = JSON::get_value<std::string>( _col, "type_" );

    if ( type == "StringColumn" )
        {
            const auto name = JSON::get_value<std::string>( _col, "name_" );

            const auto role = JSON::get_value<std::string>( _col, "role_" );

            const auto df_name =
                JSON::get_value<std::string>( _col, "df_name_" );

            const auto it = data_frames_->find( df_name );

            if ( it == data_frames_->end() )
                {
                    throw std::invalid_argument(
                        "Column '" + name + "' is from DataFrame '" + df_name +
                        "', but such a DataFrame is not known." );
                }

            if ( role == "categorical" )
                {
                    return to_vec(
                        it->second.int_column( name, role ), categories_ );
                }
            else if ( role == "join_key" )
                {
                    return to_vec(
                        it->second.int_column( name, role ),
                        join_keys_encoding_ );
                }
            else if ( role == "unused" || role == "unused_string" )
                {
                    return to_vec( it->second.unused_string( name ) );
                }
            else
                {
                    throw std::invalid_argument(
                        "Column '" + name +
                        "' is a categorical column, but has unknown role '" +
                        role + "'." );
                }
        }
    else if ( type == "CategoricalValue" )
        {
            const auto val = JSON::get_value<std::string>( _col, "value_" );

            auto vec = std::vector<std::string>( num_elem_ );

            std::fill( vec.begin(), vec.end(), val );

            return vec;
        }
    else if ( type == "VirtualStringColumn" )
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
                "' not recognized for categorical columns." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::unary_operation(
    const Poco::JSON::Object& _col )
{
    const auto op = JSON::get_value<std::string>( _col, "operator_" );

    const auto operand_type = JSON::get_value<std::string>(
        *JSON::get_object( _col, "operand1_" ), "type_" );

    const auto is_boolean = ( operand_type == "BooleanValue" ) ||
                            ( operand_type == "VirtualBooleanColumn" );

    if ( is_boolean && op == "as_str" )
        {
            return boolean_as_string( _col );
        }
    else if ( !is_boolean && op == "as_str" )
        {
            return numerical_as_string( _col );
        }
    else if ( op == "categorical_value" )
        {
            return parse( *JSON::get_object( _col, "operand1_" ) );
        }
    else if ( op == "substr" )
        {
            const auto begin = JSON::get_value<size_t>( _col, "begin_" );

            const auto len = JSON::get_value<size_t>( _col, "len_" );

            const auto substr = [begin, len]( const std::string& val ) {
                return val.substr( begin, len );
            };

            return un_op( _col, substr );
        }
    else
        {
            throw std::invalid_argument(
                "Operator '" + op +
                "' not recognized for categorical columns." );

            return std::vector<std::string>( 0 );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> CatOpParser::update( const Poco::JSON::Object& _col )
{
    const auto operand1 = parse( *JSON::get_object( _col, "operand1_" ) );

    const auto operand2 = parse( *JSON::get_object( _col, "operand2_" ) );

    const auto condition =
        BoolOpParser(
            categories_,
            join_keys_encoding_,
            data_frames_,
            num_elem_,
            subselection_ )
            .parse( *JSON::get_object( _col, "condition_" ) );

    assert_true( operand1.size() == operand2.size() );

    assert_true( operand1.size() == condition.size() );

    auto result = std::vector<std::string>( operand1.size() );

    for ( size_t i = 0; i < operand1.size(); ++i )
        {
            result[i] = ( condition[i] ? operand2[i] : operand1[i] );
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

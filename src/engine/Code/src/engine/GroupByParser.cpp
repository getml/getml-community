#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

std::pair<const containers::DataFrameIndex, const containers::Column<Int>>
GroupByParser::find_index( const std::string& _join_key_name )
{
    for ( size_t i = 0; i < df().num_join_keys(); ++i )
        {
            if ( df().join_key( i ).name() == _join_key_name )
                {
                    const auto index = df().index( i );

                    auto unique =
                        containers::Column<Int>( index.map()->size() );

                    unique.set_name( _join_key_name );

                    size_t j = 0;

                    for ( const auto& [k, v] : *index.map() )
                        {
                            unique[j++] = k;
                        }

                    return std::make_pair( index, unique );
                }
        }

    assert_true( false );

    return std::make_pair(
        containers::DataFrameIndex(), containers::Column<Int>() );
}

// ----------------------------------------------------------------------------

containers::DataFrame GroupByParser::group_by(
    const std::string& _name,
    const std::string& _join_key_name,
    const Poco::JSON::Array& _aggregations )
{
    auto result =
        containers::DataFrame( _name, categories_, join_keys_encoding_ );

    if ( df().has_categorical( _join_key_name ) )
        {
            const auto col = df().categorical( _join_key_name );
            const auto [index, unique] = make_index( col );
            result.add_int_column(
                unique, containers::DataFrame::ROLE_CATEGORICAL );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_join_key( _join_key_name ) )
        {
            const auto [index, unique] = find_index( _join_key_name );
            result.add_int_column(
                unique, containers::DataFrame::ROLE_JOIN_KEY );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_numerical( _join_key_name ) )
        {
            const auto col = df().numerical( _join_key_name );
            const auto [index, unique] = make_index( col );
            result.add_float_column(
                unique, containers::DataFrame::ROLE_NUMERICAL );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_target( _join_key_name ) )
        {
            const auto col = df().target( _join_key_name );
            const auto [index, unique] = make_index( col );
            result.add_float_column(
                unique, containers::DataFrame::ROLE_TARGET );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_text( _join_key_name ) )
        {
            const auto col = df().text( _join_key_name );
            const auto [index, unique] =
                make_index<strings::String, strings::StringHasher>( col );
            result.add_string_column(
                unique, containers::DataFrame::ROLE_TEXT );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_unused_float( _join_key_name ) )
        {
            const auto col = df().unused_float( _join_key_name );
            const auto [index, unique] = make_index( col );
            result.add_float_column(
                unique, containers::DataFrame::ROLE_UNUSED_FLOAT );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else if ( df().has_unused_string( _join_key_name ) )
        {
            const auto col = df().unused_string( _join_key_name );
            const auto [index, unique] =
                make_index<strings::String, strings::StringHasher>( col );
            result.add_string_column(
                unique, containers::DataFrame::ROLE_UNUSED_STRING );
            group_by_unique( _aggregations, unique, index, &result );
        }
    else
        {
            assert_true( !df().has( _join_key_name ) );

            throw std::invalid_argument(
                "Could not group by '" + _join_key_name + "': Data frame '" +
                df().name() + "' has no column of that name." );
        }

    return result;
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

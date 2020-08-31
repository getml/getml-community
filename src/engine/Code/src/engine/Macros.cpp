#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

std::string Macros::get_param(
    const std::string& _splitted, const std::string& _key )
{
    const auto begin = _splitted.find( _key ) + _key.size();

    assert_true( begin != std::string::npos );

    const auto end = _splitted.find( containers::Macros::join_param(), begin );

    assert_true( end != std::string::npos );

    assert_true( end >= begin );

    return _splitted.substr( begin, end - begin );
}

// ----------------------------------------------------------------------------

std::string Macros::make_left_join( const std::string& _splitted )
{
    const auto
        [name,
         join_key,
         other_join_key,
         time_stamp,
         other_time_stamp,
         upper_time_stamp] = containers::Macros::parse_splitted( _splitted );

    auto left_join = "       LEFT JOIN \"" + name + "\"\n";

    left_join +=
        "       ON \"" + join_key + "\" = \"" + other_join_key + "\"\n";

    if ( other_time_stamp != "" )
        {
            left_join += "       AND \"" + other_time_stamp + "\" <= \"" +
                         time_stamp + "\"\n";
        }

    if ( other_time_stamp != "" )
        {
            left_join += "       AND \"" + upper_time_stamp + "\" > \"" +
                         time_stamp + "\"\n";
        }

    return left_join;
}

// ----------------------------------------------------------------------------

std::string Macros::make_subquery( const std::string& _joined_name )
{
    const auto splitted = split_joined_name( _joined_name );

    assert_true( splitted.size() != 0 );

    if ( splitted.size() == 1 )
        {
            return splitted.at( 0 );
        }

    auto subquery = "( SELECT * FROM \"" + splitted.at( 0 ) + "\"\n";

    for ( size_t i = 1; i < splitted.size(); ++i )
        {
            subquery += make_left_join( splitted.at( i ) );
        }

    subquery += "     )";

    return subquery;
}

// ----------------------------------------------------------------------------

std::vector<std::string> Macros::modify_colnames(
    const std::vector<std::string>& _names )
{
    auto names = _names;

    for ( auto& name : names )
        {
            const auto [_, to_colname] = parse_table_colname( "", name );
            name = to_colname;
        }

    return names;
}

// ----------------------------------------------------------------------------

helpers::ImportanceMaker Macros::modify_column_importances(
    const helpers::ImportanceMaker& _importance_maker )
{
    auto importance_maker = helpers::ImportanceMaker( _importance_maker );

    for ( const auto& [from_desc, _] : _importance_maker.importances() )
        {
            auto [to_table, to_colname] =
                parse_table_colname( from_desc.table_, from_desc.name_ );

            to_colname = remove_time_diff( to_colname );

            to_colname = replace( to_colname );

            if ( from_desc.table_ != to_table || from_desc.name_ != to_colname )
                {
                    const auto to_desc = helpers::ColumnDescription(
                        from_desc.marker_, to_table, to_colname );

                    importance_maker.transfer( from_desc, to_desc );
                }
        }

    return importance_maker;
}

// ----------------------------------------------------------------------------

std::string Macros::modify_sql( const std::string& _sql )
{
    auto sql = replace( _sql );

    sql = remove_many_to_one( sql, "LEFT JOIN \"", "\" t2" );

    sql = remove_many_to_one( sql, "FROM \"", "\" t1" );

    return sql;
}

// ----------------------------------------------------------------------------

std::tuple<
    std::string,
    std::string,
    std::string,
    std::string,
    std::string,
    std::string>
Macros::parse_splitted( const std::string& _splitted )
{
    const auto name = _splitted.substr(
        0, _splitted.find( containers::Macros::join_param() ) );

    const auto join_key =
        get_param( _splitted, containers::Macros::join_key() + "=" );

    const auto other_join_key =
        get_param( _splitted, containers::Macros::other_join_key() + "=" );

    const auto time_stamp =
        get_param( _splitted, containers::Macros::time_stamp() + "=" );

    const auto other_time_stamp =
        get_param( _splitted, containers::Macros::other_time_stamp() + "=" );

    const auto upper_time_stamp =
        get_param( _splitted, containers::Macros::upper_time_stamp() + "=" );

    return std::make_tuple(
        name,
        join_key,
        other_join_key,
        time_stamp,
        other_time_stamp,
        upper_time_stamp );
}

// -----------------------------------------------------------------------------

std::pair<std::string, std::string> Macros::parse_table_colname(
    const std::string& _table, const std::string& _colname )
{
    if ( _colname.find( table() ) == std::string::npos )
        {
            if ( _table.find( name() ) == std::string::npos )
                {
                    return std::make_pair( _table, _colname );
                }

            const auto table_end = _table.find( name() );

            const auto table = _table.substr( 0, table_end );

            return std::make_pair( table, _colname );
        }

    const auto table_begin = _colname.rfind( table() ) + table().length() + 1;

    const auto table_end = _colname.rfind( containers::Macros::column() );

    assert_true( table_end >= table_begin );

    const auto table_len = table_end - table_begin;

    const auto table = _colname.substr( table_begin, table_len );

    const auto colname_begin = table_end + column().length() + 1;

    const auto colname = _colname.substr( colname_begin );

    return std::make_pair( table, colname );
}

// ----------------------------------------------------------------------------

std::string Macros::remove_many_to_one(
    const std::string& _query,
    const std::string& _key1,
    const std::string& _key2 )
{
    const auto begin = _query.find( _key1 ) + _key1.length();

    const auto end = _query.find( _key2 );

    assert_true( end >= begin );

    const auto len = end - begin;

    const auto original = _query.substr( begin, len );

    const auto subquery = make_subquery( original );

    if ( subquery == original )
        {
            return _query;
        }

    auto query = _query;

    query.replace( begin - 1, len + 2, subquery );

    return query;
}

// ----------------------------------------------------------------------------

std::string Macros::remove_time_diff( const std::string& _from_colname )
{
    // --------------------------------------------------------------

    if ( _from_colname.find( containers::Macros::generated_ts() ) ==
         std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    const auto pos = _from_colname.find( "\", '" );

    if ( pos == std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    return _from_colname.substr( 0, pos );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string Macros::replace( const std::string& _query )
{
    // --------------------------------------------------------------

    auto new_query =
        utils::StringReplacer::replace_all( _query, generated_ts(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, remove_char() + "\"", "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, close_bracket() + "\"", "\" )" );

    new_query =
        utils::StringReplacer::replace_all( new_query, close_bracket(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + hour(), "hour( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + hour(), "hour( t2.\"" );

    new_query = utils::StringReplacer::replace_all( new_query, hour(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + minute(), "minute( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + minute(), "minute( t2.\"" );

    new_query = utils::StringReplacer::replace_all( new_query, minute(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + month(), "month( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + month(), "month( t2.\"" );

    new_query = utils::StringReplacer::replace_all( new_query, month(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + weekday(), "weekday( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + weekday(), "weekday( t2.\"" );

    new_query = utils::StringReplacer::replace_all( new_query, weekday(), "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + year(), "year( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + year(), "year( t2.\"" );

    new_query = utils::StringReplacer::replace_all( new_query, year(), "" );

    // --------------------------------------------------------------

    return new_query;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> Macros::split_joined_name(
    const std::string& _joined_name )
{
    auto splitted = std::vector<std::string>();

    auto joined_name = _joined_name;

    const auto delimiter = std::string( containers::Macros::name() + "=" );

    while ( true )
        {
            const auto pos = joined_name.find( delimiter );

            if ( pos == std::string::npos )
                {
                    splitted.push_back( joined_name );
                    break;
                }

            splitted.push_back( joined_name.substr( 0, pos ) );

            joined_name.erase( 0, pos + delimiter.size() );
        }

    if ( splitted.size() == 0 )
        {
            splitted.push_back( _joined_name );
        }

    return splitted;
}

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine


#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
// ----------------------------------------------------------------------------

std::string Macros::get_param(
    const std::string& _splitted, const std::string& _key )
{
    const auto key =
        ( _key.size() > 0 && _key.back() == '=' ) ? _key : _key + '=';

    const auto begin = _splitted.find( key ) + key.size();

    assert_true( begin != std::string::npos );

    const auto end = _splitted.find( containers::Macros::join_param(), begin );

    assert_true( end != std::string::npos );

    assert_true( end >= begin );

    return _splitted.substr( begin, end - begin );
}

// ----------------------------------------------------------------------------

std::string Macros::make_table_name(
    const std::string& _join_key,
    const std::string& _other_join_key,
    const std::string& _time_stamp,
    const std::string& _other_time_stamp,
    const std::string& _upper_time_stamp,
    const std::string& _name,
    const std::string& _joined_to,
    const bool _one_to_one )
{
    if ( _name.find( Macros::joined_to() ) != std::string::npos )
        {
            const auto name = get_param( _name, Macros::joined_to() + "=" );

            return make_table_name(
                       _join_key,
                       _other_join_key,
                       _time_stamp,
                       _other_time_stamp,
                       _upper_time_stamp,
                       name,
                       _joined_to,
                       _one_to_one ) +
                   _name;
        }

    if ( _joined_to.find( Macros::joined_to() ) != std::string::npos )
        {
            const auto joined_to =
                get_param( _joined_to, Macros::joined_to() + "=" );

            return _joined_to + make_table_name(
                                    _join_key,
                                    _other_join_key,
                                    _time_stamp,
                                    _other_time_stamp,
                                    _upper_time_stamp,
                                    _name,
                                    joined_to,
                                    _one_to_one );
        }

    const auto one_to_one =
        _one_to_one ? std::string( "true" ) : std::string( "false" );

    return Macros::delimiter() + Macros::name() + "=" + _name +
           Macros::join_key() + "=" + _join_key + Macros::other_join_key() +
           "=" + _other_join_key + Macros::time_stamp() + "=" + _time_stamp +
           Macros::other_time_stamp() + "=" + _other_time_stamp +
           Macros::upper_time_stamp() + "=" + _upper_time_stamp +
           Macros::joined_to() + "=" + _joined_to + Macros::one_to_one() + "=" +
           one_to_one + Macros::end();
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
         upper_time_stamp,
         joined_to,
         _] = parse_table_name( _splitted );

    auto left_join = "       LEFT JOIN \"" + name + "\"\n";

    left_join += "       ON \"" + joined_to + "\".\"" + join_key + "\" = \"" +
                 name + "\".\"" + other_join_key + "\"\n";

    if ( other_time_stamp != "" )
        {
            left_join += "       AND \"" + name + "\".\"" + other_time_stamp +
                         "\" <= \"" + joined_to + "\".\"" + time_stamp + "\"\n";
        }

    if ( other_time_stamp != "" )
        {
            left_join += "       AND \"" + name + "\".\"" + upper_time_stamp +
                         "\" > \"" + joined_to + "\".\"" + time_stamp + "\"\n";
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

            name = replace( to_colname );
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

            to_colname = remove_substring( to_colname );

            to_colname = remove_seasonal( to_colname );

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
    auto sql = remove_colnames( _sql );

    sql = replace( sql );

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
    std::string,
    std::string,
    bool>
Macros::parse_table_name( const std::string& _splitted )
{
    const auto name = get_param( _splitted, Macros::name() + "=" );

    const auto join_key = get_param( _splitted, Macros::join_key() + "=" );

    const auto other_join_key =
        get_param( _splitted, Macros::other_join_key() + "=" );

    const auto time_stamp = get_param( _splitted, Macros::time_stamp() + "=" );

    const auto other_time_stamp =
        get_param( _splitted, Macros::other_time_stamp() + "=" );

    const auto upper_time_stamp =
        get_param( _splitted, Macros::upper_time_stamp() + "=" );

    const auto joined_to = get_param( _splitted, Macros::joined_to() + "=" );

    const bool one_to_one =
        ( get_param( _splitted, Macros::one_to_one() + "=" ) == "true" );

    return std::make_tuple(
        name,
        join_key,
        other_join_key,
        time_stamp,
        other_time_stamp,
        upper_time_stamp,
        joined_to,
        one_to_one );
}

// -----------------------------------------------------------------------------

std::pair<std::string, std::string> Macros::parse_table_colname(
    const std::string& _table, const std::string& _colname )
{
    if ( _colname.find( table() ) == std::string::npos )
        {
            if ( _table.find( Macros::joined_to() ) == std::string::npos )
                {
                    return std::make_pair( _table, _colname );
                }

            const auto table = get_param( _table, Macros::joined_to() + "=" );

            return std::make_pair( table, _colname );
        }

    const auto table = get_param( _colname, Macros::table() + "=" );

    const auto colname = get_param( _colname, Macros::column() + "=" );

    return std::make_pair( table, colname );
}

// ----------------------------------------------------------------------------

std::string Macros::remove_colnames( const std::string& _sql )
{
    auto sql = _sql;

    while ( true )
        {
            const auto begin = sql.find( Macros::table() );

            if ( begin == std::string::npos )
                {
                    break;
                }

            const auto end = sql.find( Macros::end(), begin );

            assert_true( end != std::string::npos );

            assert_true( end > begin );

            const auto len = end - begin + Macros::end().length();

            const auto name = sql.substr( begin, len );

            const auto [_, colname] = parse_table_colname( "", name );

            sql.replace( begin, len, colname );
        }

    return sql;
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

std::string Macros::remove_substring( const std::string& _from_colname )
{
    // --------------------------------------------------------------

    if ( _from_colname.find( Macros::substring() ) == std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    const auto begin = _from_colname.find( Macros::substring() ) +
                       Macros::substring().length();

    const auto end = _from_colname.find( Macros::begin() );

    assert_true( end >= begin );

    const auto length = end - begin;

    // --------------------------------------------------------------

    return _from_colname.substr( begin, length );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string Macros::remove_seasonal( const std::string& _from_colname )
{
    auto to_colname =
        utils::StringReplacer::replace_all( _from_colname, hour(), "" );

    to_colname = utils::StringReplacer::replace_all( to_colname, minute(), "" );

    to_colname = utils::StringReplacer::replace_all( to_colname, month(), "" );

    to_colname =
        utils::StringReplacer::replace_all( to_colname, weekday(), "" );

    to_colname = utils::StringReplacer::replace_all( to_colname, year(), "" );

    to_colname =
        utils::StringReplacer::replace_all( to_colname, seasonal_end(), "" );

    return to_colname;
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
        new_query, close_bracket() + remove_char() + "\"", " )" );

    new_query = utils::StringReplacer::replace_all(
        new_query, close_bracket() + remove_char(), " )" );

    new_query = utils::StringReplacer::replace_all(
        new_query, close_bracket() + "\"", "\" )" );

    new_query =
        utils::StringReplacer::replace_all( new_query, close_bracket(), " )" );

    new_query = utils::StringReplacer::replace_all(
        new_query, seasonal_end() + "\"", "\" )" );

    new_query =
        utils::StringReplacer::replace_all( new_query, seasonal_end(), " )" );

    new_query = utils::StringReplacer::replace_all(
        new_query, remove_char() + "\"", "" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + no_join_key() + "\"", "1" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + substring(), "substr( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + substring(), "substr( t2.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, substring(), "substr( \"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, begin(), "\", " );

    new_query = utils::StringReplacer::replace_all( new_query, length(), ", " );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + hour(), "hour( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + hour(), "hour( t2.\"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, hour(), "hour( " );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + minute(), "minute( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + minute(), "minute( t2.\"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, minute(), "minute( " );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + month(), "month( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + month(), "month( t2.\"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, month(), "month( " );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + weekday(), "weekday( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + weekday(), "weekday( t2.\"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, weekday(), "weekday( " );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t1.\"" + year(), "year( t1.\"" );

    new_query = utils::StringReplacer::replace_all(
        new_query, "t2.\"" + year(), "year( t2.\"" );

    new_query =
        utils::StringReplacer::replace_all( new_query, year(), "year( " );

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

    while ( true )
        {
            const auto pos = joined_name.find( delimiter() );

            if ( pos == std::string::npos )
                {
                    splitted.push_back( joined_name );
                    break;
                }

            splitted.push_back( joined_name.substr( 0, pos ) );

            joined_name.erase( 0, pos + delimiter().size() );
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


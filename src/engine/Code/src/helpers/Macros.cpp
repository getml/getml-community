#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::vector<std::string> Macros::extract_table_names(
    const std::string& _joined_name )
{
    if ( _joined_name.find( Macros::delimiter() ) == std::string::npos )
        {
            return { _joined_name };
        }

    const auto splitted = StringSplitter::split( _joined_name, delimiter() );

    std::vector<std::string> table_names;

    for ( const auto& s : splitted )
        {
            const bool has_name =
                ( s.find( Macros::name() ) != std::string::npos );

            const auto name =
                has_name ? std::get<0>( parse_table_name( s ) ) : s;

            if ( name.length() != 0 )
                {
                    table_names.push_back( name );
                }
        }

    return table_names;
}

// ----------------------------------------------------------------------------

std::string Macros::get_param(
    const std::string& _splitted, const std::string& _key )
{
    const auto key =
        ( _key.size() > 0 && _key.back() == '=' ) ? _key : _key + '=';

    const auto begin = _splitted.find( key ) + key.size();

    assert_true( begin != std::string::npos );

    const auto end = _splitted.find( Macros::join_param(), begin );

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
    const std::string& _alias,
    const std::string& _joined_to_name,
    const std::string& _joined_to_alias,
    const bool _one_to_one )
{
    if ( _name.find( Macros::joined_to_name() ) != std::string::npos )
        {
            const auto name =
                get_param( _name, Macros::joined_to_name() + "=" );

            const auto alias =
                get_param( _name, Macros::joined_to_alias() + "=" );

            return make_table_name(
                       _join_key,
                       _other_join_key,
                       _time_stamp,
                       _other_time_stamp,
                       _upper_time_stamp,
                       name,
                       alias,
                       _joined_to_name,
                       _joined_to_alias,
                       _one_to_one ) +
                   _name;
        }

    if ( _joined_to_name.find( Macros::joined_to_name() ) != std::string::npos )
        {
            const auto joined_to_name =
                get_param( _joined_to_name, Macros::joined_to_name() + "=" );

            const auto joined_to_alias =
                get_param( _joined_to_name, Macros::joined_to_alias() + "=" );

            return _joined_to_name + make_table_name(
                                         _join_key,
                                         _other_join_key,
                                         _time_stamp,
                                         _other_time_stamp,
                                         _upper_time_stamp,
                                         _name,
                                         _alias,
                                         joined_to_name,
                                         joined_to_alias,
                                         _one_to_one );
        }

    const auto one_to_one =
        _one_to_one ? std::string( "true" ) : std::string( "false" );

    return Macros::delimiter() + Macros::name() + "=" + _name +
           Macros::alias() + "=" + _alias + Macros::join_key() + "=" +
           _join_key + Macros::other_join_key() + "=" + _other_join_key +
           Macros::time_stamp() + "=" + _time_stamp +
           Macros::other_time_stamp() + "=" + _other_time_stamp +
           Macros::upper_time_stamp() + "=" + _upper_time_stamp +
           Macros::joined_to_name() + "=" + _joined_to_name +
           Macros::joined_to_alias() + "=" + _joined_to_alias +
           Macros::one_to_one() + "=" + one_to_one + Macros::end();
}

// ----------------------------------------------------------------------------

std::vector<std::string> Macros::modify_colnames(
    const std::vector<std::string>& _names )
{
    auto names = _names;

    for ( auto& name : names )
        {
            name = std::get<1>( parse_table_colname( "", name ) );

            name = SQLGenerator::edit_colname( name, "" );
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

            to_colname = remove_email( to_colname );

            to_colname = remove_imputation( to_colname );

            to_colname = remove_mappings( to_colname );

            to_colname = remove_substring( to_colname );

            to_colname = remove_seasonal( to_colname );

            to_colname = remove_time_diff( to_colname );

            to_colname = SQLGenerator::edit_colname( to_colname, "" );

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

std::vector<std::string> Macros::parse_join_key_name(
    const std::string& _jk_name )
{
    if ( _jk_name.find( Macros::multiple_join_key_sep() ) == std::string::npos )
        {
            return { _jk_name };
        }

    auto jk_names =
        StringSplitter::split( _jk_name, Macros::multiple_join_key_sep() );

    assert_true( jk_names.size() > 1 );

    jk_names.front() = StringReplacer::replace_all(
        jk_names.front(), Macros::multiple_join_key_begin(), "" );

    jk_names.back() = StringReplacer::replace_all(
        jk_names.back(), Macros::multiple_join_key_end(), "" );

    return jk_names;
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
    std::string,
    std::string,
    bool>
Macros::parse_table_name( const std::string& _splitted )
{
    const auto name = get_param( _splitted, Macros::name() + "=" );

    const auto alias = get_param( _splitted, Macros::alias() + "=" );

    const auto join_key = get_param( _splitted, Macros::join_key() + "=" );

    const auto other_join_key =
        get_param( _splitted, Macros::other_join_key() + "=" );

    const auto time_stamp = get_param( _splitted, Macros::time_stamp() + "=" );

    const auto other_time_stamp =
        get_param( _splitted, Macros::other_time_stamp() + "=" );

    const auto upper_time_stamp =
        get_param( _splitted, Macros::upper_time_stamp() + "=" );

    const auto joined_to_name =
        get_param( _splitted, Macros::joined_to_name() + "=" );

    const auto joined_to_alias =
        get_param( _splitted, Macros::joined_to_alias() + "=" );

    const bool one_to_one =
        ( get_param( _splitted, Macros::one_to_one() + "=" ) == "true" );

    return std::make_tuple(
        name,
        alias,
        join_key,
        other_join_key,
        time_stamp,
        other_time_stamp,
        upper_time_stamp,
        joined_to_name,
        joined_to_alias,
        one_to_one );
}

// -----------------------------------------------------------------------------

std::pair<std::string, std::string> Macros::parse_table_colname(
    const std::string& _table, const std::string& _colname )
{
    if ( _colname.find( table() ) == std::string::npos )
        {
            if ( _table.find( Macros::joined_to_name() ) == std::string::npos )
                {
                    return std::make_pair( _table, _colname );
                }

            const auto table =
                get_param( _table, Macros::joined_to_name() + "=" );

            return std::make_pair( table, _colname );
        }

    const auto table = get_param( _colname, Macros::table() + "=" );

    const auto column = get_param( _colname, Macros::column() + "=" );

    auto colname = _colname;

    const auto begin = colname.find( Macros::table() );

    assert_true( colname.find( Macros::end() ) != std::string::npos );

    const auto end = colname.find( Macros::end() ) + Macros::end().length();

    assert_true( end > begin );

    const auto len = end - begin;

    colname.replace( begin, len, column );

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

            const auto colname = std::get<1>( parse_table_colname( "", name ) );

            sql.replace( begin, len, colname );
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string Macros::remove_email( const std::string& _from_colname )
{
    auto to_colname =
        StringReplacer::replace_all( _from_colname, email_domain_begin(), "" );

    to_colname =
        StringReplacer::replace_all( to_colname, email_domain_end(), "" );

    return to_colname;
}

// ----------------------------------------------------------------------------

std::string Macros::remove_imputation( const std::string& _from_colname )
{
    // --------------------------------------------------------------

    auto to_colname =
        StringReplacer::replace_all( _from_colname, dummy_begin(), "" );

    to_colname = StringReplacer::replace_all( to_colname, dummy_end(), "" );

    // --------------------------------------------------------------

    if ( to_colname.find( Macros::imputation_begin() ) == std::string::npos )
        {
            return to_colname;
        }

    // --------------------------------------------------------------

    const auto begin = to_colname.find( Macros::imputation_begin() ) +
                       Macros::imputation_begin().length();

    const auto end = to_colname.find( Macros::imputation_replacement() );

    assert_true( end >= begin );

    const auto length = end - begin;

    // --------------------------------------------------------------

    return to_colname.substr( begin, length );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string Macros::remove_mappings( const std::string& _from_colname )
{
    const auto pos = _from_colname.find( "__mapping_" );

    if ( pos == std::string::npos )
        {
            return _from_colname;
        }

    return _from_colname.substr( 0, pos );
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
    auto to_colname = StringReplacer::replace_all( _from_colname, hour(), "" );

    to_colname = StringReplacer::replace_all( to_colname, minute(), "" );

    to_colname = StringReplacer::replace_all( to_colname, month(), "" );

    to_colname = StringReplacer::replace_all( to_colname, weekday(), "" );

    to_colname = StringReplacer::replace_all( to_colname, year(), "" );

    to_colname = StringReplacer::replace_all( to_colname, seasonal_end(), "" );

    return to_colname;
}

// ----------------------------------------------------------------------------

std::string Macros::remove_time_diff( const std::string& _from_colname )
{
    // --------------------------------------------------------------

    if ( _from_colname.find( Macros::generated_ts() ) == std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    const auto pos = _from_colname.find( Macros::diffstr() );

    if ( pos == std::string::npos )
        {
            return _from_colname;
        }

    // --------------------------------------------------------------

    return _from_colname.substr( 0, pos );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace helpers


#include "engine/containers/containers.hpp"

namespace engine
{
namespace containers
{
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

    const auto table_begin = _colname.rfind( containers::Macros::table() ) +
                             containers::Macros::table().length() + 1;

    const auto table_end = _colname.rfind( containers::Macros::column() );

    assert_true( table_end >= table_begin );

    const auto table_len = table_end - table_begin;

    const auto table = _colname.substr( table_begin, table_len );

    const auto colname_begin =
        table_end + containers::Macros::column().length() + 1;

    const auto colname = _colname.substr( colname_begin );

    return std::make_pair( table, colname );
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
}  // namespace containers
}  // namespace engine


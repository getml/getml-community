#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::string SQLGenerator::create_indices(
    const std::string& _table_name, const helpers::Schema& _schema )
{
    // ------------------------------------------------------------------------

    const auto create_index =
        [&_table_name]( const std::string& _colname ) -> std::string {
        const auto colname = SQLGenerator::make_colname( _colname );

        const auto index_name = _table_name + "__" + colname;

        const auto drop = "DROP INDEX IF EXISTS \"" + index_name + "\";\n";

        return drop + "CREATE INDEX \"" + index_name + "\" ON \"" +
               _table_name + "\" (\"" + colname + "\");\n\n";
    };

    // ------------------------------------------------------------------------

    return stl::collect::string(
               _schema.join_keys_ |
               std::ranges::views::filter( include_column ) |
               std::ranges::views::transform( create_index ) ) +
           stl::collect::string(
               _schema.time_stamps_ |
               std::ranges::views::transform( create_index ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
SQLGenerator::demangle_colname( const std::string& _raw_name )
{
    // --------------------------------------------------------------

    const auto m_pos = _raw_name.find( "__mapping_" );

    auto new_name = ( m_pos != std::string::npos )
                        ? make_colname( _raw_name.substr( 0, m_pos ) ) +
                              _raw_name.substr( m_pos )
                        : _raw_name;

    // --------------------------------------------------------------

    const auto has_col_param =
        ( new_name.find( Macros::column() ) != std::string::npos );

    new_name = has_col_param ? Macros::get_param( new_name, Macros::column() )
                             : new_name;

    // --------------------------------------------------------------

    new_name = Macros::prefix() + new_name + Macros::postfix();

    new_name =
        StringReplacer::replace_all( new_name, Macros::generated_ts(), "" );

    new_name =
        StringReplacer::replace_all( new_name, Macros::rowid(), "rowid" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::open_bracket(), "( " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::close_bracket(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::seasonal_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name,
        Macros::email_domain_begin(),
        "email_domain( " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::email_domain_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::imputation_begin(), "COALESCE( " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::imputation_replacement(), Macros::postfix() + ", " );

    new_name = StringReplacer::replace_all(
        new_name, Macros::imputation_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::dummy_begin(), "( CASE WHEN " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name,
        Macros::dummy_end(),
        Macros::postfix() + " IS NULL THEN 1 ELSE 0 END )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::diffstr(), Macros::postfix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::substring(), "substr( " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::begin(), Macros::postfix() + ", " );

    new_name = StringReplacer::replace_all(
        new_name, Macros::length(), Macros::postfix() + ", " );

    new_name = StringReplacer::replace_all(
        new_name, Macros::hour(), "strftime('%H', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::minute(), "strftime('%M', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::month(), "strftime('%m', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::weekday(), "strftime('%w', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::year(), "strftime('%Y', " + Macros::prefix() );

    // --------------------------------------------------------------

    const auto pos1 =
        new_name.rfind( Macros::prefix() ) + Macros::prefix().size();

    const auto pos2 = new_name.find( Macros::postfix() );

    throw_unless( pos2 >= pos1, "Error: Macros in colname do not make sense!" );

    const auto length = pos2 - pos1;

    const auto prefix = StringReplacer::replace_all(
        new_name.substr( 0, pos1 ), Macros::prefix(), "" );

    const auto postfix = StringReplacer::replace_all(
        new_name.substr( pos2 ), Macros::postfix(), "" );

    new_name = new_name.substr( pos1, length );

    // --------------------------------------------------------------

    return std::make_tuple( prefix, new_name, postfix );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::edit_colname(
    const std::string& _raw_name, const std::string& _alias )
{
    // --------------------------------------------------------------

    if ( _raw_name.find( Macros::no_join_key() ) != std::string::npos )
        {
            return "1";
        }

    if ( _raw_name.find( Macros::self_join_key() ) != std::string::npos )
        {
            return "1";
        }

    // --------------------------------------------------------------

    const auto [prefix, new_name, postfix] = demangle_colname( _raw_name );

    // --------------------------------------------------------------

    const bool need_alias = ( _alias != "" );

    const bool has_alias =
        ( _raw_name.find( Macros::alias() ) != std::string::npos );

    const bool not_t1_or_t2 =
        has_alias && ( Macros::get_param( _raw_name, Macros::alias() ) !=
                       Macros::t1_or_t2() );

    const bool extract_alias = need_alias && has_alias && not_t1_or_t2;

    const auto alias = extract_alias
                           ? Macros::get_param( _raw_name, Macros::alias() )
                           : _alias;

    const auto dot = ( alias == "" ) ? "" : ".";

    const auto quotation =
        ( _raw_name.find( Macros::rowid() ) != std::string::npos ||
          alias == "" )
            ? ""
            : "\"";

    // --------------------------------------------------------------

    return prefix + alias + dot + quotation + new_name + quotation + postfix;

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_colname( const std::string& _raw_name )
{
    const auto [prefix, new_name, postfix] = demangle_colname( _raw_name );

    const bool has_alias =
        ( _raw_name.find( Macros::alias() ) != std::string::npos );

    const bool not_t1_or_t2 =
        has_alias && ( Macros::get_param( _raw_name, Macros::alias() ) !=
                       Macros::t1_or_t2() );

    const bool is_not_mapping =
        ( _raw_name.find( "__mapping_" ) == std::string::npos );

    const bool extract_alias = has_alias && not_t1_or_t2 && is_not_mapping;

    const auto alias =
        extract_alias ? Macros::get_param( _raw_name, Macros::alias() ) : "";

    const auto underscore = ( alias == "" ) ? "" : "__";

    return alias + underscore + prefix + to_lower( new_name ) + postfix;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::get_table_name( const std::string& _raw_name )
{
    const auto has_delimiter =
        ( _raw_name.find( Macros::delimiter() ) != std::string::npos );

    auto name =
        has_delimiter
            ? StringSplitter::split( _raw_name, Macros::delimiter() ).at( 0 )
            : _raw_name;

    name = StringReplacer::replace_all( name, Macros::population(), "" );

    name = StringReplacer::replace_all( name, Macros::peripheral(), "" );

    const auto pos = name.find( Macros::staging_table_num() );

    if ( pos == std::string::npos )
        {
            return name;
        }

    return name.substr( 0, pos );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::handle_multiple_join_keys(
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name,
    const std::string& _output_alias,
    const std::string& _input_alias )
{
    const auto sep = Macros::multiple_join_key_sep();

    const auto join_keys1 =
        StringSplitter::split( _output_join_keys_name, sep );

    const auto join_keys2 = StringSplitter::split( _input_join_keys_name, sep );

    throw_unless(
        join_keys1.size() == join_keys2.size(),
        "Error while handling multiple join keys: Number of join keys does "
        "not "
        "match!" );

    std::stringstream sql;

    sql << "ON ";

    for ( size_t i = 0; i < join_keys1.size(); ++i )
        {
            sql << edit_colname( join_keys1.at( i ), _output_alias );
            sql << " = ";
            sql << edit_colname( join_keys2.at( i ), _input_alias );
            sql << std::endl;

            if ( i != join_keys1.size() - 1 )
                {
                    sql << "AND ";
                }
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::handle_many_to_one_joins(
    const std::string& _table_name, const std::string& _t1_or_t2 )
{
    if ( _table_name.find( Macros::delimiter() ) == std::string::npos )
        {
            return "";
        }

    const auto joins =
        StringSplitter::split( _table_name, Macros::delimiter() );

    std::stringstream sql;

    for ( size_t i = 1; i < joins.size(); ++i )
        {
            const auto
                [name,
                 alias,
                 join_key,
                 other_join_key,
                 time_stamp,
                 other_time_stamp,
                 upper_time_stamp,
                 joined_to_name,
                 joined_to_alias,
                 _] = Macros::parse_table_name( joins.at( i ) );

            sql << "LEFT JOIN \"" << name << "\" " << alias << std::endl;

            sql << make_join_keys(
                join_key, other_join_key, joined_to_alias, alias );

            if ( time_stamp != "" && other_time_stamp != "" )
                {
                    sql << "AND "
                        << helpers::SQLGenerator::make_time_stamps(
                               time_stamp,
                               other_time_stamp,
                               upper_time_stamp,
                               joined_to_alias,
                               alias,
                               _t1_or_t2 );
                }
        }

    return StringReplacer::replace_all(
        sql.str(), Macros::t1_or_t2(), _t1_or_t2 );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::join_mapping(
    const std::string& _name, const std::string& _colname, const bool _is_text )
{
    // ------------------------------------------------------------------------

    const auto table_name = to_upper( make_staging_table_name( _name ) );

    const auto mapping_col = to_lower( _colname );

    const auto pos = mapping_col.find( "__mapping_" );

    assert_true( pos != std::string::npos );

    const auto orig_col = mapping_col.substr( 0, pos );

    // ------------------------------------------------------------------------

    const auto join_text =
        [&table_name, &mapping_col, &orig_col]() -> std::string {
        return "UPDATE \"" + table_name + "\"\nSET \"" + mapping_col +
               "\" = t3.\"avg_value\"\nFROM \"" + table_name +
               "\",\n     ( SELECT t1.\"" + orig_col +
               "\", AVG( t2.\"value\" ) AS \"avg_value\"\n       FROM "
               "\"" +
               table_name + "\" t1\n       LEFT JOIN \"" +
               to_upper( mapping_col ) + "\" t2\n       ON contains( t1.\"" +
               orig_col + "\", t2.\"key\" ) > 0\n       GROUP BY t1.\"" +
               orig_col + "\" ) AS t3\nWHERE \"" + table_name + "\".\"" +
               orig_col + "\" = t3.\"" + orig_col + "\";\n\n";
    };

    // ------------------------------------------------------------------------

    const auto join_other =
        [&table_name, &mapping_col, &orig_col]() -> std::string {
        return "UPDATE \"" + table_name + "\"\nSET \"" + mapping_col +
               "\" = t2.\"value\"\nFROM \"" + to_upper( mapping_col ) +
               "\" AS t2\nWHERE \"" + table_name + "\".\"" + orig_col +
               "\" = t2.\"key\";\n\n";
    };

    // ------------------------------------------------------------------------

    const std::string alter_table = "ALTER TABLE \"" + table_name +
                                    "\" ADD COLUMN \"" + to_lower( _colname ) +
                                    "\";\n\n";

    const std::string drop_table =
        "DROP TABLE IF EXISTS \"" + to_upper( _colname ) + "\";\n\n\n";

    // ------------------------------------------------------------------------

    if ( _is_text )
        {
            return alter_table + join_text() + drop_table;
        }

    return alter_table + join_other() + drop_table;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_epoch_time(
    const std::string& _raw_name, const std::string& _alias )
{
    const auto colname = make_colname( _raw_name );

    return _alias + ".\"" + colname + "\"";
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_join_keys(
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name,
    const std::string& _output_alias,
    const std::string& _input_alias )
{
    if ( _output_join_keys_name.find( Macros::multiple_join_key_sep() ) !=
         std::string::npos )
        {
            return handle_multiple_join_keys(
                _output_join_keys_name,
                _input_join_keys_name,
                _output_alias,
                _input_alias );
        }

    std::stringstream sql;

    sql << "ON ";

    sql << edit_colname( _output_join_keys_name, _output_alias );
    sql << " = ";
    sql << edit_colname( _input_join_keys_name, _input_alias );
    sql << std::endl;

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_joins(
    const std::string& _output_name,
    const std::string& _input_name,
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name )
{
    const auto output_name = make_staging_table_name( _output_name );

    const auto input_name = make_staging_table_name( _input_name );

    std::stringstream sql;

    sql << "FROM \"" << output_name << "\" t1" << std::endl;

    sql << "LEFT JOIN \"" << input_name << "\" t2" << std::endl;

    sql << make_join_keys(
        _output_join_keys_name, _input_join_keys_name, "t1", "t2" );

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_relative_time(
    const std::string& _raw_name, const std::string& _alias )
{
    const auto colname = make_colname( _raw_name );

    return _alias + ".\"" + colname + "\"";
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLGenerator::make_staging_columns(
    const bool& _include_targets, const helpers::Schema& _schema )
{
    // ------------------------------------------------------------------------

    const auto cast_column = []( const std::string& _colname,
                                 const std::string& _coltype ) -> std::string {
        return "CAST( " + edit_colname( _colname, "t1" ) + " AS " + _coltype +
               " ) AS \"" + to_lower( make_colname( _colname ) ) + "\"";
    };

    // ------------------------------------------------------------------------

    const auto to_epoch_time =
        []( const std::string& _colname ) -> std::string {
        const auto epoch_time =
            _colname.find( Macros::rowid() ) == std::string::npos
                ? "( julianday( " + edit_colname( _colname, "t1" ) +
                      " ) - julianday( '1970-01-01' ) ) * 86400.0"
                : edit_colname( _colname, "t1" );

        return "CAST( " + epoch_time + " AS REAL ) AS \"" +
               to_lower( make_colname( _colname ) ) + "\"";
    };

    // ------------------------------------------------------------------------

    const auto cast_as_real =
        [cast_column]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "REAL" );

        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( cast ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_time_stamp =
        [to_epoch_time]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( to_epoch_time ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_text =
        [cast_column]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "TEXT" );

        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( cast ) );
    };

    // ------------------------------------------------------------------------

    const auto categoricals = cast_as_text( _schema.categoricals_ );

    const auto discretes = cast_as_real( _schema.discretes_ );

    const auto join_keys = cast_as_text( _schema.join_keys_ );

    const auto numericals = cast_as_real( _schema.numericals_ );

    const auto targets = _include_targets ? cast_as_real( _schema.targets_ )
                                          : std::vector<std::string>();

    const auto text = cast_as_text( _schema.text_ );

    const auto time_stamps = cast_as_time_stamp( _schema.time_stamps_ );

    // ------------------------------------------------------------------------

    return stl::join::vector<std::string>(
        { targets,
          categoricals,
          discretes,
          join_keys,
          numericals,
          text,
          time_stamps } );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool SQLGenerator::include_column( const std::string& _name )
{
    if ( _name == Macros::no_join_key() )
        {
            return false;
        }

    if ( _name == Macros::self_join_key() )
        {
            return false;
        }

    if ( _name.find( Macros::multiple_join_key_begin() ) != std::string::npos )
        {
            return false;
        }

    if ( _name.find( "__mapping_" ) != std::string::npos )
        {
            return false;
        }

    return true;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical,
    const std::string& _prefix )
{
    std::string sql = "DROP TABLE IF EXISTS \"FEATURES" + _prefix + "\";\n\n";

    sql += "CREATE TABLE \"FEATURES" + _prefix + "\" AS\n";

    sql += make_select(
        _main_table, _autofeatures, _targets, _categorical, _numerical );

    const auto main_table =
        helpers::SQLGenerator::get_table_name( _main_table );

    sql += "FROM \"" + main_table + "\" t1\n";

    sql += handle_many_to_one_joins( _main_table, "t1" );

    sql += "ORDER BY t1.rowid;\n\n";

    sql += make_updates( _autofeatures, _prefix );

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_postprocessing(
    const std::vector<std::string>& _sql )
{
    std::string sql;

    for ( const auto feature : _sql )
        {
            const auto pos = feature.find( "\";\n" );

            throw_unless(
                pos != std::string::npos,
                "Could not find end of DROP TABLE IF EXISTS statement." );

            sql += feature.substr( 0, pos ) + "\";\n";
        }

    return sql;
}
// ----------------------------------------------------------------------------

std::string SQLGenerator::make_select(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical )
{
    const auto manual = stl::join::vector<std::string>(
        { _targets, _numerical, _categorical } );

    const auto modified_colnames = helpers::Macros::modify_colnames( manual );

    std::string sql =
        manual.size() > 0 ? "SELECT " : "SELECT t1.rowid AS \"rownum\",\n";

    for ( size_t i = 0; i < _autofeatures.size(); ++i )
        {
            const std::string begin =
                ( i == 0 && manual.size() > 0 ? "" : "       " );

            const bool no_comma =
                ( i == _autofeatures.size() - 1 && manual.size() == 0 );

            const auto end = ( no_comma ? "\n" : ",\n" );

            sql += begin + "CAST( 0.0 AS REAL ) AS \"" + _autofeatures.at( i ) +
                   "\"" + end;
        }

    for ( size_t i = 0; i < manual.size(); ++i )
        {
            const std::string begin = "       ";

            const auto edited_colname =
                helpers::SQLGenerator::edit_colname( manual.at( i ), "t1" );

            const std::string data_type =
                ( i < _targets.size() + _numerical.size() ? "REAL" : "TEXT" );

            const auto target_or_feature =
                i < _targets.size()
                    ? "\"target_" + std::to_string( i + 1 )
                    : "\"manual_feature_" +
                          std::to_string( i - _targets.size() + 1 );

            const bool no_comma = ( i == manual.size() - 1 );

            const auto end = no_comma ? "\"\n" : "\",\n";

            sql += begin + "CAST( " + edited_colname + " AS " + data_type +
                   " ) AS " + target_or_feature + "__" +
                   modified_colnames.at( i ) + end;
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_sql(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _sql,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical )
{
    auto sql = _sql;

    sql.push_back( make_feature_table(
        _main_table, _autofeatures, _targets, _categorical, _numerical, "" ) );

    sql.push_back( make_postprocessing( _sql ) );

    return stl::collect::string( sql );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_staging_table(
    const bool& _include_targets, const helpers::Schema& _schema )
{
    // ------------------------------------------------------------------------

    const auto columns = make_staging_columns( _include_targets, _schema );

    const auto name = make_staging_table_name( _schema.name_ );

    // ------------------------------------------------------------------------

    std::stringstream sql;

    // ------------------------------------------------------------------------

    sql << "DROP TABLE IF EXISTS \"" << to_upper( name ) << "\";\n\n";

    sql << "CREATE TABLE \"" << to_upper( name ) << "\" AS\nSELECT ";

    for ( size_t i = 0; i < columns.size(); ++i )
        {
            const auto begin = ( i == 0 ) ? "" : "       ";
            const auto end = ( i == columns.size() - 1 ) ? "\n" : ",\n";
            sql << begin << columns.at( i ) << end;
        }

    sql << "FROM \"" << get_table_name( _schema.name_ ) << "\" t1\n";

    sql << handle_many_to_one_joins( _schema.name_, "t1" );

    sql << ";\n\n";

    sql << create_indices( name, _schema );

    sql << "\n";

    // ------------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_staging_table_name( const std::string& _name )
{
    const auto pos = _name.find( Macros::staging_table_num() );

    if ( pos == std::string::npos )
        {
            return to_upper( get_table_name( _name ) );
        }

    const auto begin = pos + Macros::staging_table_num().size();

    const auto end = _name.find_first_not_of( "0123456789", begin );

    assert_true( end > begin );

    const auto number = _name.substr( begin, end - begin );

    const auto pos_text_field = _name.find( Macros::text_field() );

    const auto text_field_col =
        ( pos_text_field == std::string::npos )
            ? std::string( "" )
            : "__" +
                  _name.substr( pos_text_field + Macros::text_field().size() );

    return to_upper( get_table_name( _name ) ) + "__STAGING_TABLE_" + number +
           to_upper( text_field_col );
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLGenerator::make_staging_tables(
    const bool _population_needs_targets,
    const std::vector<bool>& _peripheral_needs_targets,
    const Schema& _population_schema,
    const std::vector<Schema>& _peripheral_schema )
{
    // ------------------------------------------------------------------------

    auto sql = std::vector<std::string>( { make_staging_table(
        _population_needs_targets, _population_schema ) } );

    // ------------------------------------------------------------------------

    assert_true(
        _peripheral_schema.size() == _peripheral_needs_targets.size() );

    for ( size_t i = 0; i < _peripheral_schema.size(); ++i )
        {
            const auto& schema = _peripheral_schema.at( i );

            auto s =
                make_staging_table( _peripheral_needs_targets.at( i ), schema );

            sql.emplace_back( std::move( s ) );
        }

    // ------------------------------------------------------------------------

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_subfeature_identifier(
    const std::string& _feature_prefix, const size_t _peripheral_used )
{
    return _feature_prefix + std::to_string( _peripheral_used + 1 );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_subfeature_joins(
    const std::string& _feature_prefix,
    const size_t _peripheral_used,
    const std::string& _alias,
    const std::string& _feature_postfix )
{
    assert_msg( _alias == "t1" || _alias == "t2", "_alias: " + _alias );

    assert_true( _feature_prefix.size() > 0 );

    std::stringstream sql;

    const auto number =
        ( _alias == "t2" )
            ? make_subfeature_identifier( _feature_prefix, _peripheral_used )
            : _feature_prefix.substr( 0, _feature_prefix.size() - 1 );

    const auto letter = _feature_postfix == "" ? 'f' : 'p';

    sql << "LEFT JOIN \"FEATURES_" << number << _feature_postfix << "\" "
        << letter << "_" << number << std::endl;

    sql << "ON " << _alias << ".rowid = " << letter << "_" << number
        << ".\"rownum\"" << std::endl;

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_time_stamp_diff(
    const Float _diff, const bool _is_rowid )
{
    const auto diffstr = [_diff]() {
        constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
        constexpr Float seconds_per_hour = 60.0 * 60.0;
        constexpr Float seconds_per_minute = 60.0;

        const auto abs_diff = std::abs( _diff );

        if ( abs_diff >= seconds_per_day )
            {
                return std::to_string( _diff / seconds_per_day ) + " days";
            }

        if ( abs_diff >= seconds_per_hour )
            {
                return std::to_string( _diff / seconds_per_hour ) + " hours";
            }

        if ( abs_diff >= seconds_per_minute )
            {
                return std::to_string( _diff / seconds_per_minute ) +
                       " minutes";
            }

        return std::to_string( _diff ) + " seconds";
    };

    if ( _is_rowid )
        {
            return Macros::diffstr() + " + " + std::to_string( _diff );
        }

    const std::string sign = _diff >= 0.0 ? "+" : "";

    return Macros::diffstr() + ", '" + sign + diffstr() + "'";
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_time_stamps(
    const std::string& _time_stamp_name,
    const std::string& _lower_time_stamp_name,
    const std::string& _upper_time_stamp_name,
    const std::string& _output_alias,
    const std::string& _input_alias,
    const std::string& _t1_or_t2 )
{
    std::stringstream sql;

    const auto colname1 = make_relative_time( _time_stamp_name, _output_alias );

    const auto colname2 =
        make_relative_time( _lower_time_stamp_name, _input_alias );

    sql << colname2 << " <= " << colname1 << std::endl;

    if ( _upper_time_stamp_name != "" )
        {
            const auto colname3 =
                make_relative_time( _upper_time_stamp_name, _input_alias );

            sql << "AND ( " << colname3 << " > " << colname1 << " OR "
                << colname3 << " IS NULL )" << std::endl;
        }

    return StringReplacer::replace_all(
        sql.str(), Macros::t1_or_t2(), _t1_or_t2 );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_updates(
    const std::vector<std::string>& _autofeatures, const std::string& _prefix )
{
    std::string sql;

    for ( const auto colname : _autofeatures )
        {
            const auto table = helpers::StringReplacer::replace_all(
                colname, "feature", "FEATURE" );

            sql += "UPDATE \"FEATURES" + _prefix + "\"\n";
            sql += "SET \"" + colname + "\" = COALESCE( t2.\"" + colname +
                   "\", 0.0 )\n";
            sql += "FROM \"" + table + "\" AS t2\n";
            sql +=
                "WHERE \"FEATURES" + _prefix + "\".rowid = t2.\"rownum\";\n\n";
        }

    return sql;
}
// ------------------------------------------------------------------------

std::string SQLGenerator::to_lower( const std::string& _str )
{
    auto lower = _str;

    bool skip = false;

    for ( auto& c : lower )
        {
            if ( skip )
                {
                    skip = false;
                    continue;
                }

            if ( c == '%' )
                {
                    skip = true;
                }

            c = std::tolower( c );
        }

    return lower;
}

// ------------------------------------------------------------------------

std::string SQLGenerator::to_upper( const std::string& _str )
{
    auto upper = _str;

    bool skip = false;

    for ( auto& c : upper )
        {
            if ( skip )
                {
                    skip = false;
                    continue;
                }

            if ( c == '%' )
                {
                    skip = true;
                }

            c = std::toupper( c );
        }

    return upper;
}

// ----------------------------------------------------------------------------
}  // namespace helpers

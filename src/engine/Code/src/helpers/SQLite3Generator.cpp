#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::string SQLite3Generator::aggregation(
    const enums::Aggregation& _agg,
    const std::string& _colname1,
    const std::optional<std::string>& _colname2 ) const
{
    if ( _agg == enums::Aggregation::avg_time_between )
        {
            assert_true( _colname2 );

            const auto ts_name = "t2." + quotechar1() +
                                 make_colname( _colname2.value() ) +
                                 quotechar2();

            return "CASE WHEN COUNT( * ) > 1 THEN ( MAX( " + ts_name +
                   " ) - MIN ( " + ts_name +
                   " ) ) / ( COUNT( * ) - 1 )  ELSE 0 END";
        }

    const auto value = _colname2 ? _colname1 + ", " + *_colname2 : _colname1;

    if ( _agg == enums::Aggregation::count_distinct )
        {
            return "COUNT( DISTINCT " + value + " )";
        }

    if ( _agg == enums::Aggregation::count_minus_count_distinct )
        {
            return "COUNT( " + value + "  ) - COUNT( DISTINCT " + value + " )";
        }

    const auto agg_type = enums::Parser<enums::Aggregation>::to_str( _agg );

    return helpers::StringReplacer::replace_all( agg_type, " ", "_" ) + "( " +
           value + " )";
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::create_indices(
    const std::string& _table_name, const helpers::Schema& _schema ) const
{
    // ------------------------------------------------------------------------

    const auto create_index =
        [this, &_table_name]( const std::string& _colname ) -> std::string {
        const auto colname = make_colname( _colname );

        const auto index_name = _table_name + "__" + colname;

        const auto drop = "DROP INDEX IF EXISTS \"" + index_name + "\";\n";

        return drop + "CREATE INDEX \"" + index_name + "\" ON \"" +
               _table_name + "\" (\"" + colname + "\");\n\n";
    };

    // ------------------------------------------------------------------------

    return stl::collect::string(
               _schema.join_keys_ |
               std::ranges::views::filter( SQLGenerator::include_column ) |
               std::ranges::views::transform( create_index ) ) +
           stl::collect::string(
               _schema.time_stamps_ |
               std::ranges::views::transform( create_index ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
SQLite3Generator::demangle_colname( const std::string& _raw_name ) const
{
    // --------------------------------------------------------------

    const auto m_pos = _raw_name.find( "__mapping_" );

    auto new_name = ( m_pos != std::string::npos )
                        ? make_colname( _raw_name.substr( 0, m_pos ) ) +
                              _raw_name.substr( m_pos )
                        : _raw_name;

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

    // --------------------------------------------------------------

    new_name = StringReplacer::replace_all(
        new_name, Macros::hour_begin(), "strftime('%H', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::hour_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name,
        Macros::minute_begin(),
        "strftime('%M', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::minute_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::month_begin(), "strftime('%m', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::month_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name,
        Macros::weekday_begin(),
        "strftime('%w', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::weekday_end(), Macros::postfix() + " )" );

    new_name = StringReplacer::replace_all(
        new_name, Macros::year_begin(), "strftime('%Y', " + Macros::prefix() );

    new_name = StringReplacer::replace_all(
        new_name, Macros::year_end(), Macros::postfix() + " )" );

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

    const auto has_col_param =
        ( new_name.find( Macros::column() ) != std::string::npos );

    new_name = has_col_param ? Macros::get_param( new_name, Macros::column() )
                             : new_name;

    // --------------------------------------------------------------

    return std::make_tuple( prefix, new_name, postfix );

    // --------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::edit_colname(
    const std::string& _raw_name, const std::string& _alias ) const
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

std::string SQLite3Generator::make_colname( const std::string& _raw_name ) const
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

    return alias + underscore + prefix + SQLGenerator::to_lower( new_name ) +
           postfix;
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::join_mapping(
    const std::string& _name,
    const std::string& _colname,
    const bool _is_text ) const
{
    // ------------------------------------------------------------------------

    const bool is_text_field =
        ( _name.find( Macros::text_field() ) != std::string::npos );

    const auto table_name = SQLGenerator::to_upper(
        SQLGenerator::make_staging_table_name( _name ) );

    const auto mapping_col = SQLGenerator::to_lower( _colname );

    const auto pos = mapping_col.find( "__mapping_" );

    assert_true( pos != std::string::npos );

    const auto orig_col = mapping_col.substr( 0, pos );

    // ------------------------------------------------------------------------

    const auto join_text =
        [&table_name, &mapping_col, &orig_col]() -> std::string {
        return "UPDATE \"" + table_name + "\"\nSET \"" + mapping_col +
               "\" = t3.\"avg_value\"\nFROM ( SELECT t1.\"" + orig_col +
               "\", AVG( t2.\"value\" ) AS \"avg_value\"\n       FROM \"" +
               table_name + "\" t1\n       LEFT JOIN \"" +
               SQLGenerator::to_upper( mapping_col ) +
               "\" t2\n       ON contains( t1.\"" + orig_col +
               "\", t2.\"key\" ) > 0\n       GROUP BY t1.\"" + orig_col +
               "\" ) AS t3\nWHERE \"" + table_name + "\".\"" + orig_col +
               "\" = t3.\"" + orig_col + "\";\n\n";
    };

    // ------------------------------------------------------------------------

    const auto join_other =
        [&table_name, &mapping_col, &orig_col]() -> std::string {
        return "UPDATE \"" + table_name + "\"\nSET \"" + mapping_col +
               "\" = t2.\"value\"\nFROM \"" +
               SQLGenerator::to_upper( mapping_col ) + "\" AS t2\nWHERE \"" +
               table_name + "\".\"" + orig_col + "\" = t2.\"key\";\n\n";
    };

    // ------------------------------------------------------------------------

    const std::string alter_table =
        "ALTER TABLE \"" + table_name + "\" ADD COLUMN \"" +
        SQLGenerator::to_lower( _colname ) + "\";\n\n";

    const std::string set_to_zero =
        "UPDATE \"" + table_name + "\" SET \"" + mapping_col + "\" = 0.0;\n\n";

    const std::string drop_table = "DROP TABLE IF EXISTS \"" +
                                   SQLGenerator::to_upper( _colname ) +
                                   "\";\n\n\n";

    // ------------------------------------------------------------------------

    if ( _is_text && !is_text_field )
        {
            return alter_table + set_to_zero + join_text() + drop_table;
        }

    return alter_table + set_to_zero + join_other() + drop_table;
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_joins(
    const std::string& _output_name,
    const std::string& _input_name,
    const std::string& _output_join_keys_name,
    const std::string& _input_join_keys_name ) const
{
    const auto output_name =
        SQLGenerator::make_staging_table_name( _output_name );

    const auto input_name =
        SQLGenerator::make_staging_table_name( _input_name );

    std::stringstream sql;

    sql << "FROM \"" << output_name << "\" t1" << std::endl;

    sql << "INNER JOIN \"" << input_name << "\" t2" << std::endl;

    if ( _output_join_keys_name == Macros::no_join_key() ||
         _output_join_keys_name == Macros::self_join_key() )
        {
            assert_true( _output_join_keys_name == _input_join_keys_name );

            sql << "ON 1 = 1" << std::endl;
        }
    else
        {
            assert_true(
                _input_join_keys_name != Macros::no_join_key() &&
                _input_join_keys_name != Macros::self_join_key() );

            sql << SQLGenerator::handle_multiple_join_keys(
                _output_join_keys_name,
                _input_join_keys_name,
                "t1",
                "t2",
                SQLGenerator::NOT_FOR_STAGING,
                this );
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLite3Generator::make_staging_columns(
    const bool& _include_targets, const helpers::Schema& _schema ) const
{
    // ------------------------------------------------------------------------

    const auto cast_column = [this](
                                 const std::string& _colname,
                                 const std::string& _coltype ) -> std::string {
        return "CAST( " + edit_colname( _colname, "t1" ) + " AS " + _coltype +
               " ) AS \"" + SQLGenerator::to_lower( make_colname( _colname ) ) +
               "\"";
    };

    // ------------------------------------------------------------------------

    const auto is_rowid = []( const std::string& _colname ) -> bool {
        return _colname.find( Macros::rowid() ) != std::string::npos;
    };

    // ------------------------------------------------------------------------

    const auto to_epoch_time_or_rowid =
        [this, &is_rowid]( const std::string& _colname ) -> std::string {
        const auto epoch_time =
            is_rowid( _colname )
                ? edit_colname( _colname, "t1" )
                : "( julianday( " + edit_colname( _colname, "t1" ) +
                      " ) - julianday( '1970-01-01' ) ) * 86400.0";

        return "CAST( " + epoch_time + " AS REAL ) AS \"" +
               SQLGenerator::to_lower( make_colname( _colname ) ) + "\"";
    };

    // ------------------------------------------------------------------------

    const auto cast_as_real =
        [cast_column]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "REAL" );

        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( SQLGenerator::include_column ) |
            std::views::transform( cast ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_time_stamp =
        [to_epoch_time_or_rowid]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( SQLGenerator::include_column ) |
            std::views::transform( to_epoch_time_or_rowid ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_text =
        [cast_column]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "TEXT" );

        return stl::collect::vector<std::string>(
            _colnames | std::views::filter( SQLGenerator::include_column ) |
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

std::string SQLite3Generator::make_feature_table(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical,
    const std::string& _prefix ) const
{
    std::string sql = "DROP TABLE IF EXISTS \"FEATURES" + _prefix + "\";\n\n";

    sql += "CREATE TABLE \"FEATURES" + _prefix + "\" AS\n";

    sql += make_select(
        _main_table, _autofeatures, _targets, _categorical, _numerical );

    const auto main_table =
        SQLGenerator::make_staging_table_name( _main_table );

    sql += "FROM \"" + main_table + "\" t1\n";

    sql += "ORDER BY t1.rowid;\n\n";

    sql += make_updates( _autofeatures, _prefix );

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_mapping_table_header(
    const std::string& _name, const bool _key_is_num ) const
{
    const auto quote1 = quotechar1();

    const auto quote2 = quotechar2();

    std::stringstream sql;

    sql << "DROP TABLE IF EXISTS " << quote1 << _name << quote2 << ";"
        << std::endl
        << std::endl;

    const std::string key_type = _key_is_num ? "INTEGER" : "TEXT";

    sql << "CREATE TABLE " << quote1 << _name << quote2 << "(key " << key_type
        << " NOT NULL PRIMARY KEY, value REAL);" << std::endl
        << std::endl;

    sql << "INSERT INTO " << quote1 << _name << quote2 << " (key, value)"
        << std::endl
        << "VALUES";

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_postprocessing(
    const std::vector<std::string>& _sql ) const
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

std::string SQLite3Generator::make_select(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical ) const
{
    const auto manual = stl::join::vector<std::string>(
        { _targets, _numerical, _categorical } );

    const auto modified_colnames =
        helpers::Macros::modify_colnames( manual, this );

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
                "t1.\"" + modified_colnames.at( i ) + "\"";

            const std::string data_type =
                ( i < _targets.size() + _numerical.size() ? "REAL" : "TEXT" );

            const bool no_comma = ( i == manual.size() - 1 );

            const auto end = no_comma ? "\"\n" : "\",\n";

            sql += begin + "CAST( " + edited_colname + " AS " + data_type +
                   " ) AS \"" + modified_colnames.at( i ) + end;
        }

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_sql(
    const std::string& _main_table,
    const std::vector<std::string>& _autofeatures,
    const std::vector<std::string>& _sql,
    const std::vector<std::string>& _targets,
    const std::vector<std::string>& _categorical,
    const std::vector<std::string>& _numerical ) const
{
    auto sql = _sql;

    sql.push_back( make_feature_table(
        _main_table, _autofeatures, _targets, _categorical, _numerical, "" ) );

    sql.push_back( make_postprocessing( _sql ) );

    return stl::collect::string( sql );
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_staging_table(
    const bool& _include_targets, const helpers::Schema& _schema ) const
{
    // ------------------------------------------------------------------------

    const auto columns = make_staging_columns( _include_targets, _schema );

    const auto name = SQLGenerator::make_staging_table_name( _schema.name_ );

    // ------------------------------------------------------------------------

    std::stringstream sql;

    // ------------------------------------------------------------------------

    sql << "DROP TABLE IF EXISTS \"" << SQLGenerator::to_upper( name )
        << "\";\n\n";

    sql << "CREATE TABLE \"" << SQLGenerator::to_upper( name )
        << "\" AS\nSELECT ";

    for ( size_t i = 0; i < columns.size(); ++i )
        {
            const auto begin = ( i == 0 ) ? "" : "       ";
            const auto end = ( i == columns.size() - 1 ) ? "\n" : ",\n";
            sql << begin << columns.at( i ) << end;
        }

    sql << "FROM \"" << SQLGenerator::get_table_name( _schema.name_ )
        << "\" t1\n";

    sql << SQLGenerator::handle_many_to_one_joins( _schema.name_, "t1", this );

    sql << ";" << std::endl << std::endl;

    sql << create_indices( name, _schema );

    sql << std::endl;

    // ------------------------------------------------------------------------

    return sql.str();
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLite3Generator::make_staging_tables(
    const bool _population_needs_targets,
    const std::vector<bool>& _peripheral_needs_targets,
    const Schema& _population_schema,
    const std::vector<Schema>& _peripheral_schema ) const
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

std::string SQLite3Generator::make_subfeature_joins(
    const std::string& _feature_prefix,
    const size_t _peripheral_used,
    const std::string& _alias,
    const std::string& _feature_postfix ) const
{
    assert_msg( _alias == "t1" || _alias == "t2", "_alias: " + _alias );

    assert_true( _feature_prefix.size() > 0 );

    std::stringstream sql;

    const auto number =
        ( _alias == "t2" )
            ? SQLGenerator::make_subfeature_identifier(
                  _feature_prefix, _peripheral_used )
            : _feature_prefix.substr( 0, _feature_prefix.size() - 1 );

    const auto letter = _feature_postfix == "" ? 'f' : 'p';

    sql << "LEFT JOIN \"FEATURES_" << number << _feature_postfix << "\" "
        << letter << "_" << number << std::endl;

    sql << "ON " << _alias << ".rowid = " << letter << "_" << number
        << ".\"rownum\"" << std::endl;

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_time_stamps(
    const std::string& _time_stamp_name,
    const std::string& _lower_time_stamp_name,
    const std::string& _upper_time_stamp_name,
    const std::string& _output_alias,
    const std::string& _input_alias,
    const std::string& _t1_or_t2 ) const
{
    std::stringstream sql;

    const auto make_ts_name =
        [this]( const std::string& _raw_name, const std::string& _alias ) {
            const auto colname = make_colname( _raw_name );
            return _alias + "." + quotechar1() + colname + quotechar2();
        };

    const auto colname1 = make_ts_name( _time_stamp_name, _output_alias );

    const auto colname2 = make_ts_name( _lower_time_stamp_name, _input_alias );

    sql << colname2 << " <= " << colname1 << std::endl;

    if ( _upper_time_stamp_name != "" )
        {
            const auto colname3 =
                make_ts_name( _upper_time_stamp_name, _input_alias );

            sql << "AND ( " << colname3 << " > " << colname1 << " OR "
                << colname3 << " IS NULL )" << std::endl;
        }

    return StringReplacer::replace_all(
        sql.str(), Macros::t1_or_t2(), _t1_or_t2 );
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::make_updates(
    const std::vector<std::string>& _autofeatures,
    const std::string& _prefix ) const
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

// ----------------------------------------------------------------------------

std::string SQLite3Generator::split_text_fields(
    const std::shared_ptr<ColumnDescription>& _desc ) const
{
    assert_true( _desc );

    const auto staging_table = SQLGenerator::to_upper(
        helpers::SQLGenerator::make_staging_table_name( _desc->table_ ) );

    const auto colname = SQLGenerator::to_lower( make_colname( _desc->name_ ) );

    const auto new_table =
        staging_table + "__" + SQLGenerator::to_upper( colname );

    return "DROP TABLE IF EXISTS \"" + new_table +
           "\";\n\n"
           "CREATE TABLE \"" +
           new_table +
           "\" AS\nWITH RECURSIVE\nsplit_text_field(i, field, word, "
           "rownum, n) AS (\n"
           "SELECT 1, field, get_word(field, 1), rownum, num_words(field)\n"
           "FROM ( SELECT t1.\"" +
           colname + "\" AS field, rowid AS rownum FROM \"" + staging_table +
           "\" t1 )\n"
           "UNION ALL\n"
           "SELECT i + 1, field, get_word(field, i + 1), rownum, n FROM "
           "split_text_field\n"
           "WHERE i < n\n)\n"
           "SELECT rownum, word AS \"" +
           colname + "\" FROM split_text_field;\n\n\n";
}

// ----------------------------------------------------------------------------

std::string SQLite3Generator::string_contains(
    const std::string& _colname,
    const std::string& _keyword,
    const bool _contains ) const
{
    const std::string comparison = _contains ? " > 0 " : " == 0 ";

    return "( contains( " + _colname + ", '" + _keyword + "' )" + comparison +
           ")";
}

// ----------------------------------------------------------------------------
}  // namespace helpers

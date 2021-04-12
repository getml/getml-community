#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

std::tuple<std::string, std::string, std::string>
SQLGenerator::demangle_colname( const std::string& _raw_name )
{
    // --------------------------------------------------------------

    const auto has_col_param =
        ( _raw_name.find( Macros::column() ) != std::string::npos );

    auto new_name = has_col_param
                        ? Macros::get_param( _raw_name, Macros::column() )
                        : _raw_name;

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

    const bool extract_alias = has_alias && not_t1_or_t2;

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

std::string SQLGenerator::join_mappings(
    const std::string& _name, const std::vector<std::string>& _mappings )
{
    const auto extract_colname = []( const std::string& _col ) -> std::string {
        const auto pos = _col.find( "__MAPPING_" );
        assert_true( pos != std::string::npos );
        return to_lower( _col.substr( 0, pos ) );
    };

    const auto join = [&_name, extract_colname](
                          const std::string& _colname ) -> std::string {
        return "UPDATE \"" + _name + "\"\nSET \"" + to_lower( _colname ) +
               "\" = t2.\"value\"\nFROM \"" + to_upper( _colname ) +
               "\" AS t2\nWHERE \"" + _name + "\".\"" +
               extract_colname( _colname ) + "\" = t2.\"key\";\n\n";
    };

    return stl::make::string( _mappings | std::views::transform( join ) );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_epoch_time(
    const std::string& _raw_name, const std::string& _alias )
{
    const auto colname = make_colname( _raw_name );

    return _alias + ".\"" + colname + "\"";

    /*if ( _raw_name.find( Macros::rowid() ) != std::string::npos )
        {
            return colname;
        }

    return "( julianday( " + colname +
           " ) - julianday( '1970-01-01' ) ) * 86400.0";*/
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

    /*if ( _raw_name.find( Macros::rowid() ) != std::string::npos )
        {
            return colname;
        }

    return "julianday( " + colname + " )";*/
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLGenerator::make_staging_columns(
    const bool& _include_targets,
    const Placeholder& _schema,
    const std::vector<std::string>& _mappings )
{
    // ------------------------------------------------------------------------

    const auto include_column = []( const std::string& _name ) {
        if ( _name == helpers::Macros::no_join_key() )
            {
                return false;
            }

        if ( _name == helpers::Macros::self_join_key() )
            {
                return false;
            }

        return true;
    };

    // ------------------------------------------------------------------------

    const auto cast_column = []( const std::string& _colname,
                                 const std::string& _coltype ) -> std::string {
        return "CAST( " + SQLGenerator::edit_colname( _colname, "t1" ) +
               " AS " + _coltype + " ) AS \"" +
               SQLGenerator::make_colname( _colname ) + "\"";
    };

    // ------------------------------------------------------------------------

    const auto to_epoch_time =
        []( const std::string& _colname ) -> std::string {
        const auto epoch_time =
            _colname.find( Macros::rowid() ) == std::string::npos
                ? "( julianday( " +
                      SQLGenerator::edit_colname( _colname, "t1" ) +
                      " ) - julianday( '1970-01-01' ) ) * 86400.0"
                : SQLGenerator::edit_colname( _colname, "t1" );

        return epoch_time + " AS \"" + SQLGenerator::make_colname( _colname ) +
               "\"";
    };

    // ------------------------------------------------------------------------

    const auto cast_as_numeric = [include_column, cast_column](
                                     const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "NUMERIC" );

        return stl::make::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( cast ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_time_stamp =
        [include_column,
         to_epoch_time]( const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        return stl::make::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( to_epoch_time ) );
    };

    // ------------------------------------------------------------------------

    const auto cast_as_text = [include_column, cast_column](
                                  const std::vector<std::string>& _colnames )
        -> std::vector<std::string> {
        const auto cast =
            std::bind( cast_column, std::placeholders::_1, "TEXT" );

        return stl::make::vector<std::string>(
            _colnames | std::views::filter( include_column ) |
            std::views::transform( cast ) );
    };

    // ------------------------------------------------------------------------

    const auto init_as_zero = []( const std::string& _colname ) -> std::string {
        return "0.0 AS \"" + to_lower( _colname ) + "\"";
    };

    // ------------------------------------------------------------------------

    const auto make_mappings = [&_mappings, init_as_zero]() {
        return stl::make::vector<std::string>(
            _mappings | std::views::transform( init_as_zero ) );
    };

    // ------------------------------------------------------------------------

    const auto categoricals = cast_as_text( _schema.categoricals_ );

    const auto discretes = cast_as_numeric( _schema.discretes_ );

    const auto join_keys = cast_as_text( _schema.join_keys_ );

    const auto numericals = cast_as_numeric( _schema.numericals_ );

    const auto targets = cast_as_numeric( _schema.targets_ );

    const auto text = cast_as_text( _schema.text_ );

    const auto time_stamps = cast_as_time_stamp( _schema.time_stamps_ );

    const auto mappings = make_mappings();

    // ------------------------------------------------------------------------

    const auto all = _include_targets ? std::vector<std::vector<std::string>>(
                                            { targets,
                                              categoricals,
                                              discretes,
                                              join_keys,
                                              numericals,
                                              text,
                                              time_stamps,
                                              mappings } )
                                      : std::vector<std::vector<std::string>>(
                                            { categoricals,
                                              discretes,
                                              join_keys,
                                              numericals,
                                              text,
                                              time_stamps,
                                              mappings } );

    // ------------------------------------------------------------------------

    return stl::make::vector<std::string>(
        all | std::ranges::views::join |
        std::ranges::views::filter( include_column ) );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_staging_table(
    const bool& _include_targets,
    const Placeholder& _schema,
    const std::vector<std::string>& _mappings )
{
    // ------------------------------------------------------------------------

    const auto columns =
        make_staging_columns( _include_targets, _schema, _mappings );

    const auto name = make_staging_table_name( _schema.name_ );

    // ------------------------------------------------------------------------

    std::stringstream sql;

    // ------------------------------------------------------------------------

    sql << "DROP TABLE IF EXISTS \"" << name << "\";\n\n";

    sql << "CREATE TABLE \"" << name << "\" AS\nSELECT ";

    for ( size_t i = 0; i < columns.size(); ++i )
        {
            const auto begin = ( i == 0 ) ? "" : "       ";
            const auto end = ( i == columns.size() - 1 ) ? "\n" : ",\n";
            sql << begin << columns.at( i ) << end;
        }

    sql << "FROM \"" << get_table_name( _schema.name_ ) << "\" t1\n";

    sql << handle_many_to_one_joins( _schema.name_, "t1" );

    sql << ";\n\n";

    sql << join_mappings( name, _mappings );

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

    return to_upper( get_table_name( _name ) ) + "__STAGING_TABLE_" + number;
}

// ----------------------------------------------------------------------------

std::vector<std::string> SQLGenerator::make_staging_tables(
    const bool& _include_targets,
    const Placeholder& _population_schema,
    const std::vector<Placeholder>& _peripheral_schema,
    const ColnameMap& _colname_map )
{
    // ------------------------------------------------------------------------

    const auto get_mapping =
        [&_colname_map](
            const Placeholder& _schema ) -> std::vector<std::string> {
        const auto it = _colname_map.find( _schema.name_ );

        if ( it == _colname_map.end() )
            {
                return std::vector<std::string>();
            }

        return it->second;
    };

    // ------------------------------------------------------------------------

    auto sql = std::vector<std::string>( { make_staging_table(
        _include_targets,
        _population_schema,
        get_mapping( _population_schema ) ) } );

    // ------------------------------------------------------------------------

    for ( size_t i = 0; i < _peripheral_schema.size(); ++i )
        {
            const auto& schema = _peripheral_schema.at( i );

            auto s = make_staging_table( false, schema, get_mapping( schema ) );

            sql.emplace_back( std::move( s ) );
        }

    // ------------------------------------------------------------------------

    return sql;
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_subfeature_identifier(
    const std::string& _feature_prefix,
    const size_t _peripheral_used,
    const size_t _column )
{
    return _feature_prefix + std::to_string( _peripheral_used + 1 ) + "_" +
           std::to_string( _column + 1 );
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_subfeature_joins(
    const std::string& _feature_prefix,
    const size_t _peripheral_used,
    const std::set<size_t>& _columns )
{
    std::stringstream sql;

    for ( const auto col : _columns )
        {
            const auto number = make_subfeature_identifier(
                _feature_prefix, _peripheral_used, col );

            sql << "LEFT JOIN \"FEATURE_" << number << "\" f_" << number
                << std::endl;

            sql << "ON t2.rowid = f_" << number << ".\"rownum\"" << std::endl;
        }

    return sql.str();
}

// ----------------------------------------------------------------------------

std::string SQLGenerator::make_time_stamp_diff(
    const Float _diff, const bool _is_rowid )
{
    if ( _is_rowid )
        {
            return Macros::diffstr() + " + " + std::to_string( _diff );
        }

    constexpr Float seconds_per_day = 24.0 * 60.0 * 60.0;
    constexpr Float seconds_per_hour = 60.0 * 60.0;
    constexpr Float seconds_per_minute = 60.0;

    const auto abs_diff = std::abs( _diff );

    auto diffstr = std::to_string( _diff ) + " seconds";

    if ( abs_diff >= seconds_per_day )
        {
            diffstr = std::to_string( _diff / seconds_per_day ) + " days";
        }
    else if ( abs_diff >= seconds_per_hour )
        {
            diffstr = std::to_string( _diff / seconds_per_hour ) + " hours";
        }
    else if ( abs_diff >= seconds_per_minute )
        {
            diffstr = std::to_string( _diff / seconds_per_minute ) + " minutes";
        }

    const std::string sign = _diff >= 0.0 ? "+" : "";

    return Macros::diffstr() + ", '" + sign + diffstr + "'";
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

// ------------------------------------------------------------------------

std::string SQLGenerator::to_lower( const std::string& _str )
{
    auto lower = _str;
    for ( auto& c : lower )
        {
            c = std::tolower( c );
        }
    return lower;
}

// ------------------------------------------------------------------------

std::string SQLGenerator::to_upper( const std::string& _str )
{
    auto upper = _str;
    for ( auto& c : upper )
        {
            c = std::toupper( c );
        }
    return upper;
}

// ----------------------------------------------------------------------------
}  // namespace helpers

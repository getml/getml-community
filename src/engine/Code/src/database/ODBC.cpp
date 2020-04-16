#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

void ODBC::drop_table( const std::string& _tname )
{
    std::string query = "DROP TABLE ";

    if ( escape_char1_ != ' ' )
        {
            query += escape_char1_;
        }

    query += _tname;

    if ( escape_char2_ != ' ' )
        {
            query += escape_char2_;
        }

    query += ";";

    execute( query );
}

// ----------------------------------------------------------------------------

std::pair<char, char> ODBC::extract_escape_chars(
    const Poco::JSON::Object& _obj ) const
{
    const auto escape_chars =
        jsonutils::JSON::get_value<std::string>( _obj, "escape_chars_" );

    switch ( escape_chars.size() )
        {
            case 0:
                return std::make_pair( ' ', ' ' );

            case 1:
                return std::make_pair( escape_chars[0], escape_chars[0] );

            case 2:
                return std::make_pair( escape_chars[0], escape_chars[1] );

            default:
                throw std::invalid_argument(
                    "escape_chars cannot contain more than two "
                    "characters." );
        }

    return std::make_pair( ' ', ' ' );
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_colnames( const std::string& _table ) const
{
    auto query = std::string( "SELECT * FROM " );

    if ( escape_char1_ != ' ' )
        {
            query += escape_char1_;
        }

    query += _table;

    if ( escape_char2_ != ' ' )
        {
            query += escape_char2_;
        }

    query += ';';

    const auto iter = ODBCIterator( make_connection(), query, time_formats_ );

    return iter.colnames();
}

// ----------------------------------------------------------------------------

std::vector<io::Datatype> ODBC::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const auto conn = ODBCConn( env(), server_name_, user_, passwd_ );

    auto query = std::string( "SELECT * FROM " );

    if ( escape_char1_ != ' ' )
        {
            query += escape_char1_;
        }

    query += _table;

    if ( escape_char2_ != ' ' )
        {
            query += escape_char2_;
        }

    query += ';';

    const auto stmt = ODBCStmt( conn, query );

    SQLSMALLINT ncols = 0;

    auto ret = SQLNumResultCols( stmt.handle_, &ncols );

    ODBCError::check(
        ret,
        "SQLNumResultCols in get_coltypes",
        stmt.handle_,
        SQL_HANDLE_STMT );

    SQLSMALLINT name_length = 0;
    SQLSMALLINT data_type = 0;
    SQLULEN column_size = 0;
    SQLSMALLINT decimal_digits = 0;
    SQLSMALLINT nullable = 0;

    auto buffer = std::make_unique<SQLCHAR[]>( 1024 );

    auto coltypes = std::vector<io::Datatype>( ncols );

    for ( SQLSMALLINT i = 0; i < ncols; ++i )
        {
            ret = SQLDescribeCol(
                stmt.handle_,
                i + 1,
                buffer.get(),
                1024,
                &name_length,
                &data_type,
                &column_size,
                &decimal_digits,
                &nullable );

            ODBCError::check(
                ret,
                "SQLDescribeCol in get_coltypes",
                stmt.handle_,
                SQL_HANDLE_STMT );

            coltypes.at( i ) = interpret_field_type( data_type );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_catalogs() const
{
    constexpr SQLSMALLINT ncols = 5;

    const auto conn = ODBCConn( env(), server_name_, user_, passwd_ );

    auto stmt = ODBCStmt( conn );

    auto ret = SQLTables(
        stmt.handle_,
        (SQLCHAR*)SQL_ALL_CATALOGS,
        SQL_NTS,
        (SQLCHAR*)"",
        SQL_NTS,
        (SQLCHAR*)"",
        SQL_NTS,
        (SQLCHAR*)"",
        SQL_NTS );

    ODBCError::check(
        ret, "SQLTables in get_catalogs", stmt.handle_, SQL_HANDLE_STMT );

    auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

    auto lens = std::array<SQLLEN, ncols>();

    for ( SQLSMALLINT i = 0; i < ncols; ++i )
        {
            data[i] = std::make_unique<SQLCHAR[]>( 1024 );

            ret = SQLBindCol(
                stmt.handle_,
                i + 1,
                SQL_C_CHAR,
                data[i].get(),
                1024,
                &lens[i] );

            ODBCError::check(
                ret,
                "SQLBindCol in get_catalogs",
                stmt.handle_,
                SQL_HANDLE_STMT );
        }

    auto vec = std::vector<std::string>( {""} );

    while ( true )
        {
            ret = SQLFetch( stmt.handle_ );

            if ( ret == SQL_NO_DATA )
                {
                    return vec;
                }

            ODBCError::check(
                ret,
                "SQLFetch in get_catalogs",
                stmt.handle_,
                SQL_HANDLE_STMT );

            if ( std::get<0>( lens ) != SQL_NULL_DATA )
                {
                    vec.push_back( std::string( reinterpret_cast<const char*>(
                        std::get<0>( data ).get() ) ) );
                }
        }

    return vec;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object ODBC::get_content(
    const std::string& _tname,
    const std::int32_t _draw,
    const std::int32_t _start,
    const std::int32_t _length )
{
    // ----------------------------------------

    const auto nrows = get_nrows( _tname );

    const auto colnames = get_colnames( _tname );

    const auto ncols = colnames.size();

    // ----------------------------------------

    if ( _length < 0 )
        {
            throw std::invalid_argument( "length must be positive!" );
        }

    if ( _start < 0 )
        {
            throw std::invalid_argument( "start must be positive!" );
        }

    if ( _start >= nrows )
        {
            throw std::invalid_argument(
                "start must be smaller than number of rows!" );
        }

    // ----------------------------------------

    Poco::JSON::Object obj;

    // ----------------------------------------

    obj.set( "draw", _draw );

    obj.set( "recordsTotal", nrows );

    obj.set( "recordsFiltered", nrows );

    // ----------------------------------------

    const auto begin = _start;

    const auto end = ( _start + _length > nrows ) ? nrows : _start + _length;

    const auto query = make_get_content_query( _tname, colnames, begin, end );

    // ----------------------------------------

    auto iterator = std::make_shared<ODBCIterator>(
        make_connection(), query, time_formats_ );

    // ----------------------------------------

    Poco::JSON::Array data;

    for ( auto i = begin; i < end; ++i )
        {
            Poco::JSON::Array row;

            for ( size_t j = 0; j < ncols; ++j )
                {
                    row.add( iterator->get_string() );
                }

            data.add( row );
        }

    // ----------------------------------------

    obj.set( "data", data );

    // ----------------------------------------

    return obj;

    // ----------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_schemas( const std::string& _catalog ) const
{
    constexpr SQLSMALLINT ncols = 5;

    const auto catalog = to_ptr( _catalog );

    const auto conn = ODBCConn( env(), server_name_, user_, passwd_ );

    auto stmt = ODBCStmt( conn );

    auto ret = SQLTables(
        stmt.handle_,
        (SQLCHAR*)"",
        SQL_NTS,
        (SQLCHAR*)SQL_ALL_SCHEMAS,
        SQL_NTS,
        (SQLCHAR*)"",
        SQL_NTS,
        (SQLCHAR*)"",
        SQL_NTS );

    ODBCError::check(
        ret, "SQLTables in get_schemas", stmt.handle_, SQL_HANDLE_STMT );

    auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

    auto lens = std::array<SQLLEN, ncols>();

    for ( SQLSMALLINT i = 0; i < ncols; ++i )
        {
            data[i] = std::make_unique<SQLCHAR[]>( 1024 );

            ret = SQLBindCol(
                stmt.handle_,
                i + 1,
                SQL_C_CHAR,
                data[i].get(),
                1024,
                &lens[i] );

            ODBCError::check(
                ret,
                "SQLBindCol in get_schemas",
                stmt.handle_,
                SQL_HANDLE_STMT );
        }

    auto vec = std::vector<std::string>( {""} );

    while ( true )
        {
            ret = SQLFetch( stmt.handle_ );

            if ( ret == SQL_NO_DATA )
                {
                    return vec;
                }

            ODBCError::check(
                ret, "SQLFetch in get_schemas", stmt.handle_, SQL_HANDLE_STMT );

            if ( std::get<1>( lens ) != SQL_NULL_DATA )
                {
                    vec.push_back( std::string( reinterpret_cast<const char*>(
                        std::get<1>( data ).get() ) ) );
                }
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::get_tables(
    const std::string& _catalog, const std::string& _schema ) const
{
    constexpr SQLSMALLINT ncols = 5;

    const auto catalog = to_ptr( _catalog );

    const auto schema = to_ptr( _catalog );

    const auto conn = ODBCConn( env(), server_name_, user_, passwd_ );

    auto stmt = ODBCStmt( conn );

    auto ret = SQLTables(
        stmt.handle_,
        catalog.get(),
        SQL_NTS,
        schema.get(),
        SQL_NTS,
        (SQLCHAR*)SQL_ALL_TABLE_TYPES,
        SQL_NTS,
        (SQLCHAR*)"'TABLE'",
        SQL_NTS );

    ODBCError::check(
        ret, "SQLTables in get_tables", stmt.handle_, SQL_HANDLE_STMT );

    auto data = std::array<std::unique_ptr<SQLCHAR[]>, ncols>();

    auto lens = std::array<SQLLEN, ncols>();

    auto vec = std::vector<std::string>();

    while ( true )
        {
            ret = SQLFetch( stmt.handle_ );

            if ( ret == SQL_NO_DATA )
                {
                    return vec;
                }

            ODBCError::check(
                ret, "SQLFetch in get_tables", stmt.handle_, SQL_HANDLE_STMT );

            if ( std::get<0>( lens ) != SQL_NULL_DATA )
                {
                    vec.push_back( std::string( reinterpret_cast<const char*>(
                        std::get<0>( data ).get() ) ) );
                }
        }
}

// ----------------------------------------------------------------------------

io::Datatype ODBC::interpret_field_type( const SQLSMALLINT _type ) const
{
    switch ( _type )
        {
            case SQL_DECIMAL:
            case SQL_NUMERIC:
            case SQL_REAL:
            case SQL_FLOAT:
            case SQL_DOUBLE:
                return io::Datatype::double_precision;

            case SQL_SMALLINT:
            case SQL_INTEGER:
            case SQL_TINYINT:
            case SQL_BIGINT:
                return io::Datatype::integer;

            default:
                return io::Datatype::string;
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> ODBC::list_tables()
{
    // ------------------------------------------------------

    auto all_tables = std::vector<std::string>();

    // ------------------------------------------------------

    const auto catalogs = get_catalogs();

    for ( const auto& cat : catalogs )
        {
            const auto schemas = get_schemas( cat );

            for ( const auto& sch : schemas )
                {
                    const auto tables = get_tables( cat, sch );

                    for ( const auto& table : tables )
                        {
                            std::string tname;

                            if ( cat != "" )
                                {
                                    tname += cat + ".";
                                }

                            if ( sch != "" )
                                {
                                    tname += sch + ".";
                                }

                            tname += table;

                            all_tables.push_back( tname );
                        }
                }
        }

    // ------------------------------------------------------

    if ( all_tables.size() > 0 )
        {
            return all_tables;
        }

    // ------------------------------------------------------

    try
        {
            auto iter = ODBCIterator(
                make_connection(), "SHOW TABLES;", time_formats_ );

            while ( !iter.end() )
                {
                    all_tables.push_back( iter.get_string() );
                }
        }
    catch ( std::exception& e )
        {
        }

    // ------------------------------------------------------

    return all_tables;

    // ------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string ODBC::make_get_content_query(
    const std::string& _table,
    const std::vector<std::string>& _colnames,
    const std::int32_t _begin,
    const std::int32_t _end ) const
{
    assert_true( _end >= _begin );

    std::string query = "SELECT ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            query += "`";
            query += _colnames[i];
            query += "`";

            if ( i != _colnames.size() - 1 )
                {
                    query += ",";
                }

            query += " ";
        }

    query += "FROM `";

    query += _table;

    query += "` LIMIT " + std::to_string( _end - _begin );

    query += " OFFSET " + std::to_string( _begin );

    query += ";";

    return query;
}
// ----------------------------------------------------------------------------
}  // namespace database

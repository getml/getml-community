
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

void Sqlite3::check_colnames(
    const std::vector<std::string>& _colnames, csv::Reader* _reader )
{
    std::vector<std::string> csv_colnames = _reader->next_line();

    if ( csv_colnames.size() != _colnames.size() )
        {
            throw std::runtime_error(
                "Wrong number of columns. Expected " +
                std::to_string( _colnames.size() ) + ", saw " +
                std::to_string( csv_colnames.size() ) + "." );
        }

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            if ( csv_colnames[i] != _colnames[i] )
                {
                    throw std::runtime_error(
                        "Column " + std::to_string( i + 1 ) +
                        " has wrong name. Expected '" + _colnames[i] +
                        "', saw '" + csv_colnames[i] + "'." );
                }
        }
}

// ----------------------------------------------------------------------------

void Sqlite3::execute( const std::string& _sql )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    char* error_message = nullptr;

    int rc = sqlite3_exec(
        db(), _sql.c_str(), Sqlite3::do_nothing, 0, &error_message );

    if ( error_message != nullptr )
        {
            throw_exception( error_message );
        }

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error( "Query could not be executed!" );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> Sqlite3::get_colnames(
    const std::string& _table ) const
{
    // ------------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // ------------------------------------------------------------------------
    // Prepare statement.

    const std::string sql = "SELECT * FROM " + _table + " LIMIT 0";

    // We set this to nullptr, so it will not be deleted if doesn't point to
    // anything.
    // https://en.cppreference.com/w/cpp/memory/unique_ptr/operator_bool
    sqlite3_stmt* stmt = nullptr;

    int rc = sqlite3_prepare_v2(
        db(),                            // Database handle.
        sql.c_str(),                     // SQL statement, UTF-8 encoded.
        static_cast<int>( sql.size() ),  // Maximum length of zSql in bytes.
        &stmt,                           // OUT: Statement handle.
        NULL  // OUT: Pointer to unused portion of zSql
    );

    // The unique_ptr takes ownership of stmt and finalizes it when
    // necessary.
    const std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )> ptr(
        stmt, sqlite3_finalize );

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error( sqlite3_errmsg( db() ) );
        }

    // ------------------------------------------------------------------------
    // Fill colnames.

    std::vector<std::string> colnames;

    const int num_cols = sqlite3_column_count( stmt );

    for ( int i = 0; i < num_cols; ++i )
        {
            const auto name = sqlite3_column_name( stmt, i );

            colnames.push_back( name );
        }

    // ------------------------------------------------------------------------

    return colnames;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<csv::Datatype> Sqlite3::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    std::vector<csv::Datatype> datatypes;

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            int pNotNull = 0, pPrimaryKey = 0, pAutoinc = 0;

            char const* data_type = nullptr;

            char const* pzCollSeq = nullptr;

            int rc = sqlite3_table_column_metadata(
                db(),                  // Connection handle
                NULL,                  // Database name or NULL
                _table.c_str(),        // Table name
                _colnames[i].c_str(),  // Column name
                &data_type,            // OUTPUT: Declared data type
                &pzCollSeq,            // OUTPUT: Collation sequence name
                &pNotNull,     // OUTPUT: True if NOT NULL constraint exists
                &pPrimaryKey,  // OUTPUT: True if column part of PK
                &pAutoinc      // OUTPUT: True if column is auto-increment
            );

            if ( rc != SQLITE_OK )
                {
                    throw std::runtime_error( sqlite3_errmsg( db() ) );
                }

            assert_true( data_type != nullptr );

            const auto str = std::string( data_type );

            if ( str == "REAL" )
                {
                    datatypes.push_back( csv::Datatype::double_precision );
                }
            else if ( str == "INTEGER" )
                {
                    datatypes.push_back( csv::Datatype::integer );
                }
            else
                {
                    datatypes.push_back( csv::Datatype::string );
                }
        }

    return datatypes;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Sqlite3::get_content(
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

    // ----------------------------------------

    const auto where = std::string( "rowid > " ) + std::to_string( begin ) +
                       std::string( " AND rowid <= " ) + std::to_string( end );

    auto iterator = select( colnames, _tname, where );

    // ----------------------------------------

    Poco::JSON::Array data;

    while ( !iterator->end() )
        {
            Poco::JSON::Array row;

            for ( size_t i = 0; i < ncols; ++i )
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

void Sqlite3::insert_line(
    const std::vector<std::string>& _line,
    const std::vector<csv::Datatype>& _coltypes,
    sqlite3_stmt* _stmt ) const
{
    const int num_cols = static_cast<int>( _line.size() );

    for ( int i = 0; i < num_cols; ++i )
        {
            switch ( _coltypes[i] )
                {
                    case csv::Datatype::double_precision:
                        insert_double( _line, i, _stmt );
                        break;

                    case csv::Datatype::integer:
                        insert_int( _line, i, _stmt );
                        break;

                    default:
                        insert_text( _line, i, _stmt );
                }
        }

    int rc = sqlite3_step( _stmt );

    if ( rc != SQLITE_OK && rc != SQLITE_ROW && rc != SQLITE_DONE )
        {
            throw std::runtime_error( sqlite3_errmsg( db() ) );
        }

    rc = sqlite3_reset( _stmt );

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error( sqlite3_errmsg( db() ) );
        }
}

// ----------------------------------------------------------------------------

void Sqlite3::insert_double(
    const std::vector<std::string>& _line,
    const int _colnum,
    sqlite3_stmt* _stmt ) const
{
    int rc = 0;

    const auto [val, success] = csv::Parser::to_double( _line[_colnum] );

    if ( success )
        {
            rc = sqlite3_bind_double( _stmt, _colnum + 1, val );
        }
    else
        {
            rc = sqlite3_bind_null( _stmt, _colnum + 1 );
        }

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error(
                "Could not insert value: '" + _line[_colnum] + "'" );
        }
}

// ----------------------------------------------------------------------------

void Sqlite3::insert_int(
    const std::vector<std::string>& _line,
    const int _colnum,
    sqlite3_stmt* _stmt ) const
{
    int rc = 0;

    const auto [val, success] = csv::Parser::to_int( _line[_colnum] );

    if ( success )
        {
            rc =
                sqlite3_bind_int( _stmt, _colnum + 1, static_cast<int>( val ) );
        }
    else
        {
            rc = sqlite3_bind_null( _stmt, _colnum + 1 );
        }

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error(
                "Could not insert value: '" + _line[_colnum] + "'" );
        }
}

// ----------------------------------------------------------------------------

void Sqlite3::insert_text(
    const std::vector<std::string>& _line,
    const int _colnum,
    sqlite3_stmt* _stmt ) const
{
    int rc = sqlite3_bind_text(
        _stmt, _colnum + 1, _line[_colnum].c_str(), -1, SQLITE_STATIC );

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error(
                "Could not insert value: '" + _line[_colnum] + "'" );
        }
}

// ----------------------------------------------------------------------------

std::vector<std::string> Sqlite3::list_tables()
{
    auto iterator = select( {"name"}, "sqlite_master", "type='table'" );

    std::vector<std::string> tables;

    while ( !iterator->end() )
        {
            tables.push_back( iterator->get_string() );
        }

    std::sort( tables.begin(), tables.end() );

    return tables;
}

// ----------------------------------------------------------------------------

std::shared_ptr<sqlite3> Sqlite3::make_db( const std::string& _name )
{
    sqlite3* raw_ptr = nullptr;

    int rc = sqlite3_open( _name.c_str(), &raw_ptr );

    if ( rc )
        {
            throw std::runtime_error( sqlite3_errmsg( raw_ptr ) );
        }

    return std::shared_ptr<sqlite3>( raw_ptr, sqlite3_close );
}

// ----------------------------------------------------------------------------

std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>
Sqlite3::make_insert_statement(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    // ------------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // ------------------------------------------------------------------------
    // Prepare statement as string

    std::string sql = "INSERT INTO '";
    sql += _table;
    sql += "' VALUES (";

    for ( size_t col = 0; col < _colnames.size(); ++col )
        {
            sql += '?';

            if ( col + 1 < _colnames.size() )
                {
                    sql += ',';
                }
            else
                {
                    sql += ')';
                }
        }

    // ------------------------------------------------------------------------
    // Make actual sqlite statement.

    // We set this to nullptr, so it will not be deleted if doesn't point to
    // anything.
    // https://en.cppreference.com/w/cpp/memory/unique_ptr/operator_bool
    sqlite3_stmt* raw_ptr = nullptr;

    int rc = sqlite3_prepare_v2(
        db(),                            // Database handle.
        sql.c_str(),                     // SQL statement, UTF-8 encoded.
        static_cast<int>( sql.size() ),  // Maximum length of zSql in bytes.
        &raw_ptr,                        // OUT: Statement handle.
        NULL  // OUT: Pointer to unused portion of zSql
    );

    // The unique_ptr takes ownership of stmt and finalizes it when
    // necessary.
    std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )> stmt(
        raw_ptr, sqlite3_finalize );

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error( sqlite3_errmsg( db() ) );
        }

    // ------------------------------------------------------------------------

    return stmt;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sqlite3::read(
    const std::string& _table,
    const bool _header,
    const size_t _skip,
    csv::Reader* _reader )
{
    // ------------------------------------------------------------------------
    // Get colnames and coltypes

    const std::vector<std::string> colnames = get_colnames( _table );

    const std::vector<csv::Datatype> coltypes =
        get_coltypes( _table, colnames );

    if ( colnames.size() != coltypes.size() )
        {
            throw std::runtime_error(
                "Table '" + _table + "' has been altered while reading!" );
        }

    // ------------------------------------------------------------------------
    // Prepare INSERT INTO SQL statement.

    const auto stmt = make_insert_statement( _table, colnames );

    // ------------------------------------------------------------------------
    // Skip lines, if necessary.

    size_t line_count = 0;

    for ( size_t i = 0; i < _skip; ++i )
        {
            _reader->next_line();
            ++line_count;
        }

    // ------------------------------------------------------------------------
    // Check headers, if necessary.

    if ( _header )
        {
            check_colnames( colnames, _reader );
            ++line_count;
        }

    // ------------------------------------------------------------------------

    execute( "BEGIN;" );

    // ----------------------------------------------------------------
    // Insert line by line, then COMMIT.
    // If something goes wrong, call ROLLBACK.

    try
        {
            // ----------------------------------------------------------------
            // Insert line by line.

            multithreading::WriteLock write_lock( read_write_lock_ );

            while ( !_reader->eof() )
                {
                    std::vector<std::string> line = _reader->next_line();

                    ++line_count;

                    if ( line.size() == 0 )
                        {
                            continue;
                        }
                    else if ( line.size() != colnames.size() )
                        {
                            std::cout << "Corrupted line: " << line_count
                                      << ". Expected " << colnames.size()
                                      << " fields, saw " << line.size() << "."
                                      << std::endl;
                            continue;
                        }

                    insert_line( line, coltypes, stmt.get() );
                }

            write_lock.unlock();

            // ----------------------------------------------------------------

            execute( "COMMIT;" );

            // ----------------------------------------------------------------
        }
    catch ( std::exception& e )
        {
            execute( "ROLLBACK;" );

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace database

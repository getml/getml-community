
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

Sqlite3Iterator::Sqlite3Iterator(
    const std::shared_ptr<sqlite3>& _db,
    const std::vector<std::string>& _colnames,
    const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock,
    const std::vector<std::string>& _time_formats,
    const std::string& _tname,
    const std::string& _where )
    : colnum_( 0 ),
      db_( _db ),
      end_( false ),
      num_cols_( static_cast<int>( _colnames.size() ) ),
      read_lock_( multithreading::ReadLock( _read_write_lock ) ),
      stmt_( std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>(
          nullptr, sqlite3_finalize ) ),
      time_formats_( _time_formats )
{
    // ------------------------------------------------------------------------
    // Prepare SQL query.

    std::string sql = "SELECT ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            sql += _colnames[i];
            if ( i + 1 < _colnames.size() )
                {
                    sql += ", ";
                }
        }

    sql += " FROM " + _tname;

    if ( _where != "" )
        {
            sql += " WHERE " + _where;
        }

    sql += ";";

    // ------------------------------------------------------------------------
    // Prepare statement.

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

    // The unique_ptr takes ownership of stmt and finalizes it when necessary.
    stmt_ = std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>(
        raw_ptr, sqlite3_finalize );

    if ( rc != SQLITE_OK )
        {
            throw std::runtime_error( sqlite3_errmsg( db() ) );
        }

    // ------------------------------------------------------------------------

    next_row();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

DATABASE_FLOAT Sqlite3Iterator::get_double()
{
    assert( !end_ );

    auto val =
        static_cast<DATABASE_FLOAT>( sqlite3_column_double( stmt(), colnum_ ) );

    // sqlite3_column_double(...) returns 0.0 when the value is NULL.
    if ( val == 0.0 )
        {
            const auto txt = sqlite3_column_text( stmt(), colnum_ );

            // sqlite3_column_text(...) returns NULL when the value is NULL.
            if ( txt )
                {
                    try
                        {
                            val = csv::Parser::to_double( std::string(
                                reinterpret_cast<const char*>( txt ) ) );
                        }
                    catch ( std::exception& e )
                        {
                            val = NAN;
                        }
                }
            else
                {
                    val = NAN;
                }
        }

    if ( ++colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

DATABASE_INT Sqlite3Iterator::get_int()
{
    assert( !end_ );

    const auto val =
        static_cast<DATABASE_INT>( sqlite3_column_int( stmt(), colnum_++ ) );

    if ( colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

std::string Sqlite3Iterator::get_string()
{
    assert( !end_ );

    const auto ptr = sqlite3_column_text( stmt(), colnum_++ );

    std::string val;

    // sqlite3_column_text(...) returns NULL when the value is NULL.
    if ( ptr )
        {
            val = reinterpret_cast<const char*>( ptr );
        }
    else
        {
            val = "NULL";
        }

    if ( colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

DATABASE_FLOAT Sqlite3Iterator::get_time_stamp()
{
    assert( !end_ );

    const auto ptr = sqlite3_column_text( stmt(), colnum_ );

    DATABASE_FLOAT val = 0.0;

    // sqlite3_column_text(...) returns NULL when the value is NULL.
    if ( ptr )
        {
            const std::string str = reinterpret_cast<const char*>( ptr );

            try
                {
                    val = csv::Parser::to_time_stamp( str, time_formats_ );
                }
            catch ( std::exception& e )
                {
                    return get_double();
                }
        }
    else
        {
            val = NAN;
        }

    if ( colnum_++ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

}  // namespace database

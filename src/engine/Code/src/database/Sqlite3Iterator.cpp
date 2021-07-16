
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

Sqlite3Iterator::Sqlite3Iterator(
    const std::shared_ptr<sqlite3>& _db,
    const std::string& _sql,
    const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock,
    const std::vector<std::string>& _time_formats )
    : colnum_( 0 ),
      db_( _db ),
      end_( false ),
      read_lock_( multithreading::ReadLock(
          _read_write_lock, std::chrono::milliseconds( 1000 ) ) ),
      stmt_( std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>(
          nullptr, sqlite3_finalize ) ),
      time_formats_( _time_formats )
{
    // ------------------------------------------------------------------------
    // Prepare statement.

    // We set this to nullptr, so it will not be deleted if doesn't point to
    // anything.
    // https://en.cppreference.com/w/cpp/memory/unique_ptr/operator_bool
    sqlite3_stmt* raw_ptr = nullptr;

    int rc = sqlite3_prepare_v2(
        db(),                             // Database handle.
        _sql.c_str(),                     // SQL statement, UTF-8 encoded.
        static_cast<int>( _sql.size() ),  // Maximum length of zSql in bytes.
        &raw_ptr,                         // OUT: Statement handle.
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

    num_cols_ = sqlite3_column_count( stmt_.get() );

    // ------------------------------------------------------------------------

    next_row();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Sqlite3Iterator::Sqlite3Iterator(
    const std::shared_ptr<sqlite3>& _db,
    const std::vector<std::string>& _colnames,
    const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock,
    const std::vector<std::string>& _time_formats,
    const std::string& _tname,
    const std::string& _where )
    : Sqlite3Iterator(
          _db,
          make_sql( _colnames, _tname, _where ),
          _read_write_lock,
          _time_formats )

{
}

// ----------------------------------------------------------------------------

std::vector<std::string> Sqlite3Iterator::colnames() const
{
    std::vector<std::string> colnames( num_cols_ );

    for ( int i = 0; i < num_cols_; ++i )
        {
            colnames[i] = sqlite3_column_name( stmt(), i );
        }

    return colnames;
}

// ----------------------------------------------------------------------------

Float Sqlite3Iterator::get_double()
{
    if ( end_ )
        {
            throw std::runtime_error( "End of table!" );
        }

    auto val = static_cast<Float>( sqlite3_column_double( stmt(), colnum_ ) );

    // sqlite3_column_double(...) returns 0.0 when the value is NULL.
    if ( val == 0.0 )
        {
            const auto txt = sqlite3_column_text( stmt(), colnum_ );

            // sqlite3_column_text(...) returns NULL when the value is NULL.
            if ( txt )
                {
                    const auto str =
                        std::string( reinterpret_cast<const char*>( txt ) );

                    val = Getter::get_double( str );
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

Int Sqlite3Iterator::get_int()
{
    if ( end_ )
        {
            throw std::runtime_error( "End of table!" );
        }

    const auto val = static_cast<Int>( sqlite3_column_int( stmt(), colnum_ ) );

    if ( ++colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

std::string Sqlite3Iterator::get_string()
{
    if ( end_ )
        {
            throw std::runtime_error( "End of table!" );
        }

    const auto ptr = sqlite3_column_text( stmt(), colnum_ );

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

    if ( ++colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

Float Sqlite3Iterator::get_time_stamp()
{
    if ( end_ )
        {
            throw std::runtime_error( "End of table!" );
        }

    const auto ptr = sqlite3_column_text( stmt(), colnum_ );

    Float val = 0.0;

    // sqlite3_column_text(...) returns NULL when the value is NULL.
    if ( ptr )
        {
            const std::string str = reinterpret_cast<const char*>( ptr );

            val = Getter::get_time_stamp( str, time_formats_ );
        }
    else
        {
            val = NAN;
        }

    if ( ++colnum_ == num_cols_ )
        {
            next_row();
            colnum_ = 0;
        }

    return val;
}

// ----------------------------------------------------------------------------

std::string Sqlite3Iterator::make_sql(
    const std::vector<std::string>& _colnames,
    const std::string& _tname,
    const std::string& _where )
{
    std::string sql = "SELECT ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            if ( _colnames[i] == "COUNT(*)" )
                {
                    sql += _colnames[i];
                }
            else
                {
                    sql += "\"";
                    sql += _colnames[i];
                    sql += "\"";
                }

            if ( i + 1 < _colnames.size() )
                {
                    sql += ", ";
                }
        }

    sql += " FROM \"" + _tname + "\"";

    if ( _where != "" )
        {
            sql += " WHERE " + _where;
        }

    sql += ";";

    return sql;
}

// ----------------------------------------------------------------------------

}  // namespace database

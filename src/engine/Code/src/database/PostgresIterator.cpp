
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------
// Refer to the following sources in the documentation:
// https://www.postgresql.org/docs/8.4/libpq-example.html
// https://www.postgresql.org/docs/8.1/sql-fetch.html

PostgresIterator::PostgresIterator(
    const std::shared_ptr<PGconn>& _connection,
    const std::vector<std::string>& _colnames,
    const std::vector<std::string>& _time_formats,
    const std::string& _tname,
    const std::string& _where )
    : close_required_( false ),
      colnum_( 0 ),
      connection_( _connection ),
      end_required_( false ),
      num_cols_( static_cast<int>( _colnames.size() ) ),
      rownum_( 0 ),
      time_formats_( _time_formats )
{
    // ------------------------------------------------------------------------
    // Prepare SQL query.

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

    // ------------------------------------------------------------------------
    // Begin transaction and prepare cursor.

    execute( "BEGIN" );

    end_required_ = true;

    execute( "DECLARE scalemlcursor CURSOR FOR " + sql );

    close_required_ = true;

    // ------------------------------------------------------------------------
    // Fetch the first 10000 rows.

    fetch_next_10000();

    if ( end() )
        {
            throw std::runtime_error( "Query returned no results!" );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

PostgresIterator::~PostgresIterator()
{
    if ( close_required_ )
        {
            close_cursor();
        }

    if ( end_required_ )
        {
            end_transaction();
        }
}

// ----------------------------------------------------------------------------

}  // namespace database

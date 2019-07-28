
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

// https://stackoverflow.com/questions/23332978/c-postgres-libpqxx-huge-query

PostgresIterator::PostgresIterator(
    const std::shared_ptr<pqxx::connection>& _connection,
    const std::vector<std::string>& _colnames,
    const std::vector<std::string>& _time_formats,
    const std::string& _tname,
    const std::string& _where )
    : colnum_( 0 ),
      connection_( _connection ),
      num_cols_( _colnames.size() ),
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
    // Prepare work and rows.

    work_ = std::make_unique<pqxx::work>( connection() );

    rows_ = std::make_unique<pqxx::result>( work().exec( sql ) );

    rows_iterator_ = rows().begin();

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

PostgresIterator::~PostgresIterator() = default;

// ----------------------------------------------------------------------------

}  // namespace database

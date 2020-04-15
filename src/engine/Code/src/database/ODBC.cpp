#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::vector<io::Datatype> ODBC::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const auto conn = ODBCConn( env(), server_name_, user_, passwd_ );

    const auto query = std::string( "SELECT * FROM `" + _table + "` LIMIT 1;" );

    const auto stmt = ODBCStmt( conn, query );

    SQLSMALLINT ncols = 0;

    auto ret = SQLNumResultCols( stmt.handle_, &ncols );

    ODBCError::check( ret, "SQLNumResultCols", stmt.handle_, SQL_HANDLE_STMT );

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
                ret, "SQLDescribeCol", stmt.handle_, SQL_HANDLE_STMT );

            coltypes.at( i ) = interpret_field_type( data_type );
        }

    return coltypes;
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

// ----------------------------------------------------------------------------
}  // namespace database

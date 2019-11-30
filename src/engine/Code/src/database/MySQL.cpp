
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::vector<std::string> MySQL::get_colnames( const std::string& _table ) const
{
    const std::string sql = "SELECT * FROM " + _table + " LIMIT 0;";

    const auto conn = make_connection();

    const auto result = exec( sql, conn );

    const auto num_cols = mysql_num_fields( result.get() );

    auto colnames = std::vector<std::string>( num_cols );

    for ( unsigned int i = 0; i < num_cols; ++i )
        {
            colnames[i] = mysql_fetch_field( result.get() )->name;
        }

    return colnames;
}

// ----------------------------------------------------------------------------

std::vector<csv::Datatype> MySQL::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const std::string sql = "SELECT * FROM " + _table + " LIMIT 0;";

    const auto conn = make_connection();

    const auto result = exec( sql, conn );

    const auto num_cols = mysql_num_fields( result.get() );

    auto coltypes = std::vector<csv::Datatype>( num_cols );

    for ( unsigned int i = 0; i < num_cols; ++i )
        {
            coltypes[i] =
                interpret_field_type( mysql_fetch_field( result.get() )->type );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

/*Poco::JSON::Object Postgres::get_content(
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

    auto iterator = std::make_shared<PostgresIterator>(
        make_connection(), colnames, time_formats_, _tname, "", begin, end );

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
}*/

// ----------------------------------------------------------------------------

csv::Datatype MySQL::interpret_field_type( const enum_field_types _type ) const
{
    // https://dev.mysql.com/doc/refman/5.7/en/c-api-prepared-statement-type-codes.html
    switch ( _type )
        {
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
                return csv::Datatype::double_precision;

            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_LONGLONG:
                return csv::Datatype::integer;

            default:
                return csv::Datatype::string;
        }
}

// ----------------------------------------------------------------------------

/*std::vector<std::string> Postgres::list_tables()
{
    auto iterator = std::make_shared<PostgresIterator>(
        make_connection(),
        std::vector<std::string>( {"table_name"} ),
        time_formats_,
        "information_schema.tables",
        "table_schema='public'" );

    auto tnames = std::vector<std::string>( 0 );

    while ( !iterator->end() )
        {
            tnames.push_back( iterator->get_string() );
        }

    return tnames;
}*/

// ----------------------------------------------------------------------------

std::string MySQL::make_load_data_query(
    const std::string& _table,
    const std::string& _fname,
    const std::vector<std::string>& _colnames,
    const csv::Reader* _reader ) const
{
    std::string query = "LOAD DATA LOCAL INFILE '" + _fname + "' INTO TABLE " +
                        _table + " FIELDS TERMINATED BY '" + _reader->sep() +
                        "' ENCLOSED BY '" + _reader->quotechar() +
                        "' LINES TERMINATED BY '\n' IGNORE 0 LINES (";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            query += "@y" + _colnames[i];

            if ( i == _colnames.size() - 1 )
                {
                    query += ") ";
                }
            else
                {
                    query += ", ";
                }
        }

    query += "SET ";

    for ( size_t i = 0; i < _colnames.size(); ++i )
        {
            query +=
                _colnames[i] + " = nullif( " + "@y" + _colnames[i] + ", '')";

            if ( i == _colnames.size() - 1 )
                {
                    query += ";";
                }
            else
                {
                    query += ", ";
                }
        }

    return query;
}

// ----------------------------------------------------------------------------

void MySQL::read(
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

    assert_true( colnames.size() == coltypes.size() );

    // ------------------------------------------------------------------------
    // Skip lines, if necessary.

    size_t line_count = 0;

    for ( size_t i = 0; i < _skip; ++i )
        {
            _reader->next_line();
            ++line_count;
        }

    //  ------------------------------------------------------------------------
    // Check headers, if necessary.

    if ( _header )
        {
            // check_colnames( colnames, _reader ); // TODO
            _reader->next_line();
            ++line_count;
        }

    // ----------------------------------------------------------------
    // Create a temporary file and insert the parsed CSV, line by line.

    const auto tempfile = Poco::TemporaryFile();

    std::ofstream ofs( tempfile.path(), std::ofstream::out );

    while ( !_reader->eof() )
        {
            const std::vector<std::string> line = _reader->next_line();

            ++line_count;

            if ( line.size() == 0 )
                {
                    continue;
                }
            else if ( line.size() != coltypes.size() )
                {
                    std::cout << "Corrupted line: " << line_count
                              << ". Expected " << colnames.size()
                              << " fields, saw " << line.size() << "."
                              << std::endl;

                    continue;
                }

            const std::string buffer = CSVBuffer::make_buffer(
                line, coltypes, _reader->sep(), _reader->quotechar() );

            ofs << buffer;
        }

    ofs.close();

    // ----------------------------------------------------------------
    // Read the temporary file into MySQL.

    const auto query =
        make_load_data_query( _table, tempfile.path(), colnames, _reader );

    execute( query );

    // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace database

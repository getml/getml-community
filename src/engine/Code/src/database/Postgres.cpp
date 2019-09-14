
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::vector<std::string> Postgres::get_colnames(
    const std::string& _table ) const
{
    const std::string sql = "SELECT * FROM \"" + _table + "\" LIMIT 0";

    const auto connection = make_connection();

    const auto result = exec( sql, connection.get() );

    const int num_cols = PQnfields( result.get() );

    auto colnames = std::vector<std::string>( num_cols );

    for ( int i = 0; i < num_cols; ++i )
        {
            colnames[i] = PQfname( result.get(), i );
        }

    return colnames;
}

// ----------------------------------------------------------------------------

std::vector<csv::Datatype> Postgres::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const std::string sql = "SELECT * FROM \"" + _table + "\" LIMIT 0";

    const auto connection = make_connection();

    const auto result = exec( sql, connection.get() );

    const int num_cols = PQnfields( result.get() );

    auto coltypes = std::vector<csv::Datatype>( num_cols );

    for ( int i = 0; i < num_cols; ++i )
        {
            const auto oid = PQftype( result.get(), i );

            coltypes[i] = interpret_oid( oid );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Postgres::get_content(
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
}

// ----------------------------------------------------------------------------

csv::Datatype Postgres::interpret_oid( Oid _oid ) const
{
    // ------------------------------------------------------------------------
    // Get the typname associated with the oid

    const std::string sql =
        "SELECT typname FROM pg_type WHERE oid=" + std::to_string( _oid ) + ";";

    auto connection = make_connection();

    const auto result = exec( sql, connection.get() );

    if ( PQntuples( result.get() ) == 0 )
        {
            throw std::runtime_error(
                "Type for oid " + std::to_string( _oid ) + " not known!" );
        }

    const std::string typname = PQgetvalue( result.get(), 0, 0 );

    // ------------------------------------------------------------------------
    // Check whether it might be double precision.

    auto typnames = typnames_double_precision();

    if ( std::find( typnames.begin(), typnames.end(), typname ) !=
         typnames.end() )
        {
            return csv::Datatype::double_precision;
        }

    // ------------------------------------------------------------------------
    // Check whether it might be an integer.

    typnames = typnames_int();

    if ( std::find( typnames.begin(), typnames.end(), typname ) !=
         typnames.end() )
        {
            return csv::Datatype::integer;
        }

    // ------------------------------------------------------------------------
    // Otherwise, interpret it as a string.

    return csv::Datatype::string;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<std::string> Postgres::list_tables()
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
}

// ----------------------------------------------------------------------------

std::string Postgres::make_buffer(
    const std::vector<std::string>& _line,
    const std::vector<csv::Datatype>& _coltypes,
    const char _sep,
    const char _quotechar )
{
    std::string buffer;

    assert_true( _line.size() == _coltypes.size() );

    for ( size_t i = 0; i < _line.size(); ++i )
        {
            buffer += parse_field( _line[i], _coltypes[i], _sep, _quotechar );

            if ( i < _line.size() - 1 )
                {
                    buffer += _sep;
                }
            else
                {
                    buffer += '\n';
                }
        }

    return buffer;
}

// ----------------------------------------------------------------------------

std::string Postgres::make_connection_string( const Poco::JSON::Object& _obj )
{
    const auto host = jsonutils::JSON::get_value<std::string>( _obj, "host_" );

    const auto hostaddr =
        jsonutils::JSON::get_value<std::string>( _obj, "hostaddr_" );

    const auto port = jsonutils::JSON::get_value<size_t>( _obj, "port_" );

    const auto dbname =
        jsonutils::JSON::get_value<std::string>( _obj, "dbname_" );

    const auto user = jsonutils::JSON::get_value<std::string>( _obj, "user_" );

    const auto password =
        jsonutils::JSON::get_value<std::string>( _obj, "password_" );

    auto connection_string = std::string( "host=" ) + host + " ";

    connection_string += std::string( "hostaddr=" ) + hostaddr + " ";

    connection_string += std::string( "port=" ) + std::to_string( port ) + " ";

    connection_string += std::string( "dbname=" ) + dbname + " ";

    connection_string += std::string( "user=" ) + user + " ";

    connection_string += std::string( "password=" ) + password;

    return connection_string;
}

// ----------------------------------------------------------------------------

std::string Postgres::parse_field(
    const std::string& _raw_field,
    const csv::Datatype _datatype,
    const char _sep,
    const char _quotechar ) const
{
    switch ( _datatype )
        {
            case csv::Datatype::double_precision:
                {
                    const auto [val, success] =
                        csv::Parser::to_double( _raw_field );

                    if ( success )
                        {
                            return std::to_string( val );
                        }
                    else
                        {
                            return "";
                        }
                }

                // ------------------------------------------------------------

            case csv::Datatype::integer:
                {
                    const auto [val, success] =
                        csv::Parser::to_int( _raw_field );

                    if ( success )
                        {
                            return std::to_string( val );
                        }
                    else
                        {
                            return "";
                        }
                }

                // ------------------------------------------------------------

            default:
                auto field =
                    csv::Parser::remove_quotechars( _raw_field, _quotechar );

                if ( field.find( _sep ) != std::string::npos )
                    {
                        field = _quotechar + field + _quotechar;
                    }

                return field;

                // ------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void Postgres::read(
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
            // check_colnames( colnames, _reader ); // ToDo
            _reader->next_line();
            ++line_count;
        }

    // ----------------------------------------------------------------
    // Insert line by line.

    const auto copy_statement = std::string( "COPY \"" ) + _table +
                                std::string( "\" FROM STDIN DELIMITER '" ) +
                                _reader->sep() +
                                std::string( "' CSV QUOTE '" ) +
                                _reader->quotechar() + std::string( "';" );

    auto conn = make_connection();

    auto res = PQexec( conn.get(), copy_statement.c_str() );

    if ( !res )
        {
            throw std::runtime_error( PQerrorMessage( conn.get() ) );
        }

    PQgetResult( conn.get() );

    try
        {
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

                    const std::string buffer = make_buffer(
                        line, coltypes, _reader->sep(), _reader->quotechar() );

                    const auto success = PQputCopyData(
                        conn.get(),
                        buffer.c_str(),
                        static_cast<int>( buffer.size() ) );

                    if ( success != 1 )
                        {
                            throw std::runtime_error(
                                "Write error in line " +
                                std::to_string( line_count ) + "." );
                        }
                }
        }
    catch ( std::exception& e )
        {
            PQputCopyEnd( conn.get(), e.what() );

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------------------------
    // End copying.

    if ( PQputCopyEnd( conn.get(), NULL ) == -1 )
        {
            throw std::runtime_error( PQerrorMessage( conn.get() ) );
        }

    PQgetResult( conn.get() );

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace database

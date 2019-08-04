
#include "database/database.hpp"

namespace database
{
// ----------------------------------------------------------------------------

std::vector<std::string> Postgres::get_colnames(
    const std::string& _table ) const
{
    const std::string sql = "SELECT * FROM \"" + _table + "\" LIMIT 0";

    auto connection = make_connection();

    auto work = pqxx::work( *connection );

    auto rows = work.exec( sql );

    auto colnames = std::vector<std::string>( 0 );

    for ( pqxx::row_size_type i = 0; i < rows.columns(); ++i )
        {
            colnames.push_back( rows.column_name( i ) );
        }

    return colnames;
}

// ----------------------------------------------------------------------------

std::vector<csv::Datatype> Postgres::get_coltypes(
    const std::string& _table, const std::vector<std::string>& _colnames ) const
{
    const std::string sql = "SELECT * FROM \"" + _table + "\" LIMIT 0";

    auto connection = make_connection();

    auto work = pqxx::work( *connection );

    auto rows = work.exec( sql );

    auto coltypes = std::vector<csv::Datatype>( 0 );

    for ( pqxx::row_size_type i = 0; i < rows.columns(); ++i )
        {
            const auto oid = rows.column_type( i );

            coltypes.push_back( interpret_oid( oid ) );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

csv::Datatype Postgres::interpret_oid( pqxx::oid _oid ) const
{
    // ------------------------------------------------------------------------
    // Get the typname associated with the oid

    const std::string sql =
        "SELECT typname FROM pg_type WHERE oid=" + std::to_string( _oid ) + ";";

    auto connection = make_connection();

    auto work = pqxx::work( *connection );

    const auto rows = work.exec( sql );

    if ( rows.size() == 0 )
        {
            throw std::runtime_error(
                "Type for oid " + std::to_string( _oid ) + " not known!" );
        }

    const std::string typname = rows[0][0].c_str();

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
    // Check whether it might be time stamp.

    typnames = typnames_timestamp();

    if ( std::find( typnames.begin(), typnames.end(), typname ) !=
         typnames.end() )
        {
            return csv::Datatype::time_stamp;
        }

    // ------------------------------------------------------------------------
    // Otherwise, interpret it as a string.

    return csv::Datatype::string;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::string Postgres::make_buffer(
    const std::vector<std::string>& _line,
    const std::vector<csv::Datatype>& _coltypes,
    const char _sep,
    const char _quotechar )
{
    std::string buffer;

    assert( _line.size() == _coltypes.size() );

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

            case csv::Datatype::time_stamp:
                {
                    assert( false && "ToDo" );

                    return "";
                }

                // ------------------------------------------------------------

            default:
                auto field = _raw_field;

                // std::remove moves all occurences of _quotechar to the very
                // left.
                // Erase then gets rid of them. This is called the
                // remove-erase-idiom.
                field.erase(
                    std::remove( field.begin(), field.end(), _quotechar ),
                    field.end() );

                if ( field.find( _sep ) != std::string::npos )
                    {
                        field = _quotechar + field + _quotechar;
                    }

                return field;

                // ------------------------------------------------------------
        }
}

// ----------------------------------------------------------------------------

void Postgres::read_csv(
    const std::string& _table, const bool _header, csv::Reader* _reader )
{
    // ------------------------------------------------------------------------
    // Get colnames and coltypes

    const std::vector<std::string> colnames = get_colnames( _table );

    const std::vector<csv::Datatype> coltypes =
        get_coltypes( _table, colnames );

    //  ------------------------------------------------------------------------
    // Check headers, if necessary.

    size_t line_count = 0;

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

    auto conn = make_raw_connection();

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

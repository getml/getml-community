
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
            // ToDo

            // if ()

            // colnames.push_back( rows.column_type( i ) );
        }

    return coltypes;
}

// ----------------------------------------------------------------------------

std::string Postgres::make_buffer(
    const std::vector<std::string>& _line, const char _sep, const char _quote )
{
    std::string buffer;

    for ( size_t i = 0; i < _line.size(); ++i )
        {
            auto field = _line[i];

            // std::remove moves all occurences of _quote to the very left.
            // Erase then gets rid of them. This is called the
            // remove-erase-idiom.
            field.erase(
                std::remove( field.begin(), field.end(), _quote ),
                field.end() );

            if ( field.find( _sep ) != std::string::npos )
                {
                    buffer += _quote;
                    buffer += field + _quote;
                }
            else
                {
                    buffer += field;
                }

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

    try
        {
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

                    const auto buffer = make_buffer(
                        line, _reader->sep(), _reader->quotechar() );

                    const auto success = PQputCopyData(
                        conn.get(),
                        buffer.c_str(),
                        static_cast<int>( buffer.size() ) );

                    if ( success == -1 )
                        {
                            throw std::runtime_error(
                                PQerrorMessage( conn.get() ) );
                        }
                }
        }
    catch ( std::exception& e )
        {
            PQputCopyEnd( conn.get(), e.what() );

            throw std::runtime_error( e.what() );
        }

    // ------------------------------------------------------------------------
    // Just for testing - this proves that we need to do explicit type checks.

    /*  std::string buffer = "gege,gege,gege,gege\n";

        const auto success = PQputCopyData(
            conn.get(), buffer.c_str(), static_cast<int>( buffer.size() ) );

        if ( success == -1 )
            {
                throw std::runtime_error( PQerrorMessage( conn.get() ) );
            }*/

    // ------------------------------------------------------------------------
    // End copying.

    if ( PQputCopyEnd( conn.get(), NULL ) == -1 )
        {
            throw std::runtime_error( PQerrorMessage( conn.get() ) );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace database

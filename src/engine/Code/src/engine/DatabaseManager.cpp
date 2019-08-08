#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

void DatabaseManager::drop_table(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    connector()->drop_table( _name );

    post_tables();

    communication::Sender::send_string( "Success!", _socket );
}

// ----------------------------------------------------------------------------

void DatabaseManager::execute( Poco::Net::StreamSocket* _socket )
{
    const auto query = communication::Receiver::recv_string( _socket );

    connector()->execute( query );

    post_tables();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_colnames(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto colnames = connector()->get_colnames( _name );

    std::string array = "[";

    for ( auto& col : colnames )
        {
            array += std::string( "\"" ) + col + "\",";
        }

    if ( array.size() > 1 )
        {
            array.back() = ']';
        }
    else
        {
            array += ']';
        }

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( array, _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<Int>( _cmd, "length_" );

    const auto start = JSON::get_value<Int>( _cmd, "start_" );

    auto obj = connector()->get_content( _name, draw, start, length );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_nrows(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const std::int32_t nrows = connector()->get_nrows( _name );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send( sizeof( std::int32_t ), &nrows, _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::list_tables( Poco::Net::StreamSocket* _socket )
{
    const auto array = post_tables();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( array, _socket );
}

// ----------------------------------------------------------------------------

std::string DatabaseManager::post_tables()
{
    const auto tables = connector()->list_tables();

    std::string array = "[";

    for ( auto& table : tables )
        {
            array += std::string( "\"" ) + table + "\",";
        }

    if ( array.size() > 1 )
        {
            array.back() = ']';
        }
    else
        {
            array += ']';
        }

    monitor_->send( "postdatabasetables", array );

    return array;
}

// ----------------------------------------------------------------------------

void DatabaseManager::read_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto header = JSON::get_value<bool>( _cmd, "header_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    // --------------------------------------------------------------------

    if ( quotechar.size() != 1 )
        {
            throw std::invalid_argument(
                "The quotechar must consist of exactly one character!" );
        }

    if ( sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator (sep) must consist of exactly one character!" );
        }

    // --------------------------------------------------------------------

    for ( const auto& fname : fnames )
        {
            auto reader = csv::Reader( fname, quotechar[0], sep[0] );

            connector()->read_csv( _name, header, &reader );

            logger().log( "Read '" + fname + "'." );
        }

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto header = JSON::get_value<bool>( _cmd, "header_" );

    const auto num_lines_sniffed =
        JSON::get_value<size_t>( _cmd, "num_lines_sniffed_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    // --------------------------------------------------------------------

    if ( quotechar.size() != 1 )
        {
            throw std::invalid_argument(
                "The quotechar must consist of exactly one character!" );
        }

    if ( sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator (sep) must consist of exactly one character!" );
        }

    // --------------------------------------------------------------------

    auto sniffer = csv::Sniffer(
        connector()->dialect(),
        fnames,
        header,
        num_lines_sniffed,
        quotechar[0],
        sep[0],
        _name,
        time_formats );

    const auto create_table_statement = sniffer.sniff();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( create_table_statement, _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

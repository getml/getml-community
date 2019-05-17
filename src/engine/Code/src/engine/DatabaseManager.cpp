#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void DatabaseManager::exec_query( Poco::Net::StreamSocket* _socket )
{
    const auto sql = communication::Receiver::recv_string( _socket );

    connector()->exec( sql );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::read_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto fname = JSON::get_value<std::string>( _cmd, "fname_" );

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

    auto reader = csv::Reader( fname, quotechar[0], sep[0] );

    // --------------------------------------------------------------------

    connector()->read_csv( _name, header, &reader );

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

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

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

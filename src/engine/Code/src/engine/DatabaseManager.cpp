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
}  // namespace handlers
}  // namespace engine

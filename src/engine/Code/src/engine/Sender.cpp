#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

void Sender::send_string(
    const std::string& _string, Poco::Net::StreamSocket* _socket )
{
    std::vector<char> buf;

    // ------------------------------------------------
    // Send size of string

    const ENGINE_INT str_size = static_cast<ENGINE_INT>( _string.size() );

    Sender::send<ENGINE_INT>( sizeof( ENGINE_INT ), &str_size, _socket );

    // ------------------------------------------------
    // Send string itself

    Sender::send<char>(
        static_cast<ENGINE_UNSIGNED_LONG>( _string.length() ),
        &( _string[0] ),
        _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

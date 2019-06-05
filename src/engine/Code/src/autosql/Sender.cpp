#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

/// Sends a string to the client
void Sender::send_string(
    Poco::Net::StreamSocket& _socket, std::string _string )
{
    std::vector<char> buf;

    // ------------------------------------------------
    // Send size of string

    AUTOSQL_INT str_size = static_cast<AUTOSQL_INT>( _string.size() );

    Sender::send<AUTOSQL_INT>( _socket, sizeof( AUTOSQL_INT ), &str_size );

    // ------------------------------------------------
    // Send string itself

    Sender::send<char>(
        _socket,
        static_cast<AUTOSQL_UNSIGNED_LONG>( _string.length() ),
        &( _string[0] ) );
}

// ------------------------------------------------------------------------

void Sender::send_warning_message(
    Poco::Net::StreamSocket& _socket, std::exception& e )
{
    std::string warning_message = "";
    warning_message.append( e.what() );

    if ( warning_message == "std::bad_alloc" )
        {
            warning_message.append(
                ". You seem to be using too much memory. Please refer to the "
                "AutoSQL documentation to understand why this is happening and "
                "what to do about it." );
        }

    auto now = std::chrono::system_clock::now();

    std::time_t current_time = std::chrono::system_clock::to_time_t( now );

    std::cout << std::ctime( &current_time ) << warning_message << std::endl
              << std::endl;

    // A broken pipe shouldn't happen - so in the debug version
    // we will still have this assert statement.
    assert( warning_message != "Broken pipe" );

    if ( warning_message != "Broken pipe" )
        {
            send_string( _socket, warning_message );
        }
}

// ------------------------------------------------------------------------
}
}

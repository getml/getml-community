#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

void Logger::log( const std::string& _msg ) const
{
    assert_true( monitor_ );

    const auto now = std::chrono::system_clock::now();

    const std::time_t current_time =
        std::chrono::system_clock::to_time_t( now );

    std::cout << std::ctime( &current_time ) << _msg << std::endl << std::endl;

    const auto cmd_str = monitor_->make_cmd( "log" );

    const auto socket = monitor_->connect( communication::Monitor::TIMEOUT_ON );

    Sender::send_string( cmd_str, socket.get() );

    Sender::send_string( std::ctime( &current_time ) + _msg, socket.get() );
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#ifndef ENGINE_COMMUNICATION_SOCKETLOGGER_HPP_
#define ENGINE_COMMUNICATION_SOCKETLOGGER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

class SocketLogger : public logging::AbstractLogger
{
    // --------------------------------------------------------

   public:
    SocketLogger(
        const std::shared_ptr<const monitoring::Logger>& _logger,
        Poco::Net::StreamSocket* _socket )
        : logger_( _logger ), socket_( _socket )
    {
        assert_true( logger_ );
    }

    ~SocketLogger() = default;

    // --------------------------------------------------------

    /// Logs current events.
    void log( const std::string& _msg ) const final
    {
        auto now = std::chrono::system_clock::now();

        std::time_t current_time = std::chrono::system_clock::to_time_t( now );

        const std::string timestring = std::ctime( &current_time );

        logger_->log( _msg );

        Sender::send_string( "log: " timestring + _msg, socket_ );
    }

    // ----------------------------------------------------

   private:
    /// The Monitor is supposed to monitor all of the logs as well.
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// The socket to which we want to send the logs.
    Poco::Net::StreamSocket* socket_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_SOCKETLOGGER_HPP_

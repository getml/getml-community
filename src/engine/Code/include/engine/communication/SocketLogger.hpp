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
        const bool _silent,
        Poco::Net::StreamSocket* _socket )
        : logger_( _logger ), silent_( _silent ), socket_( _socket )
    {
        assert_true( logger_ );
    }

    ~SocketLogger() = default;

    // --------------------------------------------------------

    /// Logs current events.
    void log( const std::string& _msg ) const final
    {
        if ( !silent_ )
            {
                logger_->log( _msg );
            }

        Sender::send_string( "log: " + _msg, socket_ );
    }

    // ----------------------------------------------------

   private:
    /// The Monitor is supposed to monitor all of the logs as well.
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// Whether we want the progress to appear in the engine and the monitor
    /// log.
    const bool silent_;

    /// The socket to which we want to send the logs.
    Poco::Net::StreamSocket* socket_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_SOCKETLOGGER_HPP_

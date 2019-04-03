#ifndef ENGINE_LOGGING_LOGGER_HPP_
#define ENGINE_LOGGING_LOGGER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace logging
{
// ------------------------------------------------------------------------

class Logger
{
    // --------------------------------------------------------

   public:
    Logger( /*const std::shared_ptr<const Monitor>& _monitor*/ )
    /*: monitor_( _monitor )*/ {}

    ~Logger() = default;

    // --------------------------------------------------------

    /// Logs current events.
    void log( const std::string& _msg ) const
    {
        auto now = std::chrono::system_clock::now();

        std::time_t current_time = std::chrono::system_clock::to_time_t( now );

        std::cout << std::ctime( &current_time ) << _msg << std::endl
                  << std::endl;

        // monitor_->send( "log", std::ctime( &current_time ) + _msg );
    }

    // ----------------------------------------------------

    // private:
    /// The Monitor is supposed to monitor all of the logs as well.
    // const std::shared_ptr<const Monitor> monitor_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace logging
}  // namespace engine

#endif  // ENGINE_LOGGING_LOGGER_HPP_

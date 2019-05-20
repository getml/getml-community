#ifndef ENGINE_MONITORING_LOGGER_HPP_
#define ENGINE_MONITORING_LOGGER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace monitoring
{
// ------------------------------------------------------------------------

class Logger : public logging::AbstractLogger
{
    // --------------------------------------------------------

   public:
    Logger( /*const std::shared_ptr<const Monitor>& _monitor*/ )
    /*: monitor_( _monitor )*/ {}

    ~Logger() = default;

    // --------------------------------------------------------

    /// Logs current events.
    void log( const std::string& _msg ) const final
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
}  // namespace monitoring
}  // namespace engine

#endif  // ENGINE_MONITORING_LOGGER_HPP_

#ifndef ENGINE_CONFIG_MONITOROPTIONS_
#define ENGINE_CONFIG_MONITOROPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the monitor

struct MonitorOptions
{
    // ------------------------------------------------------

    MonitorOptions( const Poco::JSON::Object& _json_obj )
        : http_port_( JSON::get_value<size_t>( _json_obj, "httpPort" ) ),
          https_port_( JSON::get_value<size_t>( _json_obj, "httpsPort" ) )
    {
    }

    MonitorOptions() : http_port_( 1709 ), https_port_( 1710 ) {}

    ~MonitorOptions() = default;

    // ------------------------------------------------------

    /// The HTTP port of the monitor, used for local connections.
    const size_t http_port_;

    /// The port of the monitor that also accepts
    /// remote connections. (The engine will never communicate
    /// with this port. It is only needed to print out the initial message).
    const size_t https_port_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_MONITOROPTIONS_

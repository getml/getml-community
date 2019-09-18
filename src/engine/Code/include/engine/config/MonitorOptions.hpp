#ifndef ENGINE_CONFIG_MONITOROPTIONS_
#define ENGINE_CONFIG_MONITOROPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the AutoSQL monitor

struct MonitorOptions
{
    // ------------------------------------------------------

    MonitorOptions( const Poco::JSON::Object& _json_obj )
        : port_( JSON::get_value<size_t>( _json_obj, "port" ) ),
          remote_port_( JSON::get_value<size_t>( _json_obj, "remotePort" ) ),
          tls_encryption_(
              Poco::File( "../cert.pem" ).exists() &&
              Poco::File( "../key.pem" ).exists() )
    {
    }

    MonitorOptions()
        : port_( 1709 ),
          remote_port_( 1710 ),
          tls_encryption_(
              Poco::File( "../cert.pem" ).exists() &&
              Poco::File( "../key.pem" ).exists() )
    {
    }

    ~MonitorOptions() = default;

    // ------------------------------------------------------

    /// The port of the AutoSQL monitor
    const size_t port_;

    /// The port of the AutoSQL monitor that also accepts
    /// remote connections. (The engine will never communicate
    /// with this port. It is only needed to print out the initial message).
    const size_t remote_port_;

    /// Whether you want to use TLS encryption to communicate
    /// with the monitor
    const bool tls_encryption_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_MONITOROPTIONS_

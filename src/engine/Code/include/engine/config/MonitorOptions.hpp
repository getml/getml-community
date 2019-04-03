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
        : license_server_url_(
              JSON::get_value<std::string>( _json_obj, "licenseServerURL" ) ),
          port_( JSON::get_value<size_t>( _json_obj, "port" ) ),
          tls_encryption_(
              Poco::File( "../cert.pem" ).exists() &&
              Poco::File( "../key.pem" ).exists() )
    {
    }

    MonitorOptions()
        : license_server_url_( "https://api.autosql.ai/alpha/license/" ),
          port_( 1709 ),
          tls_encryption_(
              Poco::File( "../cert.pem" ).exists() &&
              Poco::File( "../key.pem" ).exists() )
    {
    }

    ~MonitorOptions() = default;

    // ------------------------------------------------------

    /// The URL of the license server
    const std::string license_server_url_;

    /// The port of the AutoSQL engine
    const size_t port_;

    /// Whether you want to use TLS encryption to communicate
    /// with the monitor
    const bool tls_encryption_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_MONITOROPTIONS_

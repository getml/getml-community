#ifndef ENGINE_CONFIG_ENGINEOPTIONS_
#define ENGINE_CONFIG_ENGINEOPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the AutoSQL engine

struct EngineOptions
{
    // ------------------------------------------------------

    EngineOptions( const Poco::JSON::Object& _json_obj )
        : allow_remote_( JSON::get_value<bool>( _json_obj, "allowRemote" ) ),
          port_( JSON::get_value<size_t>( _json_obj, "port" ) )
    {
    }

    EngineOptions() : allow_remote_( false ), port_( 1708 ) {}

    ~EngineOptions() = default;

    // ------------------------------------------------------

    /// Whether to allow remote connections.
    const bool allow_remote_;

    /// The port of the AutoSQL engine
    const size_t port_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_ENGINEOPTIONS_

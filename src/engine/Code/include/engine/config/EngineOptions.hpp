#ifndef ENGINE_CONFIG_ENGINEOPTIONS_
#define ENGINE_CONFIG_ENGINEOPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the engine

struct EngineOptions
{
    // ------------------------------------------------------

    EngineOptions( const Poco::JSON::Object& _json_obj )
        : port_( JSON::get_value<size_t>( _json_obj, "port" ) )
    {
    }

    EngineOptions() : port_( 1708 ) {}

    ~EngineOptions() = default;

    // ------------------------------------------------------

    /// The port of the engine
    const size_t port_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_ENGINEOPTIONS_

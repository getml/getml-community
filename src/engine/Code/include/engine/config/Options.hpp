#ifndef ENGINE_CONFIG_OPTIONS_
#define ENGINE_CONFIG_OPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the AutoSQL engine

struct Options
{
    // ------------------------------------------------------

    Options( const Poco::JSON::Object& _json_obj )
        : all_projects_directory_(
              JSON::get_value<std::string>( _json_obj, "projectDirectory" ) ),
          engine_( EngineOptions( *JSON::get_object( _json_obj, "engine" ) ) ),
          monitor_(
              MonitorOptions( *JSON::get_object( _json_obj, "monitor" ) ) ),
          registration_email_(
              JSON::get_value<std::string>( _json_obj, "registrationEmail" ) )
    {
    }

    Options()
        : all_projects_directory_( "../projects/" ),
          engine_( EngineOptions() ),
          monitor_( MonitorOptions() ),
          registration_email_( "" )
    {
    }

    ~Options() = default;

    // ------------------------------------------------------

    /// Generates a new Options struct
    static Options make_options();

    // ------------------------------------------------------

    /// The directory in which all projects are stored (not identical with the
    /// current project directory).
    const std::string all_projects_directory_;

    /// Configurations for the engine.
    const EngineOptions engine_;

    /// Configurations for the monitor.
    const MonitorOptions monitor_;

    /// The email with which the user is registered
    const std::string registration_email_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_OPTIONS_

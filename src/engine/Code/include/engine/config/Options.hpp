#ifndef ENGINE_CONFIG_OPTIONS_
#define ENGINE_CONFIG_OPTIONS_

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------
// Configuration information for the engine

class Options
{
    // ------------------------------------------------------

   public:
    Options( const Poco::JSON::Object& _json_obj )
        : all_projects_directory_(
              JSON::get_value<std::string>( _json_obj, "projectDirectory" ) ),
          engine_( EngineOptions( *JSON::get_object( _json_obj, "engine" ) ) ),
          monitor_(
              MonitorOptions( *JSON::get_object( _json_obj, "monitor" ) ) )
    {
    }

    Options()
        : all_projects_directory_( "../projects/" ),
          engine_( EngineOptions() ),
          monitor_( MonitorOptions() )
    {
    }

    ~Options() = default;

    // ------------------------------------------------------

   public:
    /// Generates a new Options struct
    static Options make_options( int _argc, char* _argv[] );

    // ------------------------------------------------------

   private:
    /// Parses the command line flags
    void parse_flags( int _argc, char* _argv[] );

    /// Parses the options from the config.json.
    static Options parse_from_file();

    /// Parses a size_t from a command line argument
    bool parse_size_t(
        const std::string& _arg,
        const std::string& _flag,
        size_t* _target ) const;

    /// Parses string from a command line argument
    bool parse_string(
        const std::string& _arg,
        const std::string& _flag,
        std::string* _target ) const;

    /// Prints a warning message that the config.json could not be parsed.
    static void print_warning( const std::string& _msg );

    // ------------------------------------------------------

   public:
    /// Trivial accessor
    const std::string& all_projects_directory() const
    {
        return all_projects_directory_;
    }

    /// Trivial accessor
    const EngineOptions& engine() const { return engine_; }

    /// Trivial accessor
    const MonitorOptions& monitor() const { return monitor_; }

    // ------------------------------------------------------

   private:
    /// The directory in which all projects are stored (not identical with the
    /// current project directory).
    std::string all_projects_directory_;

    /// Configurations for the engine.
    EngineOptions engine_;

    /// Configurations for the monitor.
    MonitorOptions monitor_;

    // ------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

#endif  // ENGINE_CONFIG_OPTIONS_

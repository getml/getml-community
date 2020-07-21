#include "engine/config/config.hpp"

namespace engine
{
namespace config
{
// ----------------------------------------------------------------------------

Options Options::make_options( int _argc, char* _argv[] )
{
    auto options = parse_from_file();

    options.parse_flags( _argc, _argv );

    if ( options.all_projects_directory_.back() != '/' )
        {
            options.all_projects_directory_ += "/";
        }

    return options;
}

// ----------------------------------------------------------------------------

void Options::parse_flags( int _argc, char* argv[] )
{
    std::string allow_push_notifications;

    std::string launch_browser;

    std::size_t app_pid = 0;

    for ( int i = 1; i < _argc; ++i )
        {
            const auto arg = std::string( argv[i] );

            bool success =
                parse_size_t( arg, "engine-port", &( engine_.port_ ) );

            success =
                success ||
                parse_size_t( arg, "http-port", &( monitor_.http_port_ ) );

            success =
                success ||
                parse_size_t( arg, "https-port", &( monitor_.https_port_ ) );

            success = success ||
                      parse_size_t( arg, "tcp-port", &( monitor_.tcp_port_ ) );

            success = success ||
                      parse_string(
                          arg, "project-directory", &all_projects_directory_ );

            success =
                success || parse_string( arg, "proxy", &( monitor_.proxy_ ) );

            // This is to avoid a warning message.
            success = success || parse_string(
                                     arg,
                                     "allow-push-notifications",
                                     &allow_push_notifications );

            // This is to avoid a warning message.
            success = success ||
                      parse_string( arg, "launch-browser", &launch_browser );

            // This is to avoid a warning message.
            success = success || parse_size_t( arg, "app-pid", &app_pid );

            if ( !success )
                {
                    Options::print_warning(
                        "Could not parse command line flag '" + arg + "'!" );
                }
        }
}

// ----------------------------------------------------------------------------

Options Options::parse_from_file()
{
    const std::string fname = "../config.json";

    std::ifstream input( fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            print_warning( "File 'config.json' not found!" );

            return Options();
        }

    try
        {
            const auto ptr = Poco::JSON::Parser()
                                 .parse( json.str() )
                                 .extract<Poco::JSON::Object::Ptr>();

            if ( !ptr )
                {
                    print_warning(
                        "'config.json' does not contain a proper JSON "
                        "object!" );

                    return Options();
                }

            return Options( *ptr );
        }
    catch ( std::exception& e )
        {
            print_warning( e.what() );

            return Options();
        }
}

// ----------------------------------------------------------------------------

bool Options::parse_size_t(
    const std::string& _arg, const std::string& _flag, size_t* _target ) const
{
    if ( _arg.find( "-" + _flag + "=" ) != std::string::npos )
        {
            try
                {
                    const auto substr = _arg.substr( _flag.size() + 2 );
                    const auto val =
                        static_cast<size_t>( std::stoul( substr ) );
                    *_target = val;
                    return true;
                }
            catch ( std::exception& e )
                {
                    return false;
                }
        }

    return false;
}

// ----------------------------------------------------------------------------

bool Options::parse_string(
    const std::string& _arg,
    const std::string& _flag,
    std::string* _target ) const
{
    if ( _arg.find( "-" + _flag + "=" ) != std::string::npos )
        {
            try
                {
                    *_target = _arg.substr( _flag.size() + 2 );
                    return true;
                }
            catch ( std::exception& e )
                {
                    return false;
                }
        }

    return false;
}

// ----------------------------------------------------------------------------

void Options::print_warning( const std::string& _msg )
{
    auto now = std::chrono::system_clock::now();

    std::time_t current_time = std::chrono::system_clock::to_time_t( now );

    std::cout << std::ctime( &current_time ) << _msg << std::endl << std::endl;
}

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace engine

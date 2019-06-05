#include "config/config.hpp"

namespace autosql
{
namespace config
{
// ----------------------------------------------------------------------------

Options Options::make_options()
{
    try
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
                    throw std::invalid_argument(
                        "File '" + fname + "' not found!" );
                }

            const auto obj = Poco::JSON::Parser()
                                 .parse( json.str() )
                                 .extract<Poco::JSON::Object::Ptr>();

            return Options( *obj );
        }
    catch ( std::exception& e )
        {
            auto now = std::chrono::system_clock::now();

            std::time_t current_time =
                std::chrono::system_clock::to_time_t( now );

            std::cout << std::ctime( &current_time ) << e.what() << std::endl
                      << std::endl;

            std::cout << std::ctime( &current_time )
                      << "AutoSQL failed to load config.json. Using default "
                         "configurations instead."
                      << std::endl
                      << std::endl;

            return Options();
        }
}

// ----------------------------------------------------------------------------
}  // namespace config
}  // namespace autosql

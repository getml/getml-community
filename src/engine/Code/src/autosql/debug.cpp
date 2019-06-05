
#include "debug.hpp"

namespace autosql
{
// ------------------------------------------------------------------------

void __debug_message__( std::string _msg )
{
    auto now = std::chrono::system_clock::now();

    std::time_t current_time = std::chrono::system_clock::to_time_t( now );

    std::cout << std::ctime( &current_time ) << "DEBUG: " << _msg << std::endl
              << std::endl;
}

// ------------------------------------------------------------------------
}

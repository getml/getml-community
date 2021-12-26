#ifndef ENGINE_PROCESS_HPP_
#define ENGINE_PROCESS_HPP_

namespace engine
{
// ------------------------------------------------------------------------

struct Process
{
    /// Returns an object containing the current
    /// process ID.
    static std::string get_process_id()
    {
#if ( defined( _WIN32 ) || defined( _WIN64 ) )
        return std::to_string( GetCurrentProcessId() );
#else
        return std::to_string(::getpid() );
#endif
    }
};

// ------------------------------------------------------------------------
}  // namespace engine

#endif  // ENGINE_PROCESS_HPP_

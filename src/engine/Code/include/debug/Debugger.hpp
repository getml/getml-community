#ifndef RELBOOST_DEBUG_DEBUGGER_HPP_
#define RELBOOST_DEBUG_DEBUGGER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace debug
{
// ------------------------------------------------------------------------

struct Debugger
{
    /// Prints a debug message on the screen.
    static void log( const std::string& _msg );
};

// ------------------------------------------------------------------------
}
}

#endif  // RELBOOST_DEBUG_DEBUGGER_HPP_
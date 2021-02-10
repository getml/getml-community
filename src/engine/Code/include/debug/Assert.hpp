#ifndef DEBUG_ASSERT_HPP_
#define DEBUG_ASSERT_HPP_

// ----------------------------------------------------------------------------

namespace debug
{
// ------------------------------------------------------------------------

struct Assert
{
    /// Throws an exception providing the file and the line.
    static void throw_exception(
        const char *_msg, const char *_file, const int _line )
    {
        const auto stack_trace = StackTrace::make();

        const auto msg =
            std::string( "Assertion failed: " ) + _msg + " at " + _file +
            ", line " + std::to_string( _line ) + ".\n\n" + stack_trace +
            "Please help us improve our software by reporting this "
            "incident.";

        throw_exception( msg );
    }

    /// Throws an exception based on the user-defined message.
    static void throw_exception( const std::string &_msg )
    {
        throw std::runtime_error( _msg );
    }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_ASSERT_HPP_

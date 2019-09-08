#ifndef DEBUG_ASSERT_HPP_
#define DEBUG_ASSERT_HPP_

// ----------------------------------------------------------------------------

namespace debug
{
// ------------------------------------------------------------------------

struct Assert
{
    /// Throws an exception.
    static void throw_exception(
        const char *_msg, const char *_file, const int _line )
    {
        throw std::runtime_error(
            std::string( "Assertion failed: " ) + _msg + " at " + _file +
            ", line " + std::to_string( _line ) + ". We apologize. This our " +
            "fault, not yours. Please understand that this " +
            "is still a beta release. Also, please help us improve " +
            "our software by reporting this incident." );
    }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_ASSERT_HPP_

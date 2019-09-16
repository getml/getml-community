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
            ", line " + std::to_string( _line ) + ". Please help us improve " +
            "our software by reporting this incident." );
    }
};

// ------------------------------------------------------------------------
}  // namespace debug

#endif  // DEBUG_ASSERT_HPP_

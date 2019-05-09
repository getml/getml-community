#ifndef LOGGING_ABSTRACTLOGGER_HPP_
#define LOGGING_ABSTRACTLOGGER_HPP_

// ----------------------------------------------------------------------------

namespace logging
{
// ------------------------------------------------------------------------

class AbstractLogger
{
    // --------------------------------------------------------

   public:
    AbstractLogger() {}

    ~AbstractLogger() = default;

    // --------------------------------------------------------

    /// Logs current events.
    virtual void log( const std::string& _msg ) const = 0;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace logging

#endif  // LOGGING_ABSTRACTLOGGER_HPP_

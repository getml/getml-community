#ifndef LOGGING_PROGRESSLOGGER_HPP_
#define LOGGING_PROGRESSLOGGER_HPP_

// ----------------------------------------------------------------------------

namespace logging
{
// ------------------------------------------------------------------------

class ProgressLogger
{
    // --------------------------------------------------------

   public:
    ProgressLogger(
        const std::string& _msg,
        const std::shared_ptr<const AbstractLogger>& _logger,
        const size_t _total )
        : current_value_( 0 ), logger_( _logger ), total_( _total )
    {
        if ( logger_ && total_ > 0 )
            {
                logger_->log( _msg );
            }
    }

    ~ProgressLogger() = default;

    // --------------------------------------------------------

    /// Increments the progress.
    void increment( const size_t _by = 1 )
    {
        if ( _by == 0 )
            {
                return;
            }

        current_value_ += _by;

        assert_true( current_value_ <= total_ );

        if ( logger_ && total_ > 0 )
            {
                const auto progress = ( current_value_ * 100 ) / total_;
                logger_->log(
                    "Progress: " + std::to_string( progress ) + "%." );
            }
    }

    // ----------------------------------------------------

   private:
    /// The current iteration.
    size_t current_value_;

    /// Pointer to the underlying logger.
    const std::shared_ptr<const AbstractLogger> logger_;

    /// The total number of expected iterations.
    const size_t total_;

    // ----------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace logging

#endif  // LOGGING_PROGRESSLOGGER_HPP_

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
        const std::shared_ptr<const AbstractLogger>& _logger,
        const size_t _total )
        : current_value_( 0 ), logger_( _logger ), total_( _total )
    {
        assert_true( total_ > 0 );
    }

    ~ProgressLogger() = default;

    // --------------------------------------------------------

    /// Increments the progress.
    void increment()
    {
        ++current_value_;

        assert_true( current_value_ <= total_ );

        if ( logger_ )
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

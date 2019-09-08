#ifndef ENGINE_LICENSECHECKER_HPP_
#define ENGINE_LICENSECHECKER_HPP_

namespace engine
{
namespace licensing
{
// ------------------------------------------------------------------------

class LicenseChecker
{
    // ------------------------------------------------------------------------

   public:
    LicenseChecker(
        const std::shared_ptr<const monitoring::Logger> _logger,
        const std::shared_ptr<const monitoring::Monitor> _monitor,
        const config::Options& _options )
        : logger_( _logger ),
          monitor_( _monitor ),
          options_( _options ),
          read_write_lock_( std::make_shared<multithreading::ReadWriteLock>() ),
          token_( new Token() )
    {
    }

    ~LicenseChecker(){};

    // ------------------------------------------------------------------------

   public:
    /// Makes sure that the size of the raw data used does not exceed the
    /// memory limit.
    void check_mem_size(
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const ULong _new_size = 0 ) const;

    /// Makes sure that this is an enterprise user.
    void check_enterprise() const;

    /// Receives a token from the license server
    void receive_token( const std::string& _caller_id );

    // ------------------------------------------------------------------------

   public:
    /// Whether there is a valid token.
    bool has_active_token() const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        return token_ && token_->currently_active();
    }

    /// Checks whether is an enterprise version.
    bool is_enterprise() const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        return token_ && ( token_->function_set_id_ == "enterprise" );
    }

    /// Trivial accessor
    Token token() const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        assert_true( token_ );
        return *token_;
    }

    // ------------------------------------------------------------------------

   private:
    /// Calculates the memory size of the data frames.
    ULong calc_mem_size( const std::map<std::string, containers::DataFrame>&
                             _data_frames ) const;

    // ------------------------------------------------------------------------

   private:
    /// Encrypts a message using a one-way encryption algorithm.
    std::string encrypt( const std::string& _msg );

    /// Sends a POST request to the license server
    std::pair<std::string, bool> send( const std::string& _request );

    // ------------------------------------------------------------------------

   private:
    /// For logging the licensing process.
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// The user management and licensing process is handled by the monitor.
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// Contains information on the port of the license checker process
    const config::Options options_;

    /// For protecting the token.
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    /// The token containing information on the memory limit
    std::unique_ptr<const Token> token_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace licensing
}  // namespace engine

#endif  // ENGINE_LICENSECHECKER_HPP_

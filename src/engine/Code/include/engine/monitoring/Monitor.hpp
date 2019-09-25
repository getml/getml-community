#ifndef ENGINE_MONITORING_MONITOR_HPP_
#define ENGINE_MONITORING_MONITOR_HPP_

namespace engine
{
namespace monitoring
{
// ----------------------------------------------------------------------------

class Monitor
{
    // ------------------------------------------------------------------------

   public:
    Monitor( const engine::config::Options& _options ) : options_( _options ) {}

    ~Monitor() {}

    // ------------------------------------------------------------------------

    /// For logging errors
    void log( const std::string& _msg ) const
    {
        auto now = std::chrono::system_clock::now();

        std::time_t current_time = std::chrono::system_clock::to_time_t( now );

        std::cout << std::ctime( &current_time ) << _msg << std::endl
                  << std::endl;
    }

    /// Sends a message (consisting of _type and _obj) to the Multirel monitor
    std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string> send(
        const std::string& _type, const Poco::JSON::Object& _obj ) const
    {
        return send( _type, JSON::stringify( _obj ) );
    }

    // ------------------------------------------------------------------------

    /// Makes sure that the monitor has started.
    bool get_start_message() const;

    /// Sends a message (consisting of _type and _json) to the Multirel monitor
    std::pair<Poco::Net::HTTPResponse::HTTPStatus, std::string> send(
        const std::string& _type, const std::string& _json ) const;

    /// Sends a GET request to shut down the Multirel Monitor.
    bool shutdown() const;

    // -----------------------------------------------------------------------

   private:
    /// Sends a request and receives a reponse.
    void send_and_receive(
        const std::string& _json,
        Poco::Net::HTTPRequest* _req,
        Poco::Net::HTTPResponse* _res,
        std::string* _response_content ) const;

    // -----------------------------------------------------------------------

   private:
    /// Contains information on the port of the monitor process
    const engine::config::Options options_;

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace monitoring
}  // namespace engine

#endif  // ENGINE_MONITORING_MONITOR_HPP_

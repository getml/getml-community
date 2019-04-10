#ifndef ENGINE_SRV_REQUESTHANDLER_HPP_
#define ENGINE_SRV_REQUESTHANDLER_HPP_

namespace engine
{
namespace srv
{
// -----------------------------------------------------------------

/// A RequestHandler handles all request in deploy mode.
class RequestHandler : public Poco::Net::TCPServerConnection
{
    // -------------------------------------------------------------

   public:
    RequestHandler(
        const Poco::Net::StreamSocket& _socket,
        const std::shared_ptr<handlers::DataFrameManager>& _data_frame_manager,
        const std::shared_ptr<const engine::logging::Logger>& _logger,
        const std::shared_ptr<handlers::ModelManager>& _relboost_model_manager,
        // const std::shared_ptr<const engine::logging::Monitor>& _monitor,
        const config::Options& _options,
        const std::shared_ptr<handlers::ProjectManager>& _project_manager,
        const std::shared_ptr<std::atomic<bool>>& _shutdown )
        : Poco::Net::TCPServerConnection( _socket ),
          data_frame_manager_( _data_frame_manager ),
          logger_( _logger ),
          relboost_model_manager_( _relboost_model_manager ),
          // monitor_( _monitor ),
          options_( _options ),
          project_manager_( _project_manager ),
          shutdown_( _shutdown )
    {
    }

    ~RequestHandler() = default;

    /// Required by Poco::Net::TCPServerConnection. Does the actual handling.
    void run();

    // -------------------------------------------------------------

   private:
    /// Trivial accessor
    handlers::DataFrameManager& data_frame_manager()
    {
        return *data_frame_manager_;
    }

    /// Trivial accessor
    const engine::logging::Logger& logger() { return *logger_; }

    /// Trivial accessor
    handlers::ModelManager& relboost_model_manager()
    {
        return *relboost_model_manager_;
    }

    /// Trivial accessor
    // const engine::logging::Monitor& monitor() { return *monitor_; }

    /// Trivial accessor
    handlers::ProjectManager& project_manager() { return *project_manager_; }

    // -------------------------------------------------------------

   private:
    /// Handles requests related to the data frames.
    const std::shared_ptr<handlers::DataFrameManager> data_frame_manager_;

    /// Logs commands.
    const std::shared_ptr<const logging::Logger> logger_;

    /// Handles requests related to the models such as fit or transform.
    const std::shared_ptr<handlers::ModelManager> relboost_model_manager_;

    /// Handles the communication with the monitor
    // const std::shared_ptr<const logging::Monitor> monitor_;

    /// Contains information on the port of the monitor process
    const config::Options options_;

    /// Handles requests related to the project as a whole, such as save or
    /// load.
    const std::shared_ptr<handlers::ProjectManager> project_manager_;

    /// Signals to the main process that we want to shut down.
    const std::shared_ptr<std::atomic<bool>>& shutdown_;

    // -------------------------------------------------------------
};

// -----------------------------------------------------------------
}  // namespace srv
}  // namespace engine

#endif  // ENGINE_SRV_REQUESTHANDLER_HPP_
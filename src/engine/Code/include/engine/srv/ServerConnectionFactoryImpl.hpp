#ifndef ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_
#define ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_

namespace engine
{
namespace srv
{
// -----------------------------------------------------------------

class ServerConnectionFactoryImpl : public Poco::Net::TCPServerConnectionFactory
{
    // -------------------------------------------------------------

   public:
    ServerConnectionFactoryImpl(
        const std::shared_ptr<handlers::MultirelModelManager>&
            _multirel_model_manager,
        const std::shared_ptr<handlers::DatabaseManager>& _database_manager,
        const std::shared_ptr<handlers::DataFrameManager>& _data_frame_manager,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<handlers::RelboostModelManager>&
            _relboost_model_manager,
        const config::Options& _options,
        const std::shared_ptr<handlers::PipelineManager>& _pipeline_manager,
        const std::shared_ptr<handlers::ProjectManager>& _project_manager,
        const std::shared_ptr<std::atomic<bool>>& _shutdown )
        : multirel_model_manager_( _multirel_model_manager ),
          database_manager_( _database_manager ),
          data_frame_manager_( _data_frame_manager ),
          logger_( _logger ),
          relboost_model_manager_( _relboost_model_manager ),
          options_( _options ),
          pipeline_manager_( _pipeline_manager ),
          project_manager_( _project_manager ),
          shutdown_( _shutdown )
    {
    }

    /// Required by Poco::Net::TCPServerConnectionFactory. Does the actual
    /// handling.
    Poco::Net::TCPServerConnection* createConnection(
        const Poco::Net::StreamSocket& _socket )
    {
        return new RequestHandler(
            _socket,
            multirel_model_manager_,
            database_manager_,
            data_frame_manager_,
            logger_,
            pipeline_manager_,
            relboost_model_manager_,
            options_,
            project_manager_,
            shutdown_ );
    }

    // -------------------------------------------------------------

   private:
    /// Handles requests related to the Multirel models such as fit or
    /// transform.
    const std::shared_ptr<handlers::MultirelModelManager>
        multirel_model_manager_;

    /// Handles requests related to the database.
    const std::shared_ptr<handlers::DatabaseManager> database_manager_;

    /// Handles requests related to the data frames.
    const std::shared_ptr<handlers::DataFrameManager> data_frame_manager_;

    /// Logs commands.
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// Handles requests related to the relboost models such as fit or
    /// transform.
    const std::shared_ptr<handlers::RelboostModelManager>
        relboost_model_manager_;

    /// Handles the communication with the monitor
    // const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// Contains information on the port of the monitor process
    const config::Options options_;

    /// Handles requests related to pipelines.
    const std::shared_ptr<handlers::PipelineManager> pipeline_manager_;

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

#endif  // ENGINE_SRV_SERVERCONNECTIONFACTORYIMPL_HPP_

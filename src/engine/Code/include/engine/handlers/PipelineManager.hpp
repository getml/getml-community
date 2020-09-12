#ifndef ENGINE_HANDLERS_PIPELINEMANAGER_HPP_
#define ENGINE_HANDLERS_PIPELINEMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class PipelineManager
{
    // ------------------------------------------------------------------------

   public:
    typedef std::map<std::string, pipelines::Pipeline> PipelineMapType;

    // ------------------------------------------------------------------------

   public:
    PipelineManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<DatabaseManager>& _database_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<dependency::FETracker>& _fe_tracker,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,
        const std::shared_ptr<const communication::Logger>& _logger,
        const std::shared_ptr<const communication::Monitor>& _monitor,
        const std::shared_ptr<PipelineMapType>& _pipelines,
        const std::shared_ptr<dependency::PredTracker>& _pred_tracker,
        const std::shared_ptr<std::mutex>& _project_mtx,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          database_manager_( _database_manager ),
          data_frames_( _data_frames ),
          fe_tracker_( _fe_tracker ),
          join_keys_encoding_( _join_keys_encoding ),
          license_checker_( _license_checker ),
          logger_( _logger ),
          monitor_( _monitor ),
          pipelines_( _pipelines ),
          pred_tracker_( _pred_tracker ),
          project_mtx_( _project_mtx ),
          read_write_lock_( _read_write_lock )
    {
    }

    ~PipelineManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Checks the validity of the data model.
    void check(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the column importances of a pipeline.
    void column_importances(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Determines whether the pipeline should
    /// allow HTTP requests.
    void deploy(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the feature correlations of a pipeline.
    void feature_correlations(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the feature importances of a pipeline.
    void feature_importances(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Fits a pipeline
    void fit(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends a command to the monitor to launch a hyperparameter optimization.
    void launch_hyperopt(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Writes a JSON representation of the lift curve into the socket.
    void lift_curve(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Writes a JSON representation of the precision-recall curve into the
    /// socket.
    void precision_recall_curve(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Refreshes a pipeline in the target language
    void refresh( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Writes a JSON representation of the ROC curve into the socket.
    void roc_curve(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Writes a pipeline's targets into the socket.
    void targets( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Transform a pipeline to a JSON string
    void to_json( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Extracts the SQL code
    void to_sql( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Generate features
    void transform(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Retrieves an Poco::JSON::Array::Ptr from a scores object.
    Poco::JSON::Array::Ptr get_array(
        const Poco::JSON::Object& _scores,
        const std::string& _name,
        const unsigned int _target_num ) const;

    /// Posts a pipeline to the monitor.
    void post_pipeline( const Poco::JSON::Object& _obj );

    /// Receives data from the client. This data will not be stored permanently,
    /// but locally. Once the training/transformation process is complete, it
    /// will be deleted.
    Poco::JSON::Object receive_data(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
            _data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Under some circumstances, we might want to send data to the client, such
    /// as targets from the population or the results of a transform call.
    void send_data(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
            _local_data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Scores a pipeline
    void score(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        const std::map<std::string, containers::DataFrame>& _data_frames,
        const containers::Features& _yhat,
        pipelines::Pipeline* _pipeline,
        Poco::Net::StreamSocket* _socket );

    /// Writes a set of features to the data base.
    void to_db(
        const pipelines::Pipeline& _pipeline,
        const Poco::JSON::Object& _cmd,
        const containers::Features& _numerical_features,
        const containers::CategoricalFeatures& _categorical_features,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
            _local_data_frames,
        Poco::Net::StreamSocket* _socket );

    /// Writes a set of features to a DataFrame.
    containers::DataFrame to_df(
        const pipelines::Pipeline& _pipeline,
        const Poco::JSON::Object& _cmd,
        const containers::Features& _numerical_features,
        const containers::CategoricalFeatures& _categorical_features,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
            _local_data_frames,
        Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    const containers::Encoding& categories() const
    {
        assert_true( categories_ );
        return *categories_;
    }

    /// Trivial accessor
    std::shared_ptr<database::Connector> connector( const std::string& _name )
    {
        assert_true( database_manager_ );
        return database_manager_->connector( _name );
    }

    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        assert_true( data_frames_ );
        return *data_frames_;
    }

    /// Returns a deep copy of a pipeline.
    pipelines::Pipeline get_pipeline( const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        const auto& p = utils::Getter::get( _name, &pipelines() );
        return p;
    }

    /// Trivial accessor
    const licensing::LicenseChecker& license_checker() const
    {
        assert_true( license_checker_ );
        return *license_checker_;
    }

    /// Trivial (private) accessor
    const communication::Logger& logger()
    {
        assert_true( logger_ );
        return *logger_;
    }

    /// Trivial (private) accessor
    const communication::Monitor& monitor()
    {
        assert_true( monitor_ );
        return *monitor_;
    }

    /// Trivial (private) accessor
    PipelineMapType& pipelines()
    {
        assert_true( pipelines_ );
        return *pipelines_;
    }

    /// Trivial (private) accessor
    const PipelineMapType& pipelines() const
    {
        assert_true( pipelines_ );
        multithreading::ReadLock read_lock( read_write_lock_ );
        return *pipelines_;
    }

    /// Trivial (private) accessor
    std::mutex& project_mtx()
    {
        assert_true( project_mtx_ );
        return *project_mtx_;
    }

    /// Sets a pipeline.
    void set_pipeline(
        const std::string& _name, const pipelines::Pipeline& _pipeline )
    {
        multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

        auto it = pipelines().find( _name );

        if ( it == pipelines().end() )
            {
                throw std::runtime_error(
                    "Pipeline '" + _name + "' does not exist!" );
            }

        weak_write_lock.upgrade();

        it->second = _pipeline;
    }

    // ------------------------------------------------------------------------

   private:
    /// Maps integers to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// Connector to the underlying database.
    const std::shared_ptr<DatabaseManager> database_manager_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Keeps track of all feature learners.
    const std::shared_ptr<dependency::FETracker> fe_tracker_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the number of cores and memory usage
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

    /// For logging
    const std::shared_ptr<const communication::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const communication::Monitor> monitor_;

    /// The pipelines currently held in memory
    const std::shared_ptr<PipelineMapType> pipelines_;

    /// Keeps track of all predictors.
    const std::shared_ptr<dependency::PredTracker> pred_tracker_;

    /// It is sometimes necessary to prevent us from changing the project.
    const std::shared_ptr<std::mutex> project_mtx_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PIPELINEMANAGER_HPP_

#ifndef ENGINE_HANDLERS_PROJECTMANAGER_HPP_
#define ENGINE_HANDLERS_PROJECTMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class ProjectManager
{
    // ------------------------------------------------------------------------

   public:
    typedef PipelineManager::PipelineMapType PipelineMapType;

    // ------------------------------------------------------------------------

   public:
    ProjectManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<DataFrameManager>& _data_frame_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<engine::dependency::DataFrameTracker>&
            _data_frame_tracker,
        const std::shared_ptr<dependency::FETracker>& _fe_tracker,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<std::map<std::string, hyperparam::Hyperopt>>&
            _hyperopts,
        const std::shared_ptr<licensing::LicenseChecker>& _license_checker,
        const std::shared_ptr<const communication::Logger>& _logger,
        const std::shared_ptr<const communication::Monitor>& _monitor,
        const config::Options& _options,
        const std::shared_ptr<PipelineMapType>& _pipelines,
        const std::shared_ptr<dependency::PredTracker>& _pred_tracker,
        const std::shared_ptr<dependency::PreprocessorTracker>&
            _preprocessor_tracker,
        const std::string& _project,
        const std::shared_ptr<multithreading::ReadWriteLock>& _project_lock,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          data_frame_manager_( _data_frame_manager ),
          data_frames_( _data_frames ),
          data_frame_tracker_( _data_frame_tracker ),
          fe_tracker_( _fe_tracker ),
          join_keys_encoding_( _join_keys_encoding ),
          hyperopts_( _hyperopts ),
          license_checker_( _license_checker ),
          logger_( _logger ),
          monitor_( _monitor ),
          options_( _options ),
          pipelines_( _pipelines ),
          pred_tracker_( _pred_tracker ),
          preprocessor_tracker_( _preprocessor_tracker ),
          project_( _project ),
          project_lock_( _project_lock ),
          read_write_lock_( _read_write_lock )
    {
        set_project( _project );
    }

    ~ProjectManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Adds a new data frame
    void add_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from one or several CSV files.
    void add_data_frame_from_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Creates a new data frame from one or several CSV files located in an S3
    /// bucket.
    void add_data_frame_from_s3(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new data frame taken from the database.
    void add_data_frame_from_db(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new data frame taken parsed from a JSON.
    void add_data_frame_from_json(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new data frame generated from a query.
    void add_data_frame_from_query(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new data frame generated from a view.
    void add_data_frame_from_view(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new hyperparameter optimization.
    void add_hyperopt(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Adds a new Pipeline to the project.
    void add_pipeline(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Duplicates a pipeline.
    void copy_pipeline(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a data frame
    void delete_data_frame(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a pipeline
    void delete_pipeline(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a project
    void delete_project(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Returns a list of all data_frames currently held in memory and held
    /// in the project directory.
    void list_data_frames( Poco::Net::StreamSocket* _socket ) const;

    /// Returns a list of all hyperopts currently held in memory.
    void list_hyperopts( Poco::Net::StreamSocket* _socket ) const;

    /// Returns a list of all pipelines currently held in memory.
    void list_pipelines( Poco::Net::StreamSocket* _socket ) const;

    /// Returns a list of all projects.
    void list_projects( Poco::Net::StreamSocket* _socket ) const;

    /// Loads a data container.
    void load_data_container(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a data frame
    void load_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a hyperopt
    void load_hyperopt(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a pipeline
    void load_pipeline(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Get the name of the current project.
    void project_name( Poco::Net::StreamSocket* _socket ) const;

    /// Updates the encodings in the client
    void refresh( Poco::Net::StreamSocket* _socket ) const;

    /// Saves a data container to disk.
    void save_data_container(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Saves a data frame
    void save_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Saves a hyperparameter optimization object
    void save_hyperopt(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Saves a pipeline to disc.
    void save_pipeline(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Sets the current project
    void set_project( const std::string& _name );

    /// Get the path of the directory for tempfiles.
    void temp_dir( Poco::Net::StreamSocket* _socket ) const;

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    std::string project_directory() const
    {
        return options_.project_directory();
    }

    // ------------------------------------------------------------------------

   private:
    /// Deletes all pipelines and data frames (from memory only) and clears all
    /// encodings.
    void clear();

    /// Loads a JSON object from a file.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    /// Posts an object to the monitor.
    void post( const std::string& _what, const Poco::JSON::Object& _obj ) const;

    /// Removes an object from the monitor.
    void remove( const std::string& _what, const std::string& _name ) const;

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    containers::Encoding& categories() { return *categories_; }

    /// Trivial accessor
    const containers::Encoding& categories() const { return *categories_; }

    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        return *data_frames_;
    }

    /// Trivial (const) accessor
    const std::map<std::string, containers::DataFrame>& data_frames() const
    {
        assert_true( data_frames_ );
        return *data_frames_;
    }

    /// Trivial accessor
    dependency::DataFrameTracker& data_frame_tracker()
    {
        assert_true( data_frame_tracker_ );
        return *data_frame_tracker_;
    }

    /// Trivial accessor
    DataFrameManager& data_frame_manager()
    {
        assert_true( data_frame_manager_ );
        return *data_frame_manager_;
    }

    /// Trivial (const) accessor
    const DataFrameManager& data_frame_manager() const
    {
        assert_true( data_frame_manager_ );
        return *data_frame_manager_;
    }

    /// Trivial accessor
    dependency::FETracker& fe_tracker()
    {
        assert_true( fe_tracker_ );
        return *fe_tracker_;
    }

    /// Returns a deep copy of a pipeline.
    pipelines::Pipeline get_pipeline( const std::string& _name ) const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto p = utils::Getter::get( _name, pipelines() );
        return p;
    }

    /// Trivial accessor
    containers::Encoding& join_keys_encoding() { return *join_keys_encoding_; }

    /// Trivial accessor
    const containers::Encoding& join_keys_encoding() const
    {
        return *join_keys_encoding_;
    }

    /// Trivial (private) accessor
    std::map<std::string, hyperparam::Hyperopt>& hyperopts()
    {
        assert_true( hyperopts_ );
        return *hyperopts_;
    }

    /// Trivial (private) accessor
    const std::map<std::string, hyperparam::Hyperopt>& hyperopts() const
    {
        assert_true( hyperopts_ );
        return *hyperopts_;
    }

    /// Trivial accessor
    engine::licensing::LicenseChecker& license_checker()
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
    const communication::Monitor& monitor() const
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

    /// Trivial (const private) accessor
    const PipelineMapType& pipelines() const
    {
        assert_true( pipelines_ );
        return *pipelines_;
    }

    /// Trivial accessor
    dependency::PredTracker& pred_tracker()
    {
        assert_true( pred_tracker_ );
        return *pred_tracker_;
    }

    /// Trivial (private) setter.
    void set_hyperopt(
        const std::string& _name, const hyperparam::Hyperopt& _hyperopt )
    {
        multithreading::WriteLock write_lock( read_write_lock_ );
        hyperopts().insert_or_assign( _name, _hyperopt );
    }

    /// Trivial (private) setter.
    void set_pipeline(
        const std::string& _name, const pipelines::Pipeline& _pipeline )
    {
        multithreading::WriteLock write_lock( read_write_lock_ );
        pipelines().insert_or_assign( _name, _pipeline );
    }

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// We need some methods from the data frame manager.
    const std::shared_ptr<DataFrameManager> data_frame_manager_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Keeps track of all data frames, so we don't have to
    /// reconstruct the features all of the time.
    const std::shared_ptr<dependency::DataFrameTracker> data_frame_tracker_;

    /// Keeps track of all feature learners.
    const std::shared_ptr<dependency::FETracker> fe_tracker_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// The Hyperopts currently held in memory
    const std::shared_ptr<std::map<std::string, hyperparam::Hyperopt>>
        hyperopts_;

    /// For checking the license and memory usage
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

    /// For logging
    const std::shared_ptr<const communication::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const communication::Monitor> monitor_;

    /// Settings for the engine and the monitor
    const config::Options options_;

    /// The pipelines currently held in memory
    const std::shared_ptr<PipelineMapType> pipelines_;

    /// Keeps track of all predictors.
    const std::shared_ptr<dependency::PredTracker> pred_tracker_;

    /// Keeps track of all preprocessors.
    const std::shared_ptr<dependency::PreprocessorTracker>
        preprocessor_tracker_;

    /// The name of the current project
    const std::string project_;

    /// It is sometimes necessary to prevent us from changing the project.
    const std::shared_ptr<multithreading::ReadWriteLock> project_lock_;

    /// For synchronization
    const std::shared_ptr<multithreading::ReadWriteLock>& read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGER_HPP_

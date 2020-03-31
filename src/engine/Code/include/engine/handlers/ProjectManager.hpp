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
        const std::shared_ptr<dependency::FETracker>& _fe_tracker,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<licensing::LicenseChecker>& _license_checker,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const config::Options& _options,
        const std::shared_ptr<PipelineMapType>& _pipelines,
        const std::shared_ptr<std::mutex>& _project_mtx,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          data_frame_manager_( _data_frame_manager ),
          data_frames_( _data_frames ),
          fe_tracker_( _fe_tracker ),
          join_keys_encoding_( _join_keys_encoding ),
          license_checker_( _license_checker ),
          logger_( _logger ),
          monitor_( _monitor ),
          options_( _options ),
          pipelines_( _pipelines ),
          project_mtx_( _project_mtx ),
          read_write_lock_( _read_write_lock )
    {
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

    /// Returns a list of all pipelines currently held in memory.
    void list_pipelines( Poco::Net::StreamSocket* _socket ) const;

    /// Returns a list of all projects.
    void list_projects( Poco::Net::StreamSocket* _socket ) const;

    /// Loads a data frame
    void load_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a pipeline
    void load_pipeline(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Updates the encodings in the client
    void refresh( Poco::Net::StreamSocket* _socket ) const;

    /// Saves a data frame
    void save_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Saves a pipeline to disc.
    void save_pipeline(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Sets the current project
    void set_project(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    const std::string& project_directory() const { return project_directory_; }

    // ------------------------------------------------------------------------

   private:
    /// Deletes all pipelines and data frames (from memory only) and clears all
    /// encodings.
    void clear();

    /// Loads all pipelines.
    void load_all_pipelines();

    /// Loads a JSON object from a file.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

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

    /// Trivial accessor
    engine::licensing::LicenseChecker& license_checker()
    {
        assert_true( license_checker_ );
        return *license_checker_;
    }

    /// Trivial (private) accessor
    const monitoring::Logger& logger()
    {
        assert_true( logger_ );
        return *logger_;
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

    /// Keeps track of all feature engineerers.
    const std::shared_ptr<dependency::FETracker> fe_tracker_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the license and memory usage
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

    /// For logging
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// Settings for the engine
    const config::Options options_;

    /// The pipelines currently held in memory
    const std::shared_ptr<PipelineMapType> pipelines_;

    /// It is sometimes necessary to prevent us from changing the project.
    const std::shared_ptr<std::mutex> project_mtx_;

    /// The current project directory
    std::string project_directory_;

    /// For synchronization
    const std::shared_ptr<multithreading::ReadWriteLock>& read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGER_HPP_

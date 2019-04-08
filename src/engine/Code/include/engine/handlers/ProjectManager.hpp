#ifndef ENGINE_HANDLERS_PROJECTMANAGER_HPP_
#define ENGINE_HANDLERS_PROJECTMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class ProjectManager
{
   public:
    ProjectManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<DataFrameManager>& _data_frame_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        /*const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,
        const std::shared_ptr<SQLNET_MODEL_MAP> _models,
        const std::shared_ptr<const logging::Monitor>& _monitor,*/
        const config::Options& _options,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          data_frame_manager_( _data_frame_manager ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          // license_checker_( _license_checker ),
          // models_( _models ),
          // monitor_( _monitor ),
          options_( _options ),
          read_write_lock_( _read_write_lock )
    {
    }

    ~ProjectManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Adds a new data frame
    void add_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Adds a new model
    /*void add_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket& _socket );*/

    /// Deletes a data frame
    void delete_data_frame(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a model
    /*void delete_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket& _socket );*/

    /// Deletes a project
    void delete_project(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a data frame
    void load_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a model
    /*void load_model(
        const std::string& _name, Poco::Net::StreamSocket& _socket );*/

    /// Updates the encodings in the client
    void refresh( Poco::Net::StreamSocket* _socket );

    /// Saves a data frame
    void save_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a model
    /*void save_model(
        const std::string& _name, Poco::Net::StreamSocket& _socket );*/

    /// Sets the current project
    void set_project(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    const std::string& project_directory() const { return project_directory_; }

    // ------------------------------------------------------------------------

   private:
    /// Deletes all models and data frames (from memory only) and clears all
    /// encodings.
    void clear();

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    containers::Encoding& categories() { return *categories_; }

    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        return *data_frames_;
    }

    /// Returns a deep copy of a model.
    /*decisiontrees::DecisionTreeEnsemble get_model( const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto ptr = engine::Getter::get( *models_, _name );
        return *ptr;
    }*/

    /// Trivial accessor
    containers::Encoding& join_keys_encoding() { return *join_keys_encoding_; }

    /// Trivial accessor
    /*engine::licensing::LicenseChecker& license_checker()
    {
        return *license_checker_;
    }*/

    /// Trivial accessor
    // SQLNET_MODEL_MAP& models() { return *models_; }

    /// Sets a model.
    /*void set_model(
        const std::string& _name,
        const decisiontrees::DecisionTreeEnsemble& _model )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );

        auto it = models_->find( _name );

        if ( it == models_->end() )
            {
                read_lock.unlock();
                multithreading::WriteLock write_lock(
                    read_write_lock_ );
                ( *models_ )[_name] =
                    std::make_shared<decisiontrees::DecisionTreeEnsemble>(
                        _model );
            }
        else
            {
                it->second =
                    std::make_shared<decisiontrees::DecisionTreeEnsemble>(
                        _model );
            }
    }*/

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// We need some methods from the data frame manager.
    const std::shared_ptr<DataFrameManager> data_frame_manager_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the license and memory usage
    // const std::shared_ptr<engine::licensing::LicenseChecker>
    // license_checker_;

    /// The models currently held in memory
    // const std::shared_ptr<SQLNET_MODEL_MAP> models_;

    /// For communication with the monitor
    // const std::shared_ptr<const logging::Monitor> monitor_;

    /// Settings for the engine
    const config::Options options_;

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

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
    typedef MultirelModelManager::ModelMapType MultirelModelMapType;
    typedef RelboostModelManager::ModelMapType RelboostModelMapType;

    // ------------------------------------------------------------------------

   public:
    ProjectManager(
        const std::shared_ptr<MultirelModelMapType>& _multirel_models,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<DataFrameManager>& _data_frame_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<licensing::LicenseChecker>& _license_checker,
        const std::shared_ptr<RelboostModelMapType>& _relboost_models,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const config::Options& _options,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : multirel_models_( _multirel_models ),
          categories_( _categories ),
          data_frame_manager_( _data_frame_manager ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          license_checker_( _license_checker ),
          monitor_( _monitor ),
          options_( _options ),
          read_write_lock_( _read_write_lock ),
          relboost_models_( _relboost_models )
    {
    }

    ~ProjectManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Adds a new Multirel model to the project.
    void add_multirel_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

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

    /// Adds a new relboost model to the project.
    void add_relboost_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes an Multirel model
    void delete_multirel_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a data frame
    void delete_data_frame(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a relboost model
    void delete_relboost_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Deletes a project
    void delete_project(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads an Multirel model
    void load_multirel_model(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a data frame
    void load_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Loads a relboost model
    void load_relboost_model(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Updates the encodings in the client
    void refresh( Poco::Net::StreamSocket* _socket );

    /// Saves an Multirel model to disc.
    void save_multirel_model(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Saves a data frame
    void save_data_frame(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Saves a relboost model to disc.
    void save_relboost_model(
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
    /// Deletes all models and data frames (from memory only) and clears all
    /// encodings.
    void clear();

    /// Loads all models.
    void load_all_models();

    /// Loads a JSON object from a file.
    Poco::JSON::Object load_json_obj( const std::string& _fname ) const;

    // ------------------------------------------------------------------------

   private:
    /// Trivial (private) accessor
    MultirelModelMapType& multirel_models()
    {
        assert_true( multirel_models_ );
        return *multirel_models_;
    }

    /// Trivial (private) accessor
    const MultirelModelMapType& multirel_models() const
    {
        assert_true( multirel_models_ );
        return *multirel_models_;
    }

    /// Trivial accessor
    containers::Encoding& categories() { return *categories_; }

    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        return *data_frames_;
    }

    /// Returns a deep copy of a model.
    models::MultirelModel get_multirel_model( const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto ptr = utils::Getter::get( _name, &multirel_models() );
        return *ptr;
    }

    /// Returns a deep copy of a model.
    models::RelboostModel get_relboost_model( const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto ptr = utils::Getter::get( _name, &relboost_models() );
        return *ptr;
    }

    /// Trivial accessor
    containers::Encoding& join_keys_encoding() { return *join_keys_encoding_; }

    /// Trivial accessor
    engine::licensing::LicenseChecker& license_checker()
    {
        assert_true( license_checker_ );
        return *license_checker_;
    }

    /// Trivial (private) accessor
    RelboostModelMapType& relboost_models()
    {
        assert_true( relboost_models_ );
        return *relboost_models_;
    }

    /// Trivial (private) accessor
    const RelboostModelMapType& relboost_models() const
    {
        assert_true( relboost_models_ );
        return *relboost_models_;
    }

    /// Sets a model.
    void set_multirel_model(
        const std::string& _name, const models::MultirelModel& _model )
    {
        multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

        auto it = multirel_models().find( _name );

        weak_write_lock.upgrade();

        if ( it == multirel_models().end() )
            {
                multirel_models()[_name] =
                    std::make_shared<models::MultirelModel>( _model );
            }
        else
            {
                it->second = std::make_shared<models::MultirelModel>( _model );
            }
    }

    /// Sets a model.
    void set_relboost_model(
        const std::string& _name,
        const models::Model<relboost::ensemble::DecisionTreeEnsemble>& _model )
    {
        multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

        auto it = relboost_models().find( _name );

        weak_write_lock.upgrade();

        if ( it == relboost_models().end() )
            {
                relboost_models()[_name] =
                    std::make_shared<models::RelboostModel>( _model );
            }
        else
            {
                it->second = std::make_shared<models::RelboostModel>( _model );
            }
    }

    // ------------------------------------------------------------------------

   private:
    /// The Multirel models currently held in memory
    const std::shared_ptr<MultirelModelMapType> multirel_models_;

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
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// Settings for the engine
    const config::Options options_;

    /// The current project directory
    std::string project_directory_;

    /// For synchronization
    const std::shared_ptr<multithreading::ReadWriteLock>& read_write_lock_;

    /// The relboost models currently held in memory
    const std::shared_ptr<RelboostModelMapType> relboost_models_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_PROJECTMANAGER_HPP_

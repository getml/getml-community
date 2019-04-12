#ifndef ENGINE_HANDLERS_MODELMANAGER_HPP_
#define ENGINE_HANDLERS_MODELMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class ModelManager
{
    // ------------------------------------------------------------------------

   public:
    typedef std::map<
        std::string,
        std::shared_ptr<relboost::ensemble::DecisionTreeEnsemble>>
        ModelMapType;

    // ------------------------------------------------------------------------

   public:
    ModelManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        /*const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,*/
        const std::shared_ptr<const logging::Logger>& _logger,
        const std::shared_ptr<ModelMapType> _models,
        // const std::shared_ptr<const logging::Monitor>& _monitor,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          // license_checker_( _license_checker ),
          logger_( _logger ),
          models_( _models ),
          // monitor_( _monitor ),
          read_write_lock_( _read_write_lock )

    {
    }

    ~ModelManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Copies a model
    void copy_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Fits a model
    void fit_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Refreshes a model in the target language
    void refresh_model(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Scores a model
    void score(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Transform a model to a JSON string
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
    /// Receives data from the client. This data will not be stored permanently,
    /// but locally. Once the training/transformation process is complete, it
    /// will be deleted.
    Poco::JSON::Object receive_data(
        const Poco::JSON::Object& _cmd,
        const std::shared_ptr<containers::Encoding>& _categories,
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

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    std::map<std::string, containers::DataFrame>& data_frames()
    {
        return *data_frames_;
    }

    /// Returns a deep copy of a model.
    relboost::ensemble::DecisionTreeEnsemble get_model(
        const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto ptr = utils::Getter::get( _name, models_.get() );
        return *ptr;
    }

    /// Trivial accessor
    const logging::Logger& logger() { return *logger_; }

    /// Sets a model.
    void set_model(
        const std::string& _name,
        const relboost::ensemble::DecisionTreeEnsemble& _model )
    {
        multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

        auto it = models_->find( _name );

        weak_write_lock.upgrade();

        if ( it == models_->end() )
            {
                ( *models_ )[_name] =
                    std::make_shared<relboost::ensemble::DecisionTreeEnsemble>(
                        _model );
            }
        else
            {
                it->second =
                    std::make_shared<relboost::ensemble::DecisionTreeEnsemble>(
                        _model );
            }
    }

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the license and memory usage
    // const std::shared_ptr<engine::licensing::LicenseChecker>
    // license_checker_;

    /// For logging
    const std::shared_ptr<const logging::Logger> logger_;

    /// The models currently held in memory
    const std::shared_ptr<ModelManager::ModelMapType> models_;

    /// For communication with the monitor
    // const std::shared_ptr<const logging::Monitor> monitor_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_MODELMANAGER_HPP_

#ifndef ENGINE_HANDLERS_MODELMANAGER_HPP_
#define ENGINE_HANDLERS_MODELMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

template <typename ModelType>
class ModelManager
{
    // ------------------------------------------------------------------------

   public:
    typedef std::map<std::string, std::shared_ptr<ModelType>> ModelMapType;

    // ------------------------------------------------------------------------

   public:
    ModelManager(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        /*const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,*/
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<ModelMapType>& _models,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          // license_checker_( _license_checker ),
          logger_( _logger ),
          models_( _models ),
          monitor_( _monitor ),
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
    ModelType get_model( const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        auto ptr = utils::Getter::get( _name, &models() );
        return *ptr;
    }

    /// Trivial (private) accessor
    const monitoring::Logger& logger() { return *logger_; }

    /// Trivial (private) accessor
    ModelMapType& models() { return *models_; }

    /// Trivial (private) accessor
    const ModelMapType& models() const { return *models_; }

    /// Posts an AutoSQL model.
    template <
        typename MType = ModelType,
        typename std::enable_if<
            std::is_same<MType, models::AutoSQLModel>::value,
            int>::type = 0>
    void post_model( const Poco::JSON::Object& _obj )
    {
        monitor_->send( "postautosqlmodel", _obj );
    }

    /// Posts a relboost model.
    template <
        typename MType = ModelType,
        typename std::enable_if<
            std::is_same<MType, models::RelboostModel>::value,
            int>::type = 0>
    void post_model( const Poco::JSON::Object& _obj )
    {
        monitor_->send( "postrelboostmodel", _obj );
    }

    /// Sets a model.
    void set_model( const std::string& _name, const ModelType& _model )
    {
        multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

        auto it = models().find( _name );

        weak_write_lock.upgrade();

        if ( it == models().end() )
            {
                models()[_name] = std::make_shared<ModelType>( _model );
            }
        else
            {
                it->second = std::make_shared<ModelType>( _model );
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
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// The models currently held in memory
    const std::shared_ptr<ModelMapType> models_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// For coordinating the read and write process of the data
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::copy_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string other = JSON::get_value<std::string>( _cmd, "other_" );

    auto other_model = get_model( other );

    post_model( other_model.to_monitor( _name ) );

    set_model( _name, other_model );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::fit_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // -------------------------------------------------------
    // Receive data

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>(
            data_frames() );

    auto cmd = communication::Receiver::recv_cmd( logger_, _socket );

    cmd = receive_data( cmd, local_categories, local_data_frames, _socket );

    // -------------------------------------------------------
    // Do the actual fitting

    model.fit( cmd, logger_, *local_data_frames, _socket );

    // -------------------------------------------------------
    // Upgrade to a strong write lock - we are about to write something.

    weak_write_lock.upgrade();

    // -------------------------------------------------------

    auto it = models().find( _name );

    if ( it == models().end() )
        {
            models()[_name] = std::make_shared<ModelType>( model );
        }
    else
        {
            it->second = std::make_shared<ModelType>( model );
        }

    categories_->append( *local_categories );

    // -------------------------------------------------------

    weak_write_lock.unlock();

    // -------------------------------------------------------

    post_model( model.to_monitor( _name ) );

    communication::Sender::send_string( "Trained model.", _socket );

    send_data( categories_, local_data_frames, _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename ModelType>
Poco::JSON::Object ModelManager<ModelType>::receive_data(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Declare local variables. The idea of the local variables
    // is to prevent the global variables from being affected
    // by local data frames.

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto local_read_write_lock =
        std::make_shared<multithreading::ReadWriteLock>();

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto local_data_frame_manager = DataFrameManager(
        _categories,
        std::shared_ptr<database::Connector>(),
        _data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        monitor_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Receive data.

    auto cmd = _cmd;

    while ( true )
        {
            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type == "DataFrame" )
                {
                    local_data_frame_manager.add_data_frame( name, _socket );
                }
            else if ( type == "DataFrame.from_db" )
                {
                    local_data_frame_manager.from_db(
                        name, cmd, false, _socket );
                }
            else if ( type == "DataFrame.from_json" )
                {
                    local_data_frame_manager.from_json(
                        name, cmd, false, _socket );
                }
            else
                {
                    break;
                }

            cmd = communication::Receiver::recv_cmd( logger_, _socket );
        }

    // -------------------------------------------------------

    return cmd;

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::send_data(
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Declare local variables. The idea of the local variables
    // is to prevent the global variables from being affected
    // by local data frames.

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto local_read_write_lock =
        std::make_shared<multithreading::ReadWriteLock>();

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto local_data_frame_manager = DataFrameManager(
        _categories,
        std::shared_ptr<database::Connector>(),
        _local_data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        monitor_,
        local_read_write_lock );

    auto local_model_manager = ModelManager(
        _categories,
        _local_data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        models_,
        monitor_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Send data.

    while ( true )
        {
            const auto cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type == "Column.get" )
                {
                    local_data_frame_manager.get_column( cmd, _socket );
                }
            else if ( type == "transform" )
                {
                    local_model_manager.transform( name, cmd, _socket );
                }
            else
                {
                    communication::Sender::send_string( "Success!", _socket );
                    return;
                }
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::score(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Do the actual scoring.

    auto scores = model.score( _cmd, _socket );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------

    set_model( _name, model );

    post_model( model.to_monitor( _name ) );

    communication::Sender::send_string( JSON::stringify( scores ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::to_json(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string(
        JSON::stringify( model.to_json_obj() ), _socket );
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::to_sql(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( model.to_sql(), _socket );
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::transform(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Receive data

    multithreading::ReadLock read_lock( read_write_lock_ );

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>(
            data_frames() );

    auto cmd = communication::Receiver::recv_cmd( logger_, _socket );

    cmd = receive_data( cmd, local_categories, local_data_frames, _socket );

    // -------------------------------------------------------
    // Do the actual transformation

    auto yhat = model.transform( cmd, logger_, *local_data_frames, _socket );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------
    // Send data

    communication::Sender::send_matrix( yhat, _socket );

    send_data( categories_, local_data_frames, _socket );

    // -------------------------------------------------------
    // Store model, if necessary.

    read_lock.unlock();

    if ( JSON::get_value<bool>( cmd, "score_" ) )
        {
            set_model( _name, model );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_MODELMANAGER_HPP_

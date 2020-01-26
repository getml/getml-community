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
        const std::shared_ptr<DatabaseManager>& _database_manager,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>
            _data_frames,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        const std::shared_ptr<engine::licensing::LicenseChecker>&
            _license_checker,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<ModelMapType>& _models,
        const std::shared_ptr<const monitoring::Monitor>& _monitor,
        const std::shared_ptr<multithreading::ReadWriteLock>& _read_write_lock )
        : categories_( _categories ),
          database_manager_( _database_manager ),
          data_frames_( _data_frames ),
          join_keys_encoding_( _join_keys_encoding ),
          license_checker_( _license_checker ),
          logger_( _logger ),
          models_( _models ),
          monitor_( _monitor ),
          read_write_lock_( _read_write_lock )

    {
    }

    ~ModelManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Determines whether the model should
    /// allow HTTP requests.
    void allow_http(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Fits a model
    void fit_model(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the scores of all models belonging to the session _name.
    void get_hyperopt_names(
        const std::string& _name, Poco::Net::StreamSocket* _socket ) const;

    /// Returns the scores of all models belonging to the session _name.
    void get_hyperopt_scores(
        const std::string& _name, Poco::Net::StreamSocket* _socket ) const;

    /// Sends a command to the monitor to launch a hyperparameter optimization.
    void launch_hyperopt(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

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

    /// Writes a set of features to the data base.
    void to_db(
        const ModelType& _model,
        const Poco::JSON::Object& _cmd,
        const containers::Features& _yhat,
        const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
            _local_data_frames,
        Poco::Net::StreamSocket* _socket );

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    std::shared_ptr<database::Connector> connector()
    {
        assert_true( database_manager_ );
        return database_manager_->connector();
    }

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

    /// Trivial accessor
    const licensing::LicenseChecker& license_checker() const
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
    ModelMapType& models()
    {
        assert_true( models_ );
        return *models_;
    }

    /// Trivial (private) accessor
    const ModelMapType& models() const
    {
        assert_true( models_ );
        return *models_;
    }

    /// Posts an Multirel model.
    template <
        typename MType = ModelType,
        typename std::enable_if<
            std::is_same<MType, models::MultirelModel>::value,
            int>::type = 0>
    void post_model( const Poco::JSON::Object& _obj )
    {
        monitor_->send( "postmultirelmodel", _obj );
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

        if ( it == models().end() )
            {
                throw std::runtime_error(
                    "Model '" + _name + "' does not exist!" );
            }

        weak_write_lock.upgrade();

        it->second = std::make_shared<ModelType>( _model );
    }

    // ------------------------------------------------------------------------

   private:
    /// Maps integeres to category names
    const std::shared_ptr<containers::Encoding> categories_;

    /// Connector to the underlying database.
    const std::shared_ptr<DatabaseManager> database_manager_;

    /// The data frames currently held in memory
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>
        data_frames_;

    /// Maps integers to join key names
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;

    /// For checking the number of cores and memory usage
    const std::shared_ptr<licensing::LicenseChecker> license_checker_;

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
void ModelManager<ModelType>::allow_http(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const bool allow_http = JSON::get_value<bool>( _cmd, "allow_http_" );

    auto model = get_model( _name );

    model.allow_http() = allow_http;

    set_model( _name, model );

    post_model( model.to_monitor( _name ) );

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
    // Some models are only supported by the premium version.

    if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }

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
            throw std::runtime_error(
                "Model named '" + _name + "' does not exist!" );
        }

    it->second = std::make_shared<ModelType>( model );

    assert_true( categories_ );

    assert_true( local_categories );

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
void ModelManager<ModelType>::get_hyperopt_names(
    const std::string& _name, Poco::Net::StreamSocket* _socket ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Array names;

    for ( const auto& [key, model] : models() )
        {
            if ( model->session_name() == _name )
                {
                    names.add( key );
                }
        }

    Poco::JSON::Object obj;

    obj.set( "names_", names );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::get_hyperopt_scores(
    const std::string& _name, Poco::Net::StreamSocket* _socket ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Object scores;

    for ( const auto& [key, model] : models() )
        {
            if ( model->session_name() == _name )
                {
                    const auto new_scores = metrics::Scorer::get_metrics(
                        model->scores().to_json_obj() );

                    scores.set( key, new_scores );
                }
        }

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( scores ), _socket );
}

// ------------------------------------------------------------------------

template <typename ModelType>
void ModelManager<ModelType>::launch_hyperopt(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Some models are only supported by the premium version.

    if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }

    // -------------------------------------------------------
    // Find the reference model.

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Receive the complete command and send to engine.

    const auto json_str = communication::Receiver::recv_string( _socket );

    const auto [status, response] =
        monitor_->send( "launchhyperopt", json_str );

    if ( status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK )
        {
            communication::Sender::send_string( "Success!", _socket );
        }
    else
        {
            communication::Sender::send_string( response, _socket );
        }

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
        database_manager_,
        _data_frames,
        local_join_keys_encoding,
        license_checker_,
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
            else if ( type == "DataFrame.from_query" )
                {
                    license_checker().check_enterprise();
                    local_data_frame_manager.from_query(
                        name, cmd, false, _socket );
                }
            else if ( type == "DataFrame.from_json" )
                {
                    license_checker().check_enterprise();
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
void ModelManager<ModelType>::refresh_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto model = get_model( _name );

    // true refers to _schema_only.
    const auto obj = model.to_json_obj( true );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
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
        database_manager_,
        _local_data_frames,
        local_join_keys_encoding,
        license_checker_,
        logger_,
        monitor_,
        local_read_write_lock );

    auto local_model_manager = ModelManager(
        _categories,
        database_manager_,
        _local_data_frames,
        local_join_keys_encoding,
        license_checker_,
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
                    local_data_frame_manager.get_column( name, cmd, _socket );
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
void ModelManager<ModelType>::to_db(
    const ModelType& _model,
    const Poco::JSON::Object& _cmd,
    const containers::Features& _yhat,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Get population table.

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto& population_table =
        utils::Getter::get( population_name, _local_data_frames.get() );

    // -------------------------------------------------------
    // Build data frame.

    containers::DataFrame df;

    if ( _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" ) )
        {
            const auto target_names = _model.target_names();

            assert_true( target_names.size() == _yhat.size() );

            for ( size_t i = 0; i < target_names.size(); ++i )
                {
                    auto col = containers::Column( _yhat[i] );
                    col.set_name( target_names[i] + "_prediction" );
                    df.add_float_column( col, "target" );
                }
        }
    else
        {
            const auto [autofeatures, categorical, numerical] =
                _model.feature_names();

            assert_true(
                autofeatures.size() + numerical.size() == _yhat.size() );

            size_t j = 0;

            for ( size_t i = 0; i < autofeatures.size(); ++i )
                {
                    auto col = containers::Column( _yhat[j++] );
                    col.set_name( autofeatures[i] );
                    df.add_float_column( col, "numerical" );
                }

            for ( size_t i = 0; i < numerical.size(); ++i )
                {
                    auto col = containers::Column( _yhat[j++] );
                    col.set_name( numerical[i] );
                    df.add_float_column( col, "numerical" );
                }

            for ( const auto& colname : categorical )
                {
                    auto col = population_table.categorical( colname );
                    df.add_int_column( col, "categorical" );
                }
        }

    // -------------------------------------------------------
    // Add join keys and time stamps.

    for ( size_t i = 0; i < population_table.num_join_keys(); ++i )
        {
            const auto col = population_table.join_key( i );
            df.add_int_column( col, "join_key" );
        }

    for ( size_t i = 0; i < population_table.num_time_stamps(); ++i )
        {
            const auto col = population_table.time_stamp( i );
            df.add_float_column( col, "time_stamp" );
        }

    // -------------------------------------------------------
    // Write data frame to data base.

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    // We are using the bell character (\a) as the quotechar. It is least likely
    // to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    const auto statement = csv::StatementMaker::make_statement(
        table_name,
        connector()->dialect(),
        reader.colnames(),
        reader.coltypes() );

    logger().log( statement );

    connector()->execute( statement );

    connector()->read( table_name, false, 0, &reader );

    database_manager_->post_tables();

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
    // Some models are only supported by the premium version.

    if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }

    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    if ( JSON::get_value<bool>( _cmd, "http_request_" ) )
        {
            if ( !model.allow_http() )
                {
                    throw std::invalid_argument(
                        "Model '" + _name +
                        "' does not allow HTTP requests. You can activate this "
                        "via the API or the getML monitor!" );
                }

            if constexpr ( !ModelType::premium_only_ )
                {
                    license_checker().check_enterprise();
                }
        }

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
    // Send data to client or write to data base

    if ( JSON::get_value<std::string>( cmd, "table_name_" ) == "" )
        {
            communication::Sender::send_features( yhat, _socket );
        }
    else
        {
            license_checker().check_enterprise();
            to_db( model, cmd, yhat, local_data_frames, _socket );
        }

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

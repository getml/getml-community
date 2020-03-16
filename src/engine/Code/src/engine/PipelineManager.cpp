#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void PipelineManager::deploy(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const bool deploy = JSON::get_value<bool>( _cmd, "deploy_" );

    auto pipeline = get_pipeline( _name );

    pipeline.allow_http() = deploy;

    set_pipeline( _name, pipeline );

    // TODO
    // post_pipeline( pipeline.to_monitor( _name ) );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::fit(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Some models are only supported by the premium version.

    // TODO
    /*
    if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }*/

    // -------------------------------------------------------
    // Find the pipeline.

    auto pipeline = get_pipeline( _name );

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Do the actual fitting

    multithreading::ReadLock read_lock( read_write_lock_ );

    pipeline.fit( _cmd, logger_, data_frames(), _socket );

    // -------------------------------------------------------
    // Fitting has been a success - store the pipeline.

    read_lock.unlock();

    set_pipeline( _name, pipeline );

    // -------------------------------------------------------

    // TODO
    // post_model( model.to_monitor( _name ) );

    communication::Sender::send_string( "Trained model.", _socket );

    read_lock.lock();

    // TODO
    // send_data( categories_, data_frames_, _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::get_hyperopt_names(
    const std::string& _name, Poco::Net::StreamSocket* _socket ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Array names;

    for ( const auto& [key, pipeline] : pipelines() )
        {
            if ( pipeline->session_name() == _name )
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

void PipelineManager::get_hyperopt_scores(
    const std::string& _name, Poco::Net::StreamSocket* _socket ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Object scores;

    for ( const auto& [key, pipeline] : pipelines() )
        {
            if ( pipeline->session_name() == _name )
                {
                    const auto new_scores = metrics::Scorer::get_metrics(
                        pipeline->scores().to_json_obj() );

                    scores.set( key, new_scores );
                }
        }

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( scores ), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::launch_hyperopt(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // The project guard will prevent any attempts to
    // change or delete the project while the hyperparameter
    // optimization is running.

    std::lock_guard<std::mutex> project_guard( project_mtx() );

    // -------------------------------------------------------
    // Some models are only supported by the premium version.

    // TODO
    /*if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }*/

    // -------------------------------------------------------
    // Find the reference pipeline.

    auto pipeline = get_pipeline( _name );

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

Poco::JSON::Object PipelineManager::receive_data(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
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

    auto local_data_frame_manager = DataFrameManager(
        _categories,
        database_manager_,
        _data_frames,
        _join_keys_encoding,
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
            else if ( type == "FloatColumn.set_unit" )
                {
                    local_data_frame_manager.set_unit( name, cmd, _socket );
                }
            else if ( type == "StringColumn.set_unit" )
                {
                    local_data_frame_manager.set_unit_categorical(
                        name, cmd, _socket );
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

void PipelineManager::refresh(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto pipeline = get_pipeline( _name );

    // true refers to _schema_only.
    const auto obj = pipeline.to_json_obj( true );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::send_data(
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Declare local variables. The idea of the local variables
    // is to prevent the global variables from being affected
    // by local data frames.

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

    auto local_pipeline_manager = PipelineManager(
        _categories,
        database_manager_,
        _local_data_frames,
        local_join_keys_encoding,
        license_checker_,
        logger_,
        monitor_,
        pipelines_,
        project_mtx_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Send data.

    while ( true )
        {
            const auto cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type == "FloatColumn.get" )
                {
                    local_data_frame_manager.get_column( name, cmd, _socket );
                }
            else if ( type == "transform" )
                {
                    local_pipeline_manager.transform( name, cmd, _socket );
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

void PipelineManager::score(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto pipeline = get_pipeline( _name );

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Do the actual scoring.

    // TODO
    // auto scores = pipeline.score( _cmd, _socket );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------

    set_pipeline( _name, pipeline );

    // TODO
    // post_model( model.to_monitor( _name ) );

    // TODO
    // communication::Sender::send_string( JSON::stringify( scores ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::to_db(
    const pipelines::Pipeline& _pipeline,
    const Poco::JSON::Object& _cmd,
    const containers::Features& _yhat,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Transforms the features into a data frame.

    const auto df = to_df(
        _pipeline,
        _cmd,
        _yhat,
        _categories,
        _join_keys_encoding,
        _local_data_frames,
        _socket );

    // -------------------------------------------------------
    // Write data frame to data base.

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    // We are using the bell character (\a) as the quotechar. It is least likely
    // to appear in any field.
    auto reader = containers::DataFrameReader(
        df, _categories, _join_keys_encoding, '\a', '|' );

    const auto statement = io::StatementMaker::make_statement(
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

containers::DataFrame PipelineManager::to_df(
    const pipelines::Pipeline& _pipeline,
    const Poco::JSON::Object& _cmd,
    const containers::Features& _yhat,
    const std::shared_ptr<containers::Encoding>& _categories,
    const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
    const std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Get population table.

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto population_name =
        JSON::get_value<std::string>( _cmd, "population_name_" );

    const auto& population_table =
        utils::Getter::get( population_name, _local_data_frames.get() );

    // -------------------------------------------------------
    // Build data frame.

    containers::DataFrame df( df_name, _categories, _join_keys_encoding );

    if ( _cmd.has( "predict_" ) && JSON::get_value<bool>( _cmd, "predict_" ) )
        {
            // TODO
            /*const auto target_names = _pipeline.target_names();

            assert_true( target_names.size() == _yhat.size() );

            for ( size_t i = 0; i < target_names.size(); ++i )
                {
                    auto col = containers::Column( _yhat[i] );
                    col.set_name( target_names[i] + "_prediction" );
                    df.add_float_column( col, "target" );
                }*/
        }
    else
        {
            const auto [autofeatures, categorical, numerical] =
                _pipeline.feature_names();

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
                    auto col = population_table.categorical( colname ).clone();
                    df.add_int_column( col, "categorical" );
                }
        }

    // -------------------------------------------------------
    // Add join keys, time stamps and targets.

    for ( size_t i = 0; i < population_table.num_join_keys(); ++i )
        {
            const auto col = population_table.join_key( i ).clone();
            df.add_int_column( col, "join_key" );
        }

    for ( size_t i = 0; i < population_table.num_time_stamps(); ++i )
        {
            const auto col = population_table.time_stamp( i ).clone();
            df.add_float_column( col, "time_stamp" );
        }

    for ( size_t i = 0; i < population_table.num_targets(); ++i )
        {
            const auto col = population_table.target( i ).clone();
            df.add_float_column( col, "target" );
        }

    // -------------------------------------------------------

    return df;

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::to_json(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    auto pipeline = get_pipeline( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string(
        JSON::stringify( pipeline.to_json_obj( false ) ), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::to_sql(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto pipeline = get_pipeline( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( pipeline.to_sql(), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::transform(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Some models are only supported by the premium version.

    // TODO
    /*if constexpr ( ModelType::premium_only_ )
        {
            license_checker().check_enterprise();
        }*/

    // -------------------------------------------------------
    // Find the model.

    auto pipeline = get_pipeline( _name );

    if ( JSON::get_value<bool>( _cmd, "http_request_" ) )
        {
            if ( !pipeline.allow_http() )
                {
                    throw std::invalid_argument(
                        "Pipeline '" + _name +
                        "' does not allow HTTP requests. You can activate "
                        "this "
                        "via the API or the getML monitor!" );
                }

            // TODO
            /*if constexpr ( !ModelType::premium_only_ )
                {
                    license_checker().check_enterprise();
                }*/
        }

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Receive data

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto local_data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>(
            data_frames() );

    auto cmd = communication::Receiver::recv_cmd( logger_, _socket );

    cmd = receive_data(
        cmd,
        local_categories,
        local_join_keys_encoding,
        local_data_frames,
        _socket );

    // -------------------------------------------------------
    // Do the actual transformation

    auto yhat = pipeline.transform( cmd, logger_, *local_data_frames, _socket );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------
    // Send data to client or write to data base

    const auto table_name = JSON::get_value<std::string>( cmd, "table_name_" );

    const auto df_name = JSON::get_value<std::string>( cmd, "df_name_" );

    if ( table_name == "" && df_name == "" )
        {
            communication::Sender::send_features( yhat, _socket );
        }
    else if ( table_name != "" )
        {
            license_checker().check_enterprise();

            to_db(
                pipeline,
                cmd,
                yhat,
                local_categories,
                local_join_keys_encoding,
                local_data_frames,
                _socket );
        }

    // -------------------------------------------------------
    // Write to DataFrame.

    if ( df_name != "" )
        {
            license_checker().check_enterprise();

            auto df = to_df(
                pipeline,
                cmd,
                yhat,
                local_categories,
                local_join_keys_encoding,
                local_data_frames,
                _socket );

            weak_write_lock.upgrade();

            categories_->append( *local_categories );

            join_keys_encoding_->append( *local_join_keys_encoding );

            df.set_categories( categories_ );

            df.set_join_keys_encoding( join_keys_encoding_ );

            data_frames()[df_name] = df;

            monitor_->send( "postdataframe", df.to_monitor() );
        }

    // -------------------------------------------------------

    send_data( categories_, local_data_frames, _socket );

    // -------------------------------------------------------
    // Store model, if necessary.

    weak_write_lock.unlock();

    if ( JSON::get_value<bool>( cmd, "score_" ) )
        {
            set_pipeline( _name, pipeline );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

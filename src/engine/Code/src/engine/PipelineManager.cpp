#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void PipelineManager::check(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    // -------------------------------------------------------

    if ( pipeline.premium_only() )
        {
            license_checker().check_enterprise();
        }

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    pipeline.check( _cmd, logger_, data_frames(), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::column_importances(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num = JSON::get_value<Int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores();

    // -------------------------------------------------------

    auto importances = std::vector<Float>();

    for ( const auto& vec : scores.column_importances() )
        {
            if ( target_num < 0 )
                {
                    const auto sum_importances =
                        std::accumulate( vec.begin(), vec.end(), 0.0 );

                    const auto length = static_cast<Float>( vec.size() );

                    importances.push_back( sum_importances / length );

                    continue;
                }

            if ( static_cast<size_t>( target_num ) >= vec.size() )
                {
                    throw std::invalid_argument( "target_num out of range!" );
                }

            importances.push_back( vec.at( target_num ) );
        }

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set(
        "column_descriptions_",
        JSON::vector_to_array( scores.column_descriptions() ) );

    response.set( "column_importances_", JSON::vector_to_array( importances ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

    // -------------------------------------------------------
}

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

    post_pipeline( pipeline.to_monitor( categories().vector(), _name ) );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::feature_correlations(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num =
        JSON::get_value<unsigned int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores();

    // -------------------------------------------------------

    auto correlations = std::vector<Float>();

    for ( const auto& vec : scores.feature_correlations() )
        {
            if ( static_cast<size_t>( target_num ) >= vec.size() )
                {
                    throw std::invalid_argument( "target_num out of range!" );
                }

            correlations.push_back( vec.at( target_num ) );
        }

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set(
        "feature_names_", JSON::vector_to_array( scores.feature_names() ) );

    response.set(
        "feature_correlations_", JSON::vector_to_array( correlations ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::feature_importances(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num =
        JSON::get_value<unsigned int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores();

    // -------------------------------------------------------

    auto importances = std::vector<Float>();

    for ( const auto& vec : scores.feature_importances() )
        {
            if ( static_cast<size_t>( target_num ) >= vec.size() )
                {
                    throw std::invalid_argument( "target_num out of range!" );
                }

            importances.push_back( vec.at( target_num ) );
        }

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set(
        "feature_names_", JSON::vector_to_array( scores.feature_names() ) );

    response.set(
        "feature_importances_", JSON::vector_to_array( importances ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::fit(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the pipeline.

    auto pipeline = get_pipeline( _name );

    // -------------------------------------------------------
    // Some models are only supported by the premium version.

    if ( pipeline.premium_only() )
        {
            license_checker().check_enterprise();
        }

    communication::Sender::send_string( "Found!", _socket );

    // -------------------------------------------------------
    // Do the actual fitting

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // TODO
    /*auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );*/

    pipeline.fit(
        _cmd, logger_, data_frames(), fe_tracker_, pred_tracker_, _socket );

    // -------------------------------------------------------
    // Fitting has been a success - store the pipeline.

    auto it = pipelines().find( _name );

    if ( it == pipelines().end() )
        {
            throw std::runtime_error(
                "Pipeline '" + _name + "' does not exist!" );
        }

    weak_write_lock.upgrade();

    it->second = pipeline;

    weak_write_lock.unlock();

    // -------------------------------------------------------

    post_pipeline( pipeline.to_monitor( categories().vector(), _name ) );

    communication::Sender::send_string( "Trained pipeline.", _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Array::Ptr PipelineManager::get_array(
    const Poco::JSON::Object& _scores,
    const std::string& _name,
    const unsigned int _target_num ) const
{
    const auto arr = JSON::get_array( _scores, _name );

    if ( static_cast<size_t>( _target_num ) >= arr->size() )
        {
            std::string msg = "target_num_ out of bounds! Got " +
                              std::to_string( _target_num ) + ", but '" +
                              _name + "' has " + std::to_string( arr->size() ) +
                              " entries.";

            if ( arr->size() == 0 )
                {
                    msg += " Did you maybe for get to call .score(...)?";
                }

            throw std::invalid_argument( msg );
        }

    return arr->getArray( _target_num );
}

// ------------------------------------------------------------------------

void PipelineManager::lift_curve(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num =
        JSON::get_value<unsigned int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores().to_json_obj();

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set(
        "proportion_", get_array( scores, "proportion_", target_num ) );

    response.set( "lift_", get_array( scores, "lift_", target_num ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::post_pipeline( const Poco::JSON::Object& _obj )
{
    const auto response = monitor().send_tcp( "postpipeline", _obj );

    if ( response != "Success!" )
        {
            throw std::runtime_error( response );
        }
}

// ------------------------------------------------------------------------

void PipelineManager::precision_recall_curve(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num =
        JSON::get_value<unsigned int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores().to_json_obj();

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set( "precision_", get_array( scores, "precision_", target_num ) );

    response.set( "tpr_", get_array( scores, "tpr_", target_num ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

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
    const auto pipeline = get_pipeline( _name );

    const auto obj = pipeline.obj();

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::roc_curve(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto target_num =
        JSON::get_value<unsigned int>( _cmd, "target_num_" );

    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    const auto scores = pipeline.scores().to_json_obj();

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set( "fpr_", get_array( scores, "fpr_", target_num ) );

    response.set( "tpr_", get_array( scores, "tpr_", target_num ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::score(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const std::map<std::string, containers::DataFrame>& _data_frames,
    const containers::Features& _yhat,
    pipelines::Pipeline* _pipeline,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Do the actual scoring.

    auto scores = _pipeline->score( _cmd, _data_frames, _yhat );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------

    set_pipeline( _name, *_pipeline );

    post_pipeline( _pipeline->to_monitor( categories().vector(), _name ) );

    communication::Sender::send_string( JSON::stringify( scores ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void PipelineManager::targets(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------

    const auto pipeline = get_pipeline( _name );

    // -------------------------------------------------------

    Poco::JSON::Object response;

    response.set( "targets_", JSON::vector_to_array( pipeline.targets() ) );

    // -------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( response ), _socket );

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

    const auto conn_id = JSON::get_value<std::string>( _cmd, "conn_id_" );

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    // We are using the bell character (\a) as the quotechar. It is least likely
    // to appear in any field.
    auto reader = containers::DataFrameReader(
        df, _categories, _join_keys_encoding, '\a', '|' );

    const auto conn = connector( conn_id );

    assert_true( conn );

    const auto statement = io::StatementMaker::make_statement(
        table_name,
        conn->dialect(),
        conn->describe(),
        reader.colnames(),
        reader.coltypes() );

    logger().log( statement );

    conn->execute( statement );

    conn->read( table_name, 0, &reader );

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

    if ( !_cmd.has( "predict_" ) || !JSON::get_value<bool>( _cmd, "predict_" ) )
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

void PipelineManager::to_sql(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto pipeline = get_pipeline( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string(
        pipeline.to_sql( categories().vector() ), _socket );
}

// ------------------------------------------------------------------------

void PipelineManager::transform(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto pipeline = get_pipeline( _name );

    if ( pipeline.premium_only() )
        {
            license_checker().check_enterprise();
        }

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

            if ( !pipeline.premium_only() )
                {
                    license_checker().check_enterprise();
                }
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

    const auto score = JSON::get_value<bool>( cmd, "score_" );

    if ( table_name == "" && df_name == "" && !score )
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

            monitor_->send_tcp( "postdataframe", df.to_monitor() );
        }

    // -------------------------------------------------------
    // Score model, if necessary.

    weak_write_lock.unlock();

    if ( score )
        {
            assert_true( local_data_frames );
            this->score(
                _name, cmd, *local_data_frames, yhat, &pipeline, _socket );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

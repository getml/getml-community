#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

void ModelManager::copy_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    const std::string other = _cmd.AUTOSQL_GET( "other_" );

    auto other_model = get_model( other );

    monitor_->send( "postmodel", other_model.to_monitor( _name ) );

    set_model( _name, other_model );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ModelManager::fit_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<const logging::Logger> _logger,
    Poco::Net::StreamSocket& _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    engine::Sender::send_string( _socket, "Found!" );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories.

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // -------------------------------------------------------
    // Receive data

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>(
            data_frames() );

    auto cmd = engine::Receiver::recv_cmd( _socket, logger_ );

    receive_data( local_categories, _socket, local_data_frames, cmd );

    // -------------------------------------------------------
    // Do the actual fitting

    std::string msg =
        engine::Models::fit( _socket, cmd, _logger, *local_data_frames, model );

    // -------------------------------------------------------
    // Upgrade to a strong write lock - we are about to write something.

    weak_write_lock.upgrade();

    // -------------------------------------------------------

    auto it = models_->find( _name );

    if ( it == models_->end() )
        {
            ( *models_ )[_name] =
                std::make_shared<decisiontrees::DecisionTreeEnsemble>( model );
        }
    else
        {
            it->second =
                std::make_shared<decisiontrees::DecisionTreeEnsemble>( model );
        }

    categories_->append( *local_categories );

    // -------------------------------------------------------

    weak_write_lock.unlock();

    // -------------------------------------------------------

    monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::Sender::send_string( _socket, msg );

    send_data( categories_, _socket, local_data_frames );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::receive_data(
    const std::shared_ptr<containers::Encoding>& _categories,
    Poco::Net::StreamSocket& _socket,
    std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames,
    Poco::JSON::Object& _cmd )
{
    // -------------------------------------------------------
    // Declare local variables. The idea of the local variables
    // is to prevent the global variables from being affected
    // by local data frames.

    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    const auto local_read_write_lock =
        std::make_shared<autosql::multithreading::ReadWriteLock>();

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto local_data_frame_manager = engine::DataFrameManager(
        _categories,
        _local_data_frames,
        local_join_keys_encoding,
        license_checker_,
        logger_,
        monitor_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Receive data.

    while ( true )
        {
            std::string name = _cmd.AUTOSQL_GET( "name_" );

            std::string type = _cmd.AUTOSQL_GET( "type_" );

            if ( type != "DataFrame" )
                {
                    break;
                }

            local_data_frame_manager.add_data_frame( name, _socket );

            _cmd = engine::Receiver::recv_cmd( _socket, logger_ );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::refresh_model(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    auto model = get_model( _name );

    std::string json = model.to_json();

    engine::Sender::send_string( _socket, json );
}

// ------------------------------------------------------------------------

void ModelManager::send_data(
    const std::shared_ptr<containers::Encoding>& _categories,
    Poco::Net::StreamSocket& _socket,
    std::shared_ptr<std::map<std::string, containers::DataFrame>>&
        _local_data_frames )
{
    // -------------------------------------------------------
    // Declare local variables. The idea of the local variables
    // is to prevent the global variables from being affected
    // by local data frames.

    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    const auto local_read_write_lock =
        std::make_shared<autosql::multithreading::ReadWriteLock>();

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto local_data_frame_manager = engine::DataFrameManager(
        _categories,
        _local_data_frames,
        local_join_keys_encoding,
        license_checker_,
        logger_,
        monitor_,
        local_read_write_lock );

    auto local_model_manager = engine::ModelManager(
        _categories,
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
            auto cmd = engine::Receiver::recv_cmd( _socket, logger_ );

            std::string name = cmd.AUTOSQL_GET( "name_" );

            std::string type = cmd.AUTOSQL_GET( "type_" );

            if ( type == "Matrix.get" )
                {
                    local_data_frame_manager.get_matrix( name, cmd, _socket );
                }
            else if ( type == "transform" )
                {
                    local_model_manager.transform( name, cmd, _socket );
                }
            else
                {
                    engine::Sender::send_string( _socket, "Success!" );
                    return;
                }
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::score(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    engine::Sender::send_string( _socket, "Found!" );

    // -------------------------------------------------------
    // Do the actual scoring.

    auto scores = engine::Models::score( _cmd, _socket, &model );

    engine::Sender::send_string( _socket, "Success!" );

    // -------------------------------------------------------

    set_model( _name, model );

    monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::Sender::send_string( _socket, JSON::stringify( scores ) );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::to_json(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    auto model = get_model( _name );

    engine::Sender::send_string( _socket, "Found!" );

    engine::Sender::send_string( _socket, model.to_json() );
}

// ------------------------------------------------------------------------

void ModelManager::to_sql(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto model = get_model( _name );

    engine::Sender::send_string( _socket, "Found!" );

    engine::Sender::send_string( _socket, model.to_sql() );
}

// ------------------------------------------------------------------------

void ModelManager::transform(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    // -------------------------------------------------------
    // Find the model.

    auto model = get_model( _name );

    engine::Sender::send_string( _socket, "Found!" );

    // -------------------------------------------------------
    // Receive data

    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>(
            data_frames() );

    auto cmd = engine::Receiver::recv_cmd( _socket, logger_ );

    receive_data( local_categories, _socket, local_data_frames, cmd );

    // -------------------------------------------------------
    // Do the actual transformation

    const bool score = cmd.AUTOSQL_GET( "score_" );

    const bool predict = cmd.AUTOSQL_GET( "predict_" );

    auto yhat = engine::Models::transform(
        _socket, cmd, logger_, *local_data_frames, model, score, predict );

    engine::Sender::send_string( _socket, "Success!" );

    // -------------------------------------------------------
    // Send data

    engine::Sender::send_matrix<Float>( _socket, false, yhat );

    // -------------------------------------------------------

    send_data( categories_, _socket, local_data_frames );

    // -------------------------------------------------------
    // Store the scored model, if necessary.

    read_lock.unlock();

    if ( score )
        {
            set_model( _name, model );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql

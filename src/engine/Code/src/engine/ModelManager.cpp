#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void ModelManager::copy_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string other = JSON::get_value<std::string>( _cmd, "other_" );

    auto other_model = get_model( other );

    // monitor_->send( "postmodel", other_model.to_monitor( _name ) );

    set_model( _name, other_model );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ModelManager::fit_model(
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

    Models::fit( cmd, logger_, *local_data_frames, &model, _socket );

    // -------------------------------------------------------
    // Upgrade to a strong write lock - we are about to write something.

    weak_write_lock.upgrade();

    // -------------------------------------------------------

    auto it = models_->find( _name );

    if ( it == models_->end() )
        {
            ( *models_ )[_name] =
                std::make_shared<relboost::ensemble::DecisionTreeEnsemble>(
                    model );
        }
    else
        {
            it->second =
                std::make_shared<relboost::ensemble::DecisionTreeEnsemble>(
                    model );
        }

    categories_->append( *local_categories );

    // -------------------------------------------------------

    weak_write_lock.unlock();

    // -------------------------------------------------------

    // monitor_->send( "postmodel", model.to_monitor( _name ) );

    communication::Sender::send_string( "Trained RelboostModel.", _socket );

    send_data( categories_, local_data_frames, _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object ModelManager::receive_data(
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
        _data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        // monitor_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Receive data.

    auto cmd = _cmd;

    while ( true )
        {
            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type != "DataFrame" )
                {
                    break;
                }

            local_data_frame_manager.add_data_frame( name, _socket );

            cmd = communication::Receiver::recv_cmd( logger_, _socket );
        }

    // -------------------------------------------------------

    return cmd;

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::send_data(
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
        _local_data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        // monitor_,
        local_read_write_lock );

    auto local_model_manager = ModelManager(
        _categories,
        _local_data_frames,
        local_join_keys_encoding,
        // license_checker_,
        logger_,
        models_,
        // monitor_,
        local_read_write_lock );

    // -------------------------------------------------------
    // Send data.

    while ( true )
        {
            const auto cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

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
                    communication::Sender::send_string( "Success!", _socket );
                    return;
                }
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::score(
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

    Poco::JSON::Object scores;
    // auto scores = engine::Models::score( _cmd, _socket, &model );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------

    set_model( _name, model );

    // monitor_->send( "postmodel", model.to_monitor( _name ) );

    communication::Sender::send_string( JSON::stringify( scores ), _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void ModelManager::to_json(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string(
        JSON::stringify( model.to_json_obj() ), _socket );
}

// ------------------------------------------------------------------------

void ModelManager::to_sql(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto model = get_model( _name );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( model.to_sql(), _socket );
}

// ------------------------------------------------------------------------

void ModelManager::transform(
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

    auto yhat =
        Models::transform( cmd, logger_, *local_data_frames, model, _socket );

    if ( JSON::get_value<bool>( cmd, "score_" ) )
        {
            set_model( _name, model );
        }

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------
    // Send data

    communication::Sender::send_matrix<ENGINE_FLOAT>( yhat, _socket );

    send_data( categories_, local_data_frames, _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

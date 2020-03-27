#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void ProjectManager::add_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    data_frame_manager_->add_data_frame( _name, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_csv( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_db(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_db( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_json(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_json( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_query(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_query( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_pipeline(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto pipeline = pipelines::Pipeline( categories().vector(), _cmd );

    set_pipeline( _name, pipeline );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::copy_pipeline(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string other = JSON::get_value<std::string>( _cmd, "other_" );

    const auto other_pipeline = get_pipeline( other );

    set_pipeline( _name, other_pipeline );

    monitor_->send( "postpipeline", other_pipeline.to_monitor( _name ) );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::clear()
{
    // --------------------------------
    // Remove from monitor.

    for ( auto& pair : data_frames() )
        {
            monitor_->send(
                "removedataframe", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : pipelines() )
        {
            monitor_->send(
                "removepipeline", "{\"name\":\"" + pair.first + "\"}" );
        }

    // --------------------------------
    // Remove from engine.

    data_frames() = std::map<std::string, engine::containers::DataFrame>();

    pipelines() = engine::handlers::PipelineManager::PipelineMapType();

    categories().clear();

    join_keys_encoding().clear();

    // --------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::delete_data_frame(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &data_frames() );

    monitor_->send( "removedataframe", "{\"name\":\"" + _name + "\"}" );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_pipeline(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &pipelines() );

    monitor_->send( "removepipeline", "{\"name\":\"" + _name + "\"}" );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // Some methods, particularly the hyperparameter optimization,
    // require us to keep the project fixed while they run.
    std::lock_guard<std::mutex> project_guard( project_mtx() );

    multithreading::WriteLock write_lock( read_write_lock_ );

    if ( _name == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an "
                "empty string!" );
        }

    if ( project_directory_ == options_.all_projects_directory() + _name + "/" )
        {
            project_directory_ = "";
        }

    Poco::File( options_.all_projects_directory() + _name + "/" )
        .remove( true );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::list_data_frames( Poco::Net::StreamSocket* _socket ) const
{
    // ----------------------------------------------------------------

    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    // ----------------------------------------------------------------

    Poco::JSON::Object obj;

    multithreading::ReadLock read_lock( read_write_lock_ );

    // ----------------------------------------------------------------

    Poco::JSON::Array in_memory;

    for ( const auto& [key, value] : data_frames() )
        {
            in_memory.add( key );
        }

    // ----------------------------------------------------------------

    Poco::JSON::Array in_project_folder;

    Poco::DirectoryIterator end;

    for ( Poco::DirectoryIterator it( project_directory_ + "data/" ); it != end;
          ++it )
        {
            if ( it->isDirectory() )
                {
                    in_project_folder.add( it.name() );
                }
        }

    // ----------------------------------------------------------------

    read_lock.unlock();

    // ----------------------------------------------------------------

    obj.set( "in_memory", in_memory );

    obj.set( "in_project_folder", in_project_folder );

    // ----------------------------------------------------------------

    engine::communication::Sender::send_string( "Success!", _socket );

    engine::communication::Sender::send_string(
        JSON::stringify( obj ), _socket );

    // ----------------------------------------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::list_pipelines( Poco::Net::StreamSocket* _socket ) const
{
    Poco::JSON::Object obj;

    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Array names;

    for ( const auto& [key, value] : pipelines() )
        {
            names.add( key );
        }

    read_lock.unlock();

    obj.set( "names", names );

    engine::communication::Sender::send_string( "Success!", _socket );

    engine::communication::Sender::send_string(
        JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::list_projects( Poco::Net::StreamSocket* _socket ) const
{
    Poco::JSON::Object obj;

    Poco::JSON::Array project_names;

    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::DirectoryIterator end;

    for ( Poco::DirectoryIterator it( options_.all_projects_directory() );
          it != end;
          ++it )
        {
            if ( it->isDirectory() )
                {
                    project_names.add( it.name() );
                }
        }

    read_lock.unlock();

    obj.set( "projects", project_names );

    engine::communication::Sender::send_string( "Success!", _socket );

    engine::communication::Sender::send_string(
        JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::load_all_pipelines()
{
    // --------------------------------------------------------------------

    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    Poco::DirectoryIterator end;

    // --------------------------------------------------------------------

    for ( Poco::DirectoryIterator it( project_directory_ + "pipelines/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            try
                {
                    const auto pipeline = pipelines::Pipeline(
                        categories().vector(), it->path() + "/" );

                    set_pipeline( it.name(), pipeline );

                    monitor_->send(
                        "postpipeline", pipeline.to_monitor( it.name() ) );
                }
            catch ( std::exception& e )
                {
                    logger().log(
                        "Error loading " + it.name() + ": " + e.what() );
                }
        }

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    // --------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Load data frame

    auto df = FileHandler::load(
        data_frames(),
        categories_,
        join_keys_encoding_,
        project_directory_,
        _name );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // No problems while loading the data frame - we can store it!

    data_frames()[_name] = df;

    data_frames()[_name].create_indices();

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );

    engine::communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::load_pipeline(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto path = project_directory_ + "pipelines/" + _name + "/";

    auto pipeline = pipelines::Pipeline( categories().vector(), path );

    set_pipeline( _name, pipeline );

    monitor_->send( "postpipeline", pipeline.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::refresh( Poco::Net::StreamSocket* _socket ) const
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Object obj;

    obj.set( "categories_", JSON::vector_to_array( *categories().vector() ) );

    obj.set(
        "join_keys_encoding_",
        JSON::vector_to_array( *join_keys_encoding().vector() ) );

    engine::communication::Sender::send_string(
        JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    df.save( project_directory_ + "data/", _name );

    FileHandler::save_encodings(
        project_directory_, categories(), join_keys_encoding() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::save_pipeline(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto pipeline = get_pipeline( _name );

    const auto path = project_directory_ + "pipelines/";

    pipeline.save( path, _name );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::set_project(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // Some methods, particularly the hyperparameter optimization,
    // require us to keep the project fixed while they run.
    std::lock_guard<std::mutex> project_guard( project_mtx() );

    auto absolute_path =
        handlers::FileHandler::create_project_directory( _name, options_ );

    if ( project_directory_ == absolute_path )
        {
            engine::communication::Sender::send_string( "Success!", _socket );
            return;
        }

    multithreading::WriteLock write_lock( read_write_lock_ );

    project_directory_ = absolute_path;

    monitor_->send( "postproject", "{\"name\":\"" + _name + "\"}" );

    clear();

    FileHandler::load_encodings(
        project_directory_, &categories(), &join_keys_encoding() );

    write_lock.unlock();

    load_all_pipelines();

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

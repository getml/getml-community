#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void ProjectManager::add_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    data_frame_manager_->add_data_frame( _name, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager().from_csv( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_s3(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager().from_s3( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_db(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_db( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_json(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_json( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_data_frame_from_query(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    data_frame_manager_->from_query( _name, _cmd, append, _socket );

    multithreading::ReadLock read_lock( read_write_lock_ );

    post( "dataframe", data_frames()[_name].to_monitor() );
}

// ------------------------------------------------------------------------

void ProjectManager::add_hyperopt(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto hyperopt = hyperparam::Hyperopt( _cmd );

    set_hyperopt( _name, hyperopt );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::add_pipeline(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto pipeline = pipelines::Pipeline( _cmd );

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

    post(
        "pipeline", other_pipeline.to_monitor( categories().vector(), _name ) );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::clear()
{
    // --------------------------------
    // Remove from monitor.

    for ( auto& pair : data_frames() )
        {
            remove( "dataframe", pair.first );
        }

    for ( auto& pair : pipelines() )
        {
            remove( "pipeline", pair.first );
        }

    // --------------------------------

    data_frames() = std::map<std::string, engine::containers::DataFrame>();

    hyperopts() = std::map<std::string, engine::hyperparam::Hyperopt>();

    pipelines() = engine::handlers::PipelineManager::PipelineMapType();

    // --------------------------------

    categories().clear();

    join_keys_encoding().clear();

    // --------------------------------

    data_frame_tracker().clear();

    fe_tracker().clear();

    pred_tracker().clear();

    // --------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::delete_data_frame(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory(), _cmd, &data_frames() );

    remove( "dataframe", _name );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_pipeline(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory(), _cmd, &pipelines() );

    remove( "pipeline", _name );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // Some methods, particularly the hyperparameter optimization,
    // require us to keep the project fixed while they run.
    multithreading::WriteLock project_guard( project_lock_ );

    multithreading::WriteLock write_lock( read_write_lock_ );

    if ( _name == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an "
                "empty string!" );
        }

    Poco::File( options_.all_projects_directory() + _name + "/" )
        .remove( true );

    engine::communication::Sender::send_string( "Success!", _socket );

    if ( project_directory() ==
         options_.all_projects_directory() + _name + "/" )
        {
            exit( 0 );
        }
}

// ------------------------------------------------------------------------

void ProjectManager::list_data_frames( Poco::Net::StreamSocket* _socket ) const
{
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

    for ( Poco::DirectoryIterator it( project_directory() + "data/" );
          it != end;
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

void ProjectManager::list_hyperopts( Poco::Net::StreamSocket* _socket ) const
{
    Poco::JSON::Object obj;

    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Array names;

    for ( const auto& [key, value] : hyperopts() )
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

void ProjectManager::load_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    auto df = FileHandler::load(
        data_frames(),
        categories_,
        join_keys_encoding_,
        project_directory(),
        _name );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

    df.create_indices();

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    data_frames()[_name] = df;

    if ( df.build_history() )
        {
            data_frame_tracker().add( df );
        }

    post( "dataframe", df.to_monitor() );

    engine::communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::load_hyperopt(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto path = project_directory() + "hyperopts/" + _name + "/";

    const auto hyperopt = hyperparam::Hyperopt( path );

    set_hyperopt( _name, hyperopt );

    post( "hyperopt", hyperopt.to_monitor() );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::load_pipeline(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto path = project_directory() + "pipelines/" + _name + "/";

    auto pipeline = pipelines::Pipeline(
        path, fe_tracker_, pred_tracker_, preprocessor_tracker_ );

    set_pipeline( _name, pipeline );

    post( "pipeline", pipeline.to_monitor( categories().vector(), _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::post(
    const std::string& _what, const Poco::JSON::Object& _obj ) const
{
    const auto response = monitor().send_tcp(
        "post" + _what, _obj, communication::Monitor::TIMEOUT_ON );

    if ( response != "Success!" )
        {
            throw std::runtime_error( response );
        }
}

// ------------------------------------------------------------------------

void ProjectManager::project_name( Poco::Net::StreamSocket* _socket ) const
{
    communication::Sender::send_string( project_, _socket );
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

void ProjectManager::remove(
    const std::string& _what, const std::string& _name ) const
{
    auto obj = Poco::JSON::Object();

    obj.set( "name", _name );

    const auto response = monitor().send_tcp(
        "remove" + _what, obj, communication::Monitor::TIMEOUT_ON );

    if ( response != "Success!" )
        {
            throw std::runtime_error( response );
        }
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    df.save( options_.temp_dir(), project_directory() + "data/", _name );

    FileHandler::save_encodings(
        project_directory(), categories(), join_keys_encoding() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::save_hyperopt(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto hyperopt = utils::Getter::get( _name, hyperopts() );

    hyperopt.save(
        options_.temp_dir(), project_directory() + "hyperopts/", _name );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::save_pipeline(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto pipeline = get_pipeline( _name );

    const auto path = project_directory() + "pipelines/";

    pipeline.save( options_.temp_dir(), path, _name );

    FileHandler::save_encodings(
        project_directory(), categories(), containers::Encoding() );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::set_project( const std::string& _project )
{
    if ( _project == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an empty string!" );
        }

    handlers::FileHandler::create_project_directory( project_directory() );

    multithreading::WriteLock write_lock( read_write_lock_ );

    clear();

    FileHandler::load_encodings(
        project_directory(), &categories(), &join_keys_encoding() );

    write_lock.unlock();
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

void ProjectManager::add_data_frame(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    data_frame_manager_->add_data_frame( _name, _socket );

    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name ) );
}

// ------------------------------------------------------------------------

void ProjectManager::add_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    auto placeholders_peripheral = JSON::array_to_vector<std::string>(
        _cmd.SQLNET_GET_ARRAY( "peripheral_" ) );

    auto placeholder_population =
        Placeholder( *_cmd.SQLNET_GET_OBJECT( "population_" ) );

    auto model = decisiontrees::DecisionTreeEnsemble(
        categories_, placeholders_peripheral, placeholder_population );

    set_model( _name, model );

    monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::clear( Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    // --------------------------------
    // Remove from monitor

    for ( auto& pair : data_frames() )
        {
            monitor_->send(
                "removedataframe", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : models() )
        {
            monitor_->send(
                "removemodel", "{\"name\":\"" + pair.first + "\"}" );
        }

    // --------------------------------
    // Clear encodings

    data_frames().clear();

    models().clear();

    categories().clear();

    join_keys_encoding().clear();

    // --------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::delete_data_frame(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    engine::FileHandler::remove(
        _name, project_directory_, _cmd, data_frames() );

    monitor_->send( "removedataframe", "{\"name\":\"" + _name + "\"}" );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    engine::FileHandler::remove( _name, project_directory_, _cmd, models() );

    monitor_->send( "removemodel", "{\"name\":\"" + _name + "\"}" );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    if ( _name == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an "
                "empty string!" );
        }

    if ( project_directory_ == options_.all_projects_directory + _name + "/" )
        {
            project_directory_ = "";
        }

    Poco::File( options_.all_projects_directory + _name + "/" ).remove( true );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::load_all_models()
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    Poco::DirectoryIterator end;

    for ( Poco::DirectoryIterator it( project_directory_ + "models/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            auto model = decisiontrees::DecisionTreeEnsemble( categories_ );

            model.load( it->path() + "/" );

            set_model( it.name(), model );

            monitor_->send( "postmodel", model.to_monitor( it.name() ) );
        }
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_frame(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    // --------------------------------------------------------------------

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Load data frame

    auto df = engine::FileHandler::load(
        data_frames(),
        categories_,
        join_keys_encoding_,
        project_directory_,
        _name,
        license_checker() );

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // No problems while loading the data frame - we can store it!

    data_frames()[_name] = df;

    data_frames()[_name].create_indices();

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name ) );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::load_model(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    auto model = decisiontrees::DecisionTreeEnsemble( categories_ );

    model.load( project_directory_ + "models/" + _name + "/" );

    set_model( _name, model );

    monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::refresh( Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Object obj;

    obj.set( "categories_", JSON::vector_to_array( categories().vector() ) );

    obj.set(
        "join_keys_encoding_",
        JSON::vector_to_array( join_keys_encoding().vector() ) );

    engine::Sender::send_string( _socket, JSON::stringify( obj ) );
}

// ------------------------------------------------------------------------

void ProjectManager::save_data_frame(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto& df = engine::Getter::get( data_frames(), _name );

    df.save( project_directory_ + "data/" + _name + "/" );

    FileHandler::save_encodings(
        project_directory_, categories(), join_keys_encoding() );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::save_model(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto model = get_model( _name );

    model.save( project_directory_ + "models/" + _name + "/" );

    // Note that the join keys encoding will be unaffected by models,
    // passing a zero-length-encoding means that it will not be saved.
    FileHandler::save_encodings(
        project_directory_, categories(), containers::Encoding() );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void ProjectManager::set_project(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    auto absolute_path =
        engine::FileHandler::create_project_directory( _name, options_ );

    if ( project_directory_ == absolute_path )
        {
            engine::Sender::send_string( _socket, "Success!" );
            return;
        }

    autosql::multithreading::WriteLock write_lock( read_write_lock_ );

    project_directory_ = absolute_path;

    write_lock.unlock();

    clear( _socket );

    FileHandler::load_encodings(
        project_directory_, categories(), join_keys_encoding() );

    monitor_->send( "postproject", "{\"name\":\"" + _name + "\"}" );

    load_all_models();

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql

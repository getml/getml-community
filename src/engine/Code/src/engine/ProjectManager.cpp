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

    // monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name )
    // );
}

// ------------------------------------------------------------------------

void ProjectManager::add_relboost_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    auto hyperparameters = std::make_shared<relboost::Hyperparameters>(
        JSON::get_object( _cmd, "hyperparameters_" ) );

    auto peripheral = std::make_shared<std::vector<std::string>>(
        JSON::array_to_vector<std::string>(
            JSON::get_array( _cmd, "peripheral_" ) ) );

    auto placeholder = std::make_shared<relboost::ensemble::Placeholder>(
        JSON::get_object( _cmd, "population_" ) );

    auto model = ModelManager::RelboostModelType(
        relboost::ensemble::DecisionTreeEnsemble(
            categories_->vector(), hyperparameters, peripheral, placeholder ) );

    set_relboost_model( _name, model );

    // monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::clear()
{
    // --------------------------------

    multithreading::WriteLock write_lock( read_write_lock_ );

    // --------------------------------
    // Remove from monitor

    /*for ( auto& pair : data_frames() )
        {
            monitor_->send(
                "removedataframe", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : models() )
        {
            monitor_->send(
                "removemodel", "{\"name\":\"" + pair.first + "\"}" );
        }*/

    // --------------------------------
    // Clear encodings

    data_frames().clear();

    // models().clear();

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

    // monitor_->send( "removedataframe", "{\"name\":\"" + _name + "\"}" );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_relboost_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &relboost_models() );

    // monitor_->send( "removemodel", "{\"name\":\"" + _name + "\"}" );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::delete_project(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    if ( _name == "" )
        {
            throw std::invalid_argument(
                "Project name can not be an "
                "empty string!" );
        }

    if ( project_directory_ == options_.all_projects_directory_ + _name + "/" )
        {
            project_directory_ = "";
        }

    Poco::File( options_.all_projects_directory_ + _name + "/" ).remove( true );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::load_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Load data frame

    auto df = FileHandler::load(
        data_frames(),
        categories_,
        join_keys_encoding_,
        project_directory_,
        _name/*,
        license_checker()*/ );

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // No problems while loading the data frame - we can store it!

    data_frames()[_name] = df;

    data_frames()[_name].create_indices();

    // monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name )
    // );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

Poco::JSON::Object ProjectManager::load_json_obj(
    const std::string& _fname ) const
{
    std::ifstream input( _fname );

    std::stringstream json;

    std::string line;

    if ( input.is_open() )
        {
            while ( std::getline( input, line ) )
                {
                    json << line;
                }

            input.close();
        }
    else
        {
            throw std::invalid_argument( "File '" + _fname + "' not found!" );
        }

    return *Poco::JSON::Parser()
                .parse( json.str() )
                .extract<Poco::JSON::Object::Ptr>();
}

// ------------------------------------------------------------------------

void ProjectManager::load_relboost_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto obj =
        load_json_obj( project_directory_ + "models/" + _name + "/Model.json" );

    auto model = ModelManager::RelboostModelType(
        relboost::ensemble::DecisionTreeEnsemble(
            categories_->vector(), obj ) );

    set_relboost_model( _name, model );

    // monitor_->send( "postmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::refresh( Poco::Net::StreamSocket* _socket )
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
    multithreading::ReadLock read_lock( read_write_lock_ );

    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    auto& df = utils::Getter::get( _name, &data_frames() );

    df.save( project_directory_ + "data/" + _name + "/" );

    FileHandler::save_encodings(
        project_directory_, categories(), join_keys_encoding() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::save_relboost_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    auto model = get_relboost_model( _name );

    const auto path = project_directory_ + "models/" + _name + "/";

    auto file = Poco::File( path );

    if ( file.exists() )
        {
            file.remove( true );
        }

    file.createDirectories();

    model.save( path + "Model.json" );

    // Note that the join keys encoding will be unaffected by models,
    // passing a zero-length-encoding means that it will not be saved.
    FileHandler::save_encodings(
        project_directory_, categories(), containers::Encoding() );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::set_project(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    auto absolute_path =
        handlers::FileHandler::create_project_directory( _name, options_ );

    if ( project_directory_ == absolute_path )
        {
            engine::communication::Sender::send_string( "Success!", _socket );
            return;
        }

    multithreading::WriteLock write_lock( read_write_lock_ );

    project_directory_ = absolute_path;

    // monitor_->send( "postproject", "{\"name\":\"" + _name + "\"}" );

    write_lock.unlock();

    clear();

    FileHandler::load_encodings(
        project_directory_, &categories(), &join_keys_encoding() );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void ProjectManager::add_autosql_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto hyperparameters_obj =
        JSON::get_object( _cmd, "hyperparameters_" );

    const auto hyperparameters =
        std::make_shared<autosql::descriptors::Hyperparameters>(
            *hyperparameters_obj );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        JSON::array_to_vector<std::string>(
            JSON::get_array( _cmd, "peripheral_" ) ) );

    const auto placeholder =
        std::make_shared<autosql::decisiontrees::Placeholder>(
            JSON::get_object( _cmd, "population_" ) );

    const auto model = models::AutoSQLModel(
        autosql::ensemble::DecisionTreeEnsemble(
            categories_->vector(), hyperparameters, peripheral, placeholder ),
        *hyperparameters_obj );

    set_autosql_model( _name, model );

    monitor_->send( "postautosqlmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

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
    // --------------------------------------------------------------------
    // Parse the command.

    const auto append = JSON::get_value<bool>( _cmd, "append_" );

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    const auto categoricals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "categoricals_" ) );

    const auto discretes = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "discretes_" ) );

    const auto join_keys = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "join_keys_" ) );

    const auto numericals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "numericals_" ) );

    const auto targets = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "targets_" ) );

    const auto time_stamps = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_stamps_" ) );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    // --------------------------------------------------------------------

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    df.from_csv(
        fnames,
        quotechar,
        sep,
        time_formats,
        categoricals,
        discretes,
        join_keys,
        numericals,
        targets,
        time_stamps );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    df.set_categories( categories_ );

    df.set_join_keys_encoding( join_keys_encoding_ );

    if ( !append || data_frames().find( _name ) == data_frames().end() )
        {
            data_frames()[_name] = df;
        }
    else
        {
            data_frames()[_name].append( df );
        }

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
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

void ProjectManager::add_relboost_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto hyperparameters_obj =
        JSON::get_object( _cmd, "hyperparameters_" );

    const auto hyperparameters =
        std::make_shared<relboost::Hyperparameters>( hyperparameters_obj );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        JSON::array_to_vector<std::string>(
            JSON::get_array( _cmd, "peripheral_" ) ) );

    const auto placeholder = std::make_shared<relboost::ensemble::Placeholder>(
        JSON::get_object( _cmd, "population_" ) );

    const auto model = models::RelboostModel(
        relboost::ensemble::DecisionTreeEnsemble(
            categories_->vector(), hyperparameters, peripheral, placeholder ),
        *hyperparameters_obj );

    set_relboost_model( _name, model );

    monitor_->send( "postrelboostmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::clear()
{
    // --------------------------------

    multithreading::WriteLock write_lock( read_write_lock_ );

    // --------------------------------
    // Remove from monitor.

    for ( auto& pair : data_frames() )
        {
            monitor_->send(
                "removedataframe", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : autosql_models() )
        {
            monitor_->send(
                "removeautosqlmodel", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : relboost_models() )
        {
            monitor_->send(
                "removerelboostmodel", "{\"name\":\"" + pair.first + "\"}" );
        }

    // --------------------------------
    // Remove from engine.

    data_frames().clear();

    relboost_models().clear();

    categories().clear();

    join_keys_encoding().clear();

    // --------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::delete_autosql_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &autosql_models() );

    monitor_->send( "removeautosqlmodel", "{\"name\":\"" + _name + "\"}" );

    communication::Sender::send_string( "Success!", _socket );
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

void ProjectManager::delete_relboost_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &relboost_models() );

    monitor_->send( "removerelboostmodel", "{\"name\":\"" + _name + "\"}" );

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

void ProjectManager::load_all_models()
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    Poco::DirectoryIterator end;

    for ( Poco::DirectoryIterator it( project_directory_ + "autosql-models/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            auto model =
                models::AutoSQLModel( categories().vector(), it->path() + "/" );

            set_autosql_model( it.name(), model );

            monitor_->send( "postautosqlmodel", model.to_monitor( it.name() ) );
        }

    for ( Poco::DirectoryIterator it( project_directory_ + "relboost-models/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            auto model = models::RelboostModel(
                categories().vector(), it->path() + "/" );

            set_relboost_model( it.name(), model );

            monitor_->send(
                "postrelboostmodel", model.to_monitor( it.name() ) );
        }
}

// ------------------------------------------------------------------------

void ProjectManager::load_autosql_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto path = project_directory_ + "autosql-models/" + _name + "/";

    auto model = models::AutoSQLModel( categories().vector(), path );

    set_autosql_model( _name, model );

    monitor_->send( "postautosqlmodel", model.to_monitor( _name ) );

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

void ProjectManager::load_relboost_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto path = project_directory_ + "relboost-models/" + _name + "/";

    auto model = models::RelboostModel( categories().vector(), path );

    set_relboost_model( _name, model );

    monitor_->send( "postrelboostmodel", model.to_monitor( _name ) );

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

void ProjectManager::save_autosql_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto model = get_autosql_model( _name );

    const auto path = project_directory_ + "autosql-models/" + _name + "/";

    model.save( path );

    // Note that the join keys encoding will be unaffected by models,
    // passing a zero-length-encoding means that it will not be saved.
    FileHandler::save_encodings(
        project_directory_, categories(), containers::Encoding() );

    engine::communication::Sender::send_string( "Success!", _socket );
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

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto model = get_relboost_model( _name );

    const auto path = project_directory_ + "relboost-models/" + _name + "/";

    model.save( path );

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

    monitor_->send( "postproject", "{\"name\":\"" + _name + "\"}" );

    write_lock.unlock();

    clear();

    FileHandler::load_encodings(
        project_directory_, &categories(), &join_keys_encoding() );

    load_all_models();

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

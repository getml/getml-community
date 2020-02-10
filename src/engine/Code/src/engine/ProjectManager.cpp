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

void ProjectManager::add_multirel_model(
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
        std::make_shared<multirel::descriptors::Hyperparameters>(
            *hyperparameters_obj );

    const auto peripheral = std::make_shared<std::vector<std::string>>(
        JSON::array_to_vector<std::string>(
            JSON::get_array( _cmd, "peripheral_" ) ) );

    const auto placeholder =
        std::make_shared<multirel::containers::Placeholder>(
            JSON::get_object( _cmd, "placeholder_" ) );

    auto population_schema =
        std::shared_ptr<const multirel::containers::Placeholder>();

    if ( _cmd.has( "population_schema_" ) )
        {
            population_schema =
                std::make_shared<const multirel::containers::Placeholder>(
                    *JSON::get_object( _cmd, "population_schema_" ) );
        }

    auto peripheral_schema =
        std::shared_ptr<const std::vector<multirel::containers::Placeholder>>();

    if ( _cmd.has( "peripheral_schema_" ) )
        {
            std::vector<multirel::containers::Placeholder> peripheral;

            const auto peripheral_arr =
                *JSON::get_array( _cmd, "peripheral_schema_" );

            for ( size_t i = 0; i < peripheral_arr.size(); ++i )
                {
                    const auto ptr = peripheral_arr.getObject(
                        static_cast<unsigned int>( i ) );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "peripheral_schema_, element " +
                                std::to_string( i ) + " is not an Object!" );
                        }

                    peripheral.push_back(
                        multirel::containers::Placeholder( *ptr ) );
                }

            peripheral_schema = std::make_shared<
                const std::vector<multirel::containers::Placeholder>>(
                peripheral );
        }

    const auto model = models::MultirelModel(
        multirel::ensemble::DecisionTreeEnsemble(
            categories_->vector(),
            hyperparameters,
            peripheral,
            placeholder,
            peripheral_schema,
            population_schema ),
        *hyperparameters_obj );

    set_multirel_model( _name, model, false );

    monitor_->send( "postmultirelmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
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

    const auto placeholder =
        std::make_shared<relboost::containers::Placeholder>(
            JSON::get_object( _cmd, "placeholder_" ) );

    auto population_schema =
        std::shared_ptr<const relboost::containers::Placeholder>();

    if ( _cmd.has( "population_schema_" ) )
        {
            population_schema =
                std::make_shared<const relboost::containers::Placeholder>(
                    *JSON::get_object( _cmd, "population_schema_" ) );
        }

    auto peripheral_schema =
        std::shared_ptr<const std::vector<relboost::containers::Placeholder>>();

    if ( _cmd.has( "peripheral_schema_" ) )
        {
            std::vector<relboost::containers::Placeholder> peripheral;

            const auto peripheral_arr =
                *JSON::get_array( _cmd, "peripheral_schema_" );

            for ( size_t i = 0; i < peripheral_arr.size(); ++i )
                {
                    const auto ptr = peripheral_arr.getObject(
                        static_cast<unsigned int>( i ) );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "peripheral_schema_, element " +
                                std::to_string( i ) + " is not an Object!" );
                        }

                    peripheral.push_back(
                        relboost::containers::Placeholder( *ptr ) );
                }

            peripheral_schema = std::make_shared<
                const std::vector<relboost::containers::Placeholder>>(
                peripheral );
        }

    const auto model = models::RelboostModel(
        relboost::ensemble::DecisionTreeEnsemble(
            categories_->vector(),
            hyperparameters,
            peripheral,
            placeholder,
            peripheral_schema,
            population_schema ),
        *hyperparameters_obj );

    set_relboost_model( _name, model, false );

    monitor_->send( "postrelboostmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::copy_multirel_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string other = JSON::get_value<std::string>( _cmd, "other_" );

    auto other_model = get_multirel_model( other );

    set_multirel_model( _name, other_model, false );

    monitor_->send( "postmultirelmodel", other_model.to_monitor( _name ) );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::copy_relboost_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string other = JSON::get_value<std::string>( _cmd, "other_" );

    auto other_model = get_relboost_model( other );

    set_relboost_model( _name, other_model, false );

    monitor_->send( "postrelboostmodel", other_model.to_monitor( _name ) );

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

    for ( auto& pair : multirel_models() )
        {
            monitor_->send(
                "removemultirelmodel", "{\"name\":\"" + pair.first + "\"}" );
        }

    for ( auto& pair : relboost_models() )
        {
            monitor_->send(
                "removerelboostmodel", "{\"name\":\"" + pair.first + "\"}" );
        }

    // --------------------------------
    // Remove from engine.

    multirel_models() = engine::handlers::MultirelModelManager::ModelMapType();

    data_frames() = std::map<std::string, engine::containers::DataFrame>();

    relboost_models() = engine::handlers::RelboostModelManager::ModelMapType();

    categories().clear();

    join_keys_encoding().clear();

    // --------------------------------
}

// ------------------------------------------------------------------------

void ProjectManager::delete_multirel_model(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    FileHandler::remove( _name, project_directory_, _cmd, &multirel_models() );

    monitor_->send( "removemultirelmodel", "{\"name\":\"" + _name + "\"}" );

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
    std::lock_guard<std::mutex> project_guard( project_mtx() );

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

void ProjectManager::get_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    const auto itm = multirel_models().find( _name );

    if ( itm != multirel_models().end() )
        {
            communication::Sender::send_string( "MultirelModel", _socket );
            return;
        }

    // --------------------------------------------------------------------

    const auto itr = relboost_models().find( _name );

    if ( itr != relboost_models().end() )
        {
            communication::Sender::send_string( "RelboostModel", _socket );
            return;
        }

    // --------------------------------------------------------------------

    throw std::invalid_argument(
        "Model named '" + _name + "' does not exist!" );

    // --------------------------------------------------------------------
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

void ProjectManager::list_models( Poco::Net::StreamSocket* _socket ) const
{
    Poco::JSON::Object obj;

    multithreading::ReadLock read_lock( read_write_lock_ );

    Poco::JSON::Array multirel_names;

    for ( const auto& [key, value] : multirel_models() )
        {
            multirel_names.add( key );
        }

    Poco::JSON::Array relboost_names;

    for ( const auto& [key, value] : relboost_models() )
        {
            relboost_names.add( key );
        }

    read_lock.unlock();

    obj.set( "multirel_models", multirel_names );

    obj.set( "relboost_models", relboost_names );

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

    for ( Poco::DirectoryIterator it( options_.all_projects_directory_ );
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

void ProjectManager::load_all_models()
{
    // --------------------------------------------------------------------

    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    Poco::DirectoryIterator end;

    // --------------------------------------------------------------------

    for ( Poco::DirectoryIterator it( project_directory_ + "multirel-models/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            try
                {
                    auto model = models::MultirelModel(
                        categories().vector(), it->path() + "/" );

                    set_multirel_model( it.name(), model, true );

                    monitor_->send(
                        "postmultirelmodel", model.to_monitor( it.name() ) );
                }
            catch ( std::exception& e )
                {
                    logger().log(
                        "Error loading " + it.name() + ": " + e.what() );
                }
        }

    // --------------------------------------------------------------------

    for ( Poco::DirectoryIterator it( project_directory_ + "relboost-models/" );
          it != end;
          ++it )
        {
            if ( !it->isDirectory() )
                {
                    continue;
                }

            try
                {
                    auto model = models::RelboostModel(
                        categories().vector(), it->path() + "/" );

                    set_relboost_model( it.name(), model, true );

                    monitor_->send(
                        "postrelboostmodel", model.to_monitor( it.name() ) );
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

void ProjectManager::load_multirel_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto path = project_directory_ + "multirel-models/" + _name + "/";

    auto model = models::MultirelModel( categories().vector(), path );

    set_multirel_model( _name, model, true );

    monitor_->send( "postmultirelmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
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

void ProjectManager::load_relboost_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    const auto path = project_directory_ + "relboost-models/" + _name + "/";

    auto model = models::RelboostModel( categories().vector(), path );

    set_relboost_model( _name, model, true );

    monitor_->send( "postrelboostmodel", model.to_monitor( _name ) );

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void ProjectManager::purge_model(
    const std::string& _name, const bool _mem_only )
{
    // --------------------------------------------------------------------

    Poco::JSON::Object cmd;

    cmd.set( "mem_only_", _mem_only );

    // --------------------------------------------------------------------

    const auto itm = multirel_models().find( _name );

    if ( itm != multirel_models().end() )
        {
            FileHandler::remove(
                _name, project_directory_, cmd, &multirel_models() );
            monitor_->send(
                "removemultirelmodel", "{\"name\":\"" + _name + "\"}" );
        }

    // --------------------------------------------------------------------

    const auto itr = relboost_models().find( _name );

    if ( itr != relboost_models().end() )
        {
            FileHandler::remove(
                _name, project_directory_, cmd, &relboost_models() );
            monitor_->send(
                "removerelboostmodel", "{\"name\":\"" + _name + "\"}" );
        }

    // --------------------------------------------------------------------
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

void ProjectManager::save_multirel_model(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    if ( project_directory_ == "" )
        {
            throw std::invalid_argument( "You have not set a project!" );
        }

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto model = get_multirel_model( _name );

    const auto path = project_directory_ + "multirel-models/";

    model.save( path, _name );

    // Note that the join keys encoding will be unaffected by models,
    // passing a zero-length-encoding means that it will not be saved.
    FileHandler::save_encodings(
        project_directory_, categories(), containers::Encoding() );

    engine::communication::Sender::send_string( "Success!", _socket );
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

    const auto path = project_directory_ + "relboost-models/";

    model.save( path, _name );

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

    load_all_models();

    engine::communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

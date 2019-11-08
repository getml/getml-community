#include "engine/engine.hpp"

int main( int argc, char *argv[] )
{
    // -------------------------------------------

    const auto options = engine::config::Options::make_options();

    // -------------------------------------------

    Poco::File( options.all_projects_directory_ ).createDirectories();

    // -------------------------------------------

    const auto monitor =
        std::make_shared<const engine::monitoring::Monitor>( options );

    const auto logger =
        std::make_shared<const engine::monitoring::Logger>( monitor );

    // -------------------------------------------
    // Instruct the user to log in and wait for the token.

    std::cout << "getML - Automated Machine Learning and Automated Feature"
              << " Engineering for Relational Data and Time Series."
              << std::endl;

    std::cout << "version: " << GETML_VERSION << std::endl << std::endl;

    std::cout << "Please open a web browser (like Firefox, Chrome or Safari) "
              << "and go to http://localhost:" << options.monitor_.http_port_
              << "/ to log in." << std::endl
              << std::endl;

    std::cout << "An HTTPS server that accepts remote connections has "
                 "been launched "
              << "on port " << options.monitor_.https_port_ << "." << std::endl
              << std::endl;

    std::this_thread::sleep_for( std::chrono::seconds( 1 ) );

    const auto license_checker =
        std::make_shared<engine::licensing::LicenseChecker>(
            logger, monitor, options );

    license_checker->receive_token( "main" );

    // -------------------------------------------
    // Tell the monitor the process ID of the engine.
    // This is necessary so the monitor can check whether the engine is still
    // alive.

    monitor->send( "postpid", engine::Process::get_process_id() );

    // -------------------------------------------
    // Check whether the port is currently occupied

    const auto [status, response] = monitor->send( "checkengineport", "" );

    if ( status != Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK )
        {
            monitor->log( response );
            exit( 0 );
        }

    // -------------------------------------------

    const auto categories = std::make_shared<engine::containers::Encoding>();

    const auto join_keys_encoding =
        std::make_shared<engine::containers::Encoding>();

    // -------------------------------------------

    const auto multirel_models = std::make_shared<
        engine::handlers::MultirelModelManager::ModelMapType>();

    const auto data_frames = std::make_shared<
        std::map<std::string, engine::containers::DataFrame>>();

    const auto relboost_models = std::make_shared<
        engine::handlers::RelboostModelManager::ModelMapType>();

    // -------------------------------------------

    const auto read_write_lock =
        std::make_shared<multithreading::ReadWriteLock>();

    const auto database_manager =
        std::make_shared<engine::handlers::DatabaseManager>( logger, monitor );

    const auto multirel_model_manager =
        std::make_shared<engine::handlers::MultirelModelManager>(
            categories,
            database_manager,
            data_frames,
            join_keys_encoding,
            license_checker,
            logger,
            multirel_models,
            monitor,
            read_write_lock );

    const auto data_frame_manager =
        std::make_shared<engine::handlers::DataFrameManager>(
            categories,
            database_manager,
            data_frames,
            join_keys_encoding,
            license_checker,
            logger,
            monitor,
            read_write_lock );

    const auto relboost_model_manager =
        std::make_shared<engine::handlers::RelboostModelManager>(
            categories,
            database_manager,
            data_frames,
            join_keys_encoding,
            license_checker,
            logger,
            relboost_models,
            monitor,
            read_write_lock );

    const auto project_manager =
        std::make_shared<engine::handlers::ProjectManager>(
            multirel_models,
            categories,
            data_frame_manager,
            data_frames,
            join_keys_encoding,
            license_checker,
            relboost_models,
            monitor,
            options,
            read_write_lock );

    // -------------------------------------------
    // This is where the actual communication begins

    const auto shutdown = std::make_shared<std::atomic<bool>>( false );

    Poco::Net::ServerSocket server_socket(
        static_cast<Poco::UInt16>( options.engine_.port_ ), 64 );

    server_socket.setReceiveTimeout( Poco::Timespan( 600, 0 ) );

    server_socket.setSendTimeout( Poco::Timespan( 10, 0 ) );

    Poco::Net::TCPServer srv(
        new engine::srv::ServerConnectionFactoryImpl(
            multirel_model_manager,
            database_manager,
            data_frame_manager,
            logger,
            relboost_model_manager,
            options,
            project_manager,
            shutdown ),
        server_socket );

    srv.start();

    monitor->log(
        "The getML engine launched successfully on port " +
        std::to_string( options.engine_.port_ ) + "." );

    while ( *shutdown == false )
        {
            std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
        }

    // -------------------------------------------

    std::cout << "getML engine successfully shut down." << std::endl;

    // -------------------------------------------

    return 0;
}

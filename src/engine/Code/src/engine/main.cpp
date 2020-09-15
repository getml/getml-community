#include "engine/engine.hpp"

int main( int argc, char* argv[] )
{
    // -------------------------------------------

    const auto options = engine::config::Options::make_options( argc, argv );

    // -------------------------------------------

    try
        {
            Poco::File( options.all_projects_directory() ).createDirectories();
        }
    catch ( std::exception& e )
        {
            throw std::runtime_error(
                "Unable to create the project directory. Please check the "
                "projectDirectory "
                "you have provided in your config.json." );
        }

    engine::handlers::FileHandler::delete_temp_dir();

    // -------------------------------------------

    const auto monitor =
        std::make_shared<const engine::communication::Monitor>( options );

    const auto logger =
        std::make_shared<const engine::communication::Logger>( monitor );

    // -------------------------------------------
    // Instruct the user to log in and wait for the token.

    std::cout << "getML - Automated Machine Learning and Automated Feature"
              << " Learning for Relational Data and Time Series." << std::endl;

    std::cout << "version: " << GETML_VERSION << std::endl << std::endl;

    std::cout << "Please open a web browser (like Firefox, Chrome or Safari) "
              << "and go to http://localhost:" << options.monitor().http_port()
              << "/ to log in." << std::endl
              << std::endl;

    std::cout << "An HTTPS server that accepts remote connections has "
                 "been launched "
              << "on port " << options.monitor().https_port() << "."
              << std::endl
              << std::endl;

    std::this_thread::sleep_for( std::chrono::seconds( 1 ) );

    const auto license_checker =
        std::make_shared<engine::licensing::LicenseChecker>(
            logger, monitor, options );

    license_checker->receive_token( "main" );

    // -------------------------------------------
    // Check whether the port is currently occupied

    const auto response = monitor->send_tcp( "checkengineport" );

    if ( response != "Success!" )
        {
            monitor->log( response );
            exit( 0 );
        }

    // -------------------------------------------

    const auto categories = std::make_shared<engine::containers::Encoding>();

    const auto join_keys_encoding =
        std::make_shared<engine::containers::Encoding>();

    // -------------------------------------------

    const auto data_frames = std::make_shared<
        std::map<std::string, engine::containers::DataFrame>>();

    const auto hyperopts =
        std::make_shared<std::map<std::string, engine::hyperparam::Hyperopt>>();

    const auto pipelines =
        std::make_shared<engine::handlers::PipelineManager::PipelineMapType>();

    // -------------------------------------------

    const auto data_frame_tracker =
        std::make_shared<engine::dependency::DataFrameTracker>( data_frames );

    const auto fe_tracker = std::make_shared<engine::dependency::FETracker>();

    const auto pred_tracker =
        std::make_shared<engine::dependency::PredTracker>();

    // -------------------------------------------

    const auto project_mtx = std::make_shared<std::mutex>();

    const auto read_write_lock =
        std::make_shared<multithreading::ReadWriteLock>();

    const auto database_manager =
        std::make_shared<engine::handlers::DatabaseManager>( logger, monitor );

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

    const auto hyperopt_manager =
        std::make_shared<engine::handlers::HyperoptManager>(
            hyperopts, monitor, project_mtx, read_write_lock );

    const auto pipeline_manager =
        std::make_shared<engine::handlers::PipelineManager>(
            categories,
            database_manager,
            data_frames,
            data_frame_tracker,
            fe_tracker,
            join_keys_encoding,
            license_checker,
            logger,
            monitor,
            pipelines,
            pred_tracker,
            project_mtx,
            read_write_lock );

    const auto project_manager =
        std::make_shared<engine::handlers::ProjectManager>(
            categories,
            data_frame_manager,
            data_frames,
            data_frame_tracker,
            fe_tracker,
            join_keys_encoding,
            hyperopts,
            license_checker,
            logger,
            monitor,
            options,
            pipelines,
            pred_tracker,
            project_mtx,
            read_write_lock );

    // -------------------------------------------
    // This is where the actual communication begins

    const auto shutdown = std::make_shared<std::atomic<bool>>( false );

    Poco::Net::ServerSocket server_socket(
        static_cast<Poco::UInt16>( options.engine().port() ), 64 );

    server_socket.setReceiveTimeout( Poco::Timespan( 600, 0 ) );

    server_socket.setSendTimeout( Poco::Timespan( 10, 0 ) );

    Poco::Net::TCPServer srv(
        new engine::srv::ServerConnectionFactoryImpl(
            database_manager,
            data_frame_manager,
            hyperopt_manager,
            logger,
            options,
            pipeline_manager,
            project_manager,
            shutdown ),
        server_socket );

    srv.start();

    monitor->log(
        "The getML engine launched successfully on port " +
        std::to_string( options.engine().port() ) + "." );

    while ( *shutdown == false )
        {
            std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
        }

    // -------------------------------------------

    engine::handlers::FileHandler::delete_temp_dir();

    // -------------------------------------------

    std::cout << "getML engine successfully shut down." << std::endl;

    // -------------------------------------------

    return 0;
}

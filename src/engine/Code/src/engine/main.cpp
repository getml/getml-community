#include "engine/engine.hpp"

int main( int argc, char *argv[] )
{
    // -------------------------------------------

    const auto options = engine::config::Options::make_options();

    // -------------------------------------------

    Poco::File( options.all_projects_directory_ ).createDirectories();

    // -------------------------------------------

    // const auto monitor = std::make_shared<const logging::Monitor>( options );

    const auto logger =
        std::make_shared<const engine::logging::Logger>( /*monitor*/ );

    /*  const auto license_checker =
          std::make_shared<engine::licensing::LicenseChecker>(
              logger, monitor, options );*/

    // -------------------------------------------

    const auto categories = std::make_shared<engine::containers::Encoding>();

    const auto join_keys_encoding =
        std::make_shared<engine::containers::Encoding>();

    // -------------------------------------------

    const auto data_frames = std::make_shared<
        std::map<std::string, engine::containers::DataFrame>>();

    const auto relboost_models = std::make_shared<
        engine::handlers::ProjectManager::RelboostModelMapType>();

    // -------------------------------------------

    const auto read_write_lock =
        std::make_shared<multithreading::ReadWriteLock>();

    const auto data_frame_manager =
        std::make_shared<engine::handlers::DataFrameManager>(
            categories,
            data_frames,
            join_keys_encoding,
            // license_checker,
            logger,
            // monitor,
            read_write_lock );

    const auto relboost_model_manager =
        std::make_shared<engine::handlers::RelboostModelManager>(
            categories,
            data_frames,
            join_keys_encoding,
            // license_checker,
            logger,
            relboost_models,
            // monitor,
            read_write_lock );

    const auto project_manager =
        std::make_shared<engine::handlers::ProjectManager>(
            categories,
            data_frame_manager,
            data_frames,
            join_keys_encoding,
            // license_checker,
            relboost_models,
            // monitor,
            options,
            read_write_lock );

    // -------------------------------------------
    // Print some output

    /*engine::Printer::print_license();

    engine::Printer::print_start_message( options );

    if ( !monitor->get_start_message() )
        {
            exit( 0 );
        }

    license_checker->receive_token();*/

    // -------------------------------------------
    // Tell the AutoSQL Monitor the process ID of the engine.
    // This is necessary for some system statistics.

    // monitor->send( "postpid", engine::Process::get_process_id() );

    // -------------------------------------------
    // This is where the actual communication begins

    const auto shutdown = std::make_shared<std::atomic<bool>>( false );

    Poco::Net::ServerSocket server_socket( options.engine_.port_, 64 );

    server_socket.setReceiveTimeout( Poco::Timespan( 600, 0 ) );

    server_socket.setSendTimeout( Poco::Timespan( 10, 0 ) );

    Poco::Net::TCPServer srv(
        new engine::srv::ServerConnectionFactoryImpl(
            data_frame_manager,
            logger,
            relboost_model_manager,
            // monitor,
            options,
            project_manager,
            shutdown ),
        server_socket );

    srv.start();

    while ( *shutdown == false )
        {
            std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
        }

    // -------------------------------------------

    std::cout << "Relboost engine successfully shut down." << std::endl;

    // -------------------------------------------

    return 0;
}

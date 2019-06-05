#include "engine/engine.hpp"

using namespace autosql;

int main( int argc, char *argv[] )
{
    // -------------------------------------------

    const auto options = config::Options::make_options();

    // -------------------------------------------

    Poco::File( options.all_projects_directory ).createDirectories();

    // -------------------------------------------

    const auto monitor = std::make_shared<const logging::Monitor>( options );

    const auto logger = std::make_shared<const logging::Logger>( monitor );

    const auto license_checker =
        std::make_shared<engine::licensing::LicenseChecker>(
            logger, monitor, options );

    // -------------------------------------------

    const auto categories = std::make_shared<containers::Encoding>();

    const auto join_keys_encoding = std::make_shared<containers::Encoding>();

    // -------------------------------------------

    const auto data_frames =
        std::make_shared<std::map<std::string, containers::DataFrame>>();

    const auto models = std::make_shared<SQLNET_MODEL_MAP>();

    // -------------------------------------------

    const auto read_write_lock =
        std::make_shared<autosql::multithreading::ReadWriteLock>();

    const auto data_frame_manager = std::make_shared<engine::DataFrameManager>(
        categories,
        data_frames,
        join_keys_encoding,
        license_checker,
        logger,
        monitor,
        read_write_lock );

    const auto model_manager = std::make_shared<engine::ModelManager>(
        categories,
        data_frames,
        join_keys_encoding,
        license_checker,
        logger,
        models,
        monitor,
        read_write_lock );

    const auto project_manager = std::make_shared<engine::ProjectManager>(
        categories,
        data_frame_manager,
        data_frames,
        join_keys_encoding,
        license_checker,
        models,
        monitor,
        options,
        read_write_lock );

    // -------------------------------------------
    // Print some output

    engine::Printer::print_license();

    engine::Printer::print_start_message( options );

    if ( !monitor->get_start_message() )
        {
            exit( 0 );
        }

    // -------------------------------------------
    // Tell the AutoSQL Monitor the process ID of the engine.
    // This is necessary for some system statistics.

    monitor->send( "postpid", engine::Process::get_process_id() );

    // -------------------------------------------
    // This is where the actual communication begins

    const auto shutdown = std::make_shared<std::atomic<bool>>( false );

    Poco::Net::ServerSocket server_socket( options.engine.port, 64 );

    server_socket.setReceiveTimeout( Poco::Timespan( 600, 0 ) );

    server_socket.setSendTimeout( Poco::Timespan( 10, 0 ) );

    Poco::Net::TCPServer srv(
        new engine::ServerConnectionFactoryImpl(
            data_frame_manager,
            license_checker,
            logger,
            model_manager,
            monitor,
            options,
            project_manager,
            shutdown ),
        server_socket );

    srv.start();

    while ( *shutdown == false )
        {
            if ( !license_checker->has_active_token() )
                {
                    license_checker->receive_token();
                }

            std::this_thread::sleep_for( std::chrono::seconds( 1 ) );
        }

    // -------------------------------------------

    std::cout << "AutoSQL engine successfully shut down." << std::endl;

    // -------------------------------------------

    return 0;
}

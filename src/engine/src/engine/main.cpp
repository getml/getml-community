#include <Poco/Net/TCPServer.h>

#include <chrono>
#include <csignal>

#ifdef GETML_PROFILING
#include <gperftools/profiler.h>
#endif

#include "engine/engine.hpp"

const auto shutdown_flag = std::make_shared<std::atomic<bool>>(false);

void handle_signal(int signum) { *shutdown_flag = true; }

int main(int argc, char* argv[]) {
  signal(SIGINT, handle_signal);
#ifdef GETML_PROFILING
  // The resulting profile will be
  // located in $HOME/.getml/getml-.../bin
  //
  // To analyze the profile, run the following:
  // google-pprof --svg getml-... getml_profile.log getml_profile.svg
  ProfilerStart("getml_profile.log");
#endif

  const auto options = engine::config::Options::make_options(argc, argv);

  try {
    engine::handlers::FileHandler::create_project_directory(
        options.project_directory());
  } catch (std::exception& e) {
    throw std::runtime_error(
        "Unable to create the project directory. Please check the "
        "projectDirectory "
        "you have provided in your config.json.");
  }

  engine::handlers::FileHandler::delete_temp_dir(options.temp_dir());

  const auto monitor =
      std::make_shared<const engine::communication::Monitor>(options);

  const auto logger =
      std::make_shared<const engine::communication::Logger>(monitor);

  const auto license_checker =
      std::make_shared<engine::licensing::LicenseChecker>(logger, monitor,
                                                          options);

  license_checker->receive_token("main");

  if (!license_checker->has_active_token()) {
    logger->log("License server authentication failed.");
    return 0;
  }

  const auto pool = options.make_pool();

  const auto categories = std::make_shared<engine::containers::Encoding>(pool);

  const auto join_keys_encoding =
      std::make_shared<engine::containers::Encoding>(pool);

  const auto data_frames =
      std::make_shared<std::map<std::string, engine::containers::DataFrame>>();

  const auto hyperopts =
      std::make_shared<std::map<std::string, engine::hyperparam::Hyperopt>>();

  const auto pipelines =
      std::make_shared<engine::handlers::PipelineManager::PipelineMapType>();

  const auto data_frame_tracker =
      std::make_shared<engine::dependency::DataFrameTracker>(data_frames);

  const auto preprocessor_tracker =
      std::make_shared<engine::dependency::PreprocessorTracker>();

  const auto fe_tracker = std::make_shared<engine::dependency::FETracker>();

  const auto pred_tracker = std::make_shared<engine::dependency::PredTracker>();

  const auto warning_tracker =
      std::make_shared<engine::dependency::WarningTracker>();

  const auto project_lock = std::make_shared<multithreading::ReadWriteLock>();

  const auto read_write_lock =
      std::make_shared<multithreading::ReadWriteLock>();

  const auto database_manager =
      std::make_shared<engine::handlers::DatabaseManager>(logger, monitor,
                                                          options);

  const auto data_frame_manager =
      std::make_shared<engine::handlers::DataFrameManager>(
          categories, database_manager, data_frames, join_keys_encoding,
          license_checker, logger, monitor, options, read_write_lock);

  const auto hyperopt_manager =
      std::make_shared<engine::handlers::HyperoptManager>(
          hyperopts, monitor, project_lock, read_write_lock);

  const auto pipeline_manager =
      std::make_shared<engine::handlers::PipelineManager>(
          categories, database_manager, data_frames, data_frame_tracker,
          fe_tracker, join_keys_encoding, license_checker, logger, monitor,
          options, pipelines, pred_tracker, preprocessor_tracker,
          read_write_lock, warning_tracker);

  const auto project_manager =
      std::make_shared<engine::handlers::ProjectManager>(
          categories, data_frame_manager, data_frames, data_frame_tracker,
          fe_tracker, join_keys_encoding, hyperopts, license_checker, logger,
          monitor, options, pipelines, pred_tracker, preprocessor_tracker,
          options.engine().project_, project_lock, read_write_lock);

  Poco::Net::ServerSocket server_socket(
      static_cast<Poco::UInt16>(options.engine().port()), 64);

  server_socket.setReceiveTimeout(Poco::Timespan(600, 0));

  server_socket.setSendTimeout(Poco::Timespan(10, 0));

  Poco::Net::TCPServer srv(
      new engine::srv::ServerConnectionFactoryImpl(
          database_manager, data_frame_manager, hyperopt_manager, logger,
          options, pipeline_manager, project_manager, shutdown_flag),
      server_socket);

  srv.start();

  logger->log("The getML engine launched successfully on port " +
              std::to_string(options.engine().port()) + ".");

  while (!(*shutdown_flag)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
  }

#ifdef GETML_PROFILING
  ProfilerStop();
#endif

  engine::handlers::FileHandler::delete_temp_dir(options.temp_dir());

  return 0;
}

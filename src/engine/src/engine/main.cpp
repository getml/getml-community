#include <Poco/Net/TCPServer.h>

#include <chrono>
#include <csignal>

#ifdef GETML_PROFILING
#include <gperftools/profiler.h>
#endif

#include "engine/engine.hpp"

const auto shutdown_flag = fct::Ref<std::atomic<bool>>::make(false);

void handle_signal(int signum) {
#ifdef GETML_PROFILING
  ProfilerStop();
#endif

  *shutdown_flag = true;
}

int main(int argc, char* argv[]) {
  signal(SIGINT, handle_signal);
#ifdef GETML_PROFILING
  // The resulting profile will be
  // located in $HOME/.getml/getml-.../bin
  //
  // To analyze the profile, run the following:
  // google-pprof --pdf getml-... getml_profile.log getml_profile.pdf
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
      fct::Ref<const engine::communication::Monitor>::make(options);

  const auto logger = fct::Ref<const engine::communication::Logger>::make(
      monitor.ptr());  // TODO

  const auto license_checker =
      fct::Ref<engine::licensing::LicenseChecker>::make(logger, monitor,
                                                        options);

  license_checker->receive_token("main");

  if (!license_checker->has_active_token()) {
    logger->log("License server authentication failed.");
    return 0;
  }

  const auto pool = options.make_pool();

  const auto categories = fct::Ref<engine::containers::Encoding>::make(pool);

  const auto join_keys_encoding =
      fct::Ref<engine::containers::Encoding>::make(pool);

  const auto data_frames =
      fct::Ref<std::map<std::string, engine::containers::DataFrame>>::make();

  const auto hyperopts =
      fct::Ref<std::map<std::string, engine::hyperparam::Hyperopt>>::make();

  const auto pipelines =
      fct::Ref<engine::handlers::PipelineManager::PipelineMapType>::make();

  const auto data_frame_tracker =
      fct::Ref<engine::dependency::DataFrameTracker>::make(
          data_frames.ptr());  // TODO

  const auto preprocessor_tracker =
      fct::Ref<engine::dependency::PreprocessorTracker>::make();

  const auto fe_tracker = fct::Ref<engine::dependency::FETracker>::make();

  const auto pred_tracker = fct::Ref<engine::dependency::PredTracker>::make();

  const auto warning_tracker =
      fct::Ref<engine::dependency::WarningTracker>::make();

  const auto project_lock = fct::Ref<multithreading::ReadWriteLock>::make();

  const auto read_write_lock = fct::Ref<multithreading::ReadWriteLock>::make();

  const auto database_manager =
      fct::Ref<engine::handlers::DatabaseManager>::make(logger, monitor,
                                                        options);

  const auto data_frame_manager =
      fct::Ref<engine::handlers::DataFrameManager>::make(
          categories, database_manager, data_frames, join_keys_encoding,
          license_checker, logger, monitor, options, read_write_lock);

  const auto hyperopt_manager =
      fct::Ref<engine::handlers::HyperoptManager>::make(
          hyperopts, monitor, project_lock, read_write_lock);

  const auto pipeline_manager =
      fct::Ref<engine::handlers::PipelineManager>::make(
          categories, database_manager, data_frames, data_frame_tracker,
          fe_tracker, join_keys_encoding, license_checker, logger, monitor,
          options, pipelines, pred_tracker, preprocessor_tracker,
          read_write_lock, warning_tracker);

  const auto project_manager = fct::Ref<engine::handlers::ProjectManager>::make(
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

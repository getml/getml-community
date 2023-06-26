// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <Poco/Net/TCPServer.h>

#include <chrono>
#include <csignal>
#include <stdexcept>

#include "engine/handlers/DataFrameManagerParams.hpp"

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

  try {
    engine::handlers::FileHandler::delete_temp_dir(options.temp_dir());
  } catch (std::exception& e) {
  }

  const auto monitor = fct::Ref<const communication::Monitor>::make(options);

  const auto logger =
      fct::Ref<const communication::Logger>::make(monitor.ptr());

  const auto pool = options.make_pool();

  const auto categories = fct::Ref<containers::Encoding>::make(pool);

  const auto join_keys_encoding = fct::Ref<containers::Encoding>::make(pool);

  const auto data_frames =
      fct::Ref<std::map<std::string, containers::DataFrame>>::make();

  const auto pipelines =
      fct::Ref<engine::handlers::PipelineManager::PipelineMapType>::make();

  const auto data_frame_tracker =
      fct::Ref<engine::dependency::DataFrameTracker>::make(data_frames);

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

  const auto data_params =
      fct::Ref<engine::handlers::DataFrameManagerParams>::make(
          engine::handlers::DataFrameManagerParams{
              .categories_ = categories,
              .database_manager_ = database_manager,
              .data_frames_ = data_frames,
              .join_keys_encoding_ = join_keys_encoding,
              .logger_ = logger,
              .monitor_ = monitor,
              .options_ = options,
              .read_write_lock_ = read_write_lock});

  const auto data_frame_manager =
      fct::Ref<engine::handlers::DataFrameManager>::make(*data_params);

  const auto pipeline_manager_params = engine::handlers::PipelineManagerParams{
      .categories_ = categories,
      .database_manager_ = database_manager,
      .data_frames_ = data_frames,
      .data_frame_tracker_ = data_frame_tracker,
      .fe_tracker_ = fe_tracker,
      .join_keys_encoding_ = join_keys_encoding,
      .logger_ = logger,
      .monitor_ = monitor,
      .options_ = options,
      .pipelines_ = pipelines,
      .pred_tracker_ = pred_tracker,
      .preprocessor_tracker_ = preprocessor_tracker,
      .read_write_lock_ = read_write_lock,
      .warning_tracker_ = warning_tracker};

  const auto pipeline_manager =
      fct::Ref<engine::handlers::PipelineManager>::make(
          pipeline_manager_params);

  const auto project_manager_params = engine::handlers::ProjectManagerParams{
      .categories_ = categories,
      .database_manager_ = database_manager,
      .data_frame_manager_ = data_frame_manager,
      .data_frames_ = data_frames,
      .data_frame_tracker_ = data_frame_tracker,
      .fe_tracker_ = fe_tracker,
      .join_keys_encoding_ = join_keys_encoding,
      .logger_ = logger,
      .monitor_ = monitor,
      .options_ = options,
      .pipelines_ = pipelines,
      .pred_tracker_ = pred_tracker,
      .preprocessor_tracker_ = preprocessor_tracker,
      .project_ = options.engine().project_,
      .project_lock_ = project_lock,
      .read_write_lock_ = read_write_lock};

  const auto project_manager =
      fct::Ref<engine::handlers::ProjectManager>::make(project_manager_params);

  Poco::Net::ServerSocket server_socket(
      static_cast<Poco::UInt16>(options.engine().port()), 64);

  server_socket.setReceiveTimeout(Poco::Timespan(600, 0));

  server_socket.setSendTimeout(Poco::Timespan(10, 0));

  Poco::Net::TCPServer srv(
      new engine::srv::ServerConnectionFactoryImpl(
          database_manager, data_params, logger, options, pipeline_manager,
          project_manager, shutdown_flag),
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

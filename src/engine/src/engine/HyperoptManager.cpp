#include "engine/handlers/HyperoptManager.hpp"

namespace engine {
namespace handlers {
// ------------------------------------------------------------------------

void HyperoptManager::launch(const std::string& _name,
                             const Poco::JSON::Object& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------
  // The project guard will prevent any attempts to
  // change or delete the project while the hyperparameter
  // optimization is running.

  multithreading::ReadLock project_guard(project_lock_);

  // -------------------------------------------------------

  const auto population_training_df =
      JSON::get_object(_cmd, "population_training_df_");

  const auto population_validation_df =
      JSON::get_object(_cmd, "population_validation_df_");

  const auto peripheral_dfs = JSON::get_array(_cmd, "peripheral_dfs_");

  // -------------------------------------------------------

  const auto hyperopt = get_hyperopt(_name);

  auto cmd = hyperopt.obj();

  cmd.set("population_training_df_", population_training_df);

  cmd.set("population_validation_df_", population_validation_df);

  cmd.set("peripheral_dfs_", peripheral_dfs);

  // -------------------------------------------------------

  const auto monitor_socket =
      monitor().connect(communication::Monitor::TIMEOUT_OFF);

  const auto cmd_str = monitor().make_cmd("launchhyperopt", cmd);

  communication::Sender::send_string(cmd_str, monitor_socket.get());

  // -------------------------------------------------------

  handle_logging(monitor_socket, _socket);

  // -------------------------------------------------------

  const auto evaluations_str =
      communication::Receiver::recv_string(monitor_socket.get());

  // -------------------------------------------------------

  Poco::JSON::Parser parser;

  const auto evaluations =
      parser.parse(evaluations_str).extract<Poco::JSON::Array::Ptr>();

  auto obj = hyperopt.obj();

  obj.set("evaluations_", evaluations);

  // -------------------------------------------------------

  const auto hyp = hyperparam::Hyperopt(obj);

  post_hyperopt(hyp.to_monitor());

  multithreading::WriteLock write_lock(read_write_lock_);

  hyperopts().insert_or_assign(_name, hyp);

  communication::Sender::send_string("Success!", _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void HyperoptManager::handle_logging(
    const std::shared_ptr<Poco::Net::StreamSocket>& _monitor_socket,
    Poco::Net::StreamSocket* _socket) const {
  assert_true(_monitor_socket);

  while (true) {
    const auto msg =
        communication::Receiver::recv_string(_monitor_socket.get());

    if (msg.size() > 4 && msg.substr(0, 5) == "log: ") {
      communication::Sender::send_string(msg, _socket);
    } else if (msg == "Success!") {
      break;
    } else {
      throw std::runtime_error(msg);
    }
  }
}

// ------------------------------------------------------------------------

void HyperoptManager::refresh(const std::string& _name,
                              Poco::Net::StreamSocket* _socket) {
  const auto hyperopt = get_hyperopt(_name);

  const auto obj = hyperopt.obj();

  communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void HyperoptManager::post_hyperopt(const Poco::JSON::Object& _obj) {
  const auto response = monitor().send_tcp("posthyperopt", _obj,
                                           communication::Monitor::TIMEOUT_ON);

  if (response != "Success!") {
    throw std::runtime_error(response);
  }
}

// ------------------------------------------------------------------------

void HyperoptManager::tune(const std::string& _name,
                           const Poco::JSON::Object& _cmd,
                           Poco::Net::StreamSocket* _socket) {
  // -------------------------------------------------------
  // The project guard will prevent any attempts to
  // change or delete the project while the hyperparameter
  // optimization is running.

  multithreading::ReadLock project_guard(project_lock_);

  // -------------------------------------------------------

  const auto monitor_socket =
      monitor().connect(communication::Monitor::TIMEOUT_OFF);

  const auto cmd_str = monitor().make_cmd("tune", _cmd);

  communication::Sender::send_string(cmd_str, monitor_socket.get());

  // -------------------------------------------------------

  handle_logging(monitor_socket, _socket);

  // -------------------------------------------------------

  const auto best_pipeline_name =
      communication::Receiver::recv_string(monitor_socket.get());

  // -------------------------------------------------------

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(best_pipeline_name, _socket);

  // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

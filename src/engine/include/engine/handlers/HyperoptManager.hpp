#ifndef ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_
#define ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_

// ------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ------------------------------------------------------------------------

#include <map>
#include <memory>
#include <string>

// ------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "multithreading/multithreading.hpp"

// ------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/hyperparam/hyperparam.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

class HyperoptManager {
 public:
  HyperoptManager(
      const fct::Ref<std::map<std::string, hyperparam::Hyperopt>>& _hyperopts,
      const fct::Ref<const communication::Monitor>& _monitor,
      const fct::Ref<multithreading::ReadWriteLock>& _project_lock,
      const fct::Ref<multithreading::ReadWriteLock>& _read_write_lock)
      : hyperopts_(_hyperopts),
        monitor_(_monitor),
        project_lock_(_project_lock),
        read_write_lock_(_read_write_lock) {}

  ~HyperoptManager() = default;

 public:
  /// Launches a hyperparameter optimization.
  void launch(const std::string& _name, const Poco::JSON::Object& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Sends a JSON string representation of the hyperparameter optimization to
  /// the client.
  void refresh(const std::string& _name, Poco::Net::StreamSocket* _socket);

  /// Launches a hyperparameter tuning routine.
  void tune(const std::string& _name, const Poco::JSON::Object& _cmd,
            Poco::Net::StreamSocket* _socket);

 private:
  /// Handles the logging for the hyperparameter optimization.
  void handle_logging(const fct::Ref<Poco::Net::StreamSocket>& _monitor_socket,
                      Poco::Net::StreamSocket* _socket) const;

  /// Sends the hyperopt object to the monitor.
  void post_hyperopt(const Poco::JSON::Object& _obj);

 private:
  /// Trivial (private) accessor
  std::map<std::string, hyperparam::Hyperopt>& hyperopts() {
    return *hyperopts_;
  }

  /// Trivial (private) getter
  hyperparam::Hyperopt get_hyperopt(const std::string& _name) {
    multithreading::ReadLock read_lock(read_write_lock_);
    return utils::Getter::get(_name, hyperopts());
  }

  /// Trivial (private) accessor
  const communication::Monitor& monitor() const { return *monitor_; }

  /// Trivial (private) accessor
  multithreading::ReadWriteLock& project_lock() { return *project_lock_; }

 private:
  /// The Hyperopts currently held in memory
  const fct::Ref<std::map<std::string, hyperparam::Hyperopt>> hyperopts_;

  /// For communication with the monitor
  const fct::Ref<const communication::Monitor> monitor_;

  /// It is sometimes necessary to prevent us from changing the project.
  const fct::Ref<multithreading::ReadWriteLock> project_lock_;

  /// For coordinating the read and write process of the data
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

// ------------------------------------------------------------------------

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_HYPEROPTMANAGER_HPP_


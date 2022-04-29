#ifndef ENGINE_COMMUNICATION_WARNER_HPP_
#define ENGINE_COMMUNICATION_WARNER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/JSON.hpp"

// ----------------------------------------------------------------------------

#include "fct/Ref.hpp"

// ----------------------------------------------------------------------------

#include "engine/communication/Sender.hpp"
#include "engine/communication/Warnings.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace communication {

class Warner {
 public:
  Warner() {}

  ~Warner() = default;

 public:
  /// Adds a new warning to the list of warnings.
  void add(const std::string& _warning) { warnings_.push_back(_warning); }

  /// Sends all warnings to the socket.
  void send(Poco::Net::StreamSocket* _socket) const {
    Poco::JSON::Object obj;
    obj.set("warnings_", JSON::vector_to_array_ptr(warnings_));
    const auto json_str = JSON::stringify(obj);
    Sender::send_string(json_str, _socket);
  }

  /// Trivial (const) getter
  const std::vector<std::string>& warnings() const { return warnings_; }

  /// Generates a warnings object that can be used for dependency tracking.
  const fct::Ref<Warnings> to_warnings_obj(
      Poco::JSON::Object::Ptr _fingerprints) const {
    const auto ptr =
        std::make_shared<const std::vector<std::string>>(warnings_);
    return fct::Ref<Warnings>::make(_fingerprints, ptr);
  }

 private:
  /// The list of warnings to send.
  std::vector<std::string> warnings_;
};

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_WARNER_HPP_

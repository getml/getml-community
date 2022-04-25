#include "engine/communication/Receiver.hpp"

namespace engine {
namespace communication {

Poco::JSON::Object Receiver::recv_cmd(
    const fct::Ref<const communication::Logger> &_logger,
    Poco::Net::StreamSocket *_socket) {
  const auto str = Receiver::recv_string(_socket);

  std::stringstream cmd_log;
  cmd_log << "Command sent by " << _socket->peerAddress().toString() << ":"
          << std::endl
          << str;
  _logger->log(cmd_log.str());

  Poco::JSON::Parser parser;
  const auto obj = parser.parse(str).extract<Poco::JSON::Object::Ptr>();
  return *obj;
}

// -----------------------------------------------------------------------------

std::string Receiver::recv_string(Poco::Net::StreamSocket *_socket) {
  std::vector<Int> str_length(1);

  Receiver::recv<Int>(sizeof(Int), _socket, str_length.data());

  std::string str(str_length[0], '0');

  const auto str_length_long = static_cast<ULong>(str_length[0]);

  Receiver::recv<char>(str_length_long,  // sizeof(char) = 1 by definition
                       _socket, &str[0]);

  return str;
}

// -----------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

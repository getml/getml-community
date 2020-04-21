#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void HyperoptManager::launch(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // The project guard will prevent any attempts to
    // change or delete the project while the hyperparameter
    // optimization is running.

    std::lock_guard<std::mutex> project_guard( project_mtx() );

    // -------------------------------------------------------
    // Receive the complete command and send to engine.

    auto& hyperopt = utils::Getter::get( _name, &hyperopts() );

    const auto [status, response] =
        monitor_->send( "launchhyperopt", hyperopt.cmd_str() );

    if ( status == Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK )
        {
            communication::Sender::send_string( "Success!", _socket );
        }
    else
        {
            communication::Sender::send_string( response, _socket );
        }

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

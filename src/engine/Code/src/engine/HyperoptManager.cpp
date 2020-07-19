#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void HyperoptManager::launch(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // -------------------------------------------------------
    // The project guard will prevent any attempts to
    // change or delete the project while the hyperparameter
    // optimization is running.

    std::lock_guard<std::mutex> project_guard( project_mtx() );

    // -------------------------------------------------------

    const auto population_training_name =
        JSON::get_value<std::string>( _cmd, "population_training_name_" );

    const auto population_validation_name =
        JSON::get_value<std::string>( _cmd, "population_validation_name_" );

    const auto peripheral_names = JSON::get_array( _cmd, "peripheral_names_" );

    // -------------------------------------------------------

    const auto hyperopt = get_hyperopt( _name );

    auto cmd = hyperopt.obj();

    cmd.set( "population_training_name_", population_training_name );

    cmd.set( "population_validation_name_", population_validation_name );

    cmd.set( "peripheral_names_", peripheral_names );

    // -------------------------------------------------------

    const auto monitor_socket = monitor().connect();

    const auto cmd_str = monitor().make_cmd( "launchhyperopt", cmd );

    communication::Sender::send_string( cmd_str, monitor_socket.get() );

    // -------------------------------------------------------

    while ( true )
        {
            const auto msg =
                communication::Receiver::recv_string( monitor_socket.get() );

            if ( msg.size() > 4 && msg.substr( 0, 5 ) == "log: " )
                {
                    communication::Sender::send_string( msg, _socket );
                }
            else if ( msg == "Success!" )
                {
                    break;
                }
            else
                {
                    throw std::runtime_error( msg );
                }
        }

    // -------------------------------------------------------

    const auto evaluations_str =
        communication::Receiver::recv_string( monitor_socket.get() );

    // -------------------------------------------------------

    Poco::JSON::Parser parser;

    const auto evaluations =
        parser.parse( evaluations_str ).extract<Poco::JSON::Array::Ptr>();

    auto obj = hyperopt.obj();

    obj.set( "evaluations_", evaluations );

    // -------------------------------------------------------

    multithreading::WriteLock write_lock( read_write_lock_ );

    hyperopts().insert_or_assign( _name, hyperparam::Hyperopt( obj ) );

    communication::Sender::send_string( "Success!", _socket );

    // -------------------------------------------------------
}

// ------------------------------------------------------------------------

void HyperoptManager::refresh(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto hyperopt = get_hyperopt( _name );

    const auto obj = hyperopt.obj();

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

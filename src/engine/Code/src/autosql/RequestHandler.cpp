#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

void RequestHandler::run()
{
    try
        {
            // ---------------------------------------------------------------
            // Receive and parse the command.

            if ( !options_.engine.allow_remote &&
                 socket().peerAddress().host().toString() != "127.0.0.1" )
                {
                    throw std::invalid_argument(
                        "Illegal connection attempt from " +
                        socket().peerAddress().toString() +
                        "! Only connections from localhost "
                        "(127.0.0.1) are allowed!" );
                }

            Poco::JSON::Object cmd =
                engine::Receiver::recv_cmd( socket(), logger_ );

            std::string type = cmd.SQLNET_GET( "type_" );

            std::string name = cmd.SQLNET_GET( "name_" );

            // ---------------------------------------------------------------
            // Commands that do not need licensing.

            if ( type == "is_alive" )
                {
                    return;
                }
            else if ( type == "shutdown" )
                {
                    monitor().shutdown();
                    *shutdown_ = true;
                    return;
                }

            // ---------------------------------------------------------------
            // Make sure that we have an active token.

            if ( !license_checker_->has_active_token() )
                {
                    license_checker_->receive_token();

                    if ( !license_checker_->has_active_token() )
                        {
                            throw std::runtime_error(
                                "This command was rejected, because the "
                                "AutoSQL engine "
                                "does not have an active token. Did you maybe "
                                "not log "
                                "in? If no, open your browser and point it to "
                                "the URL of the AutoSQL Monitor. If AutoSQL is "
                                "running on your local computer, that URL is " +
                                options_.monitor_url() + "." );
                        }
                }

            // ---------------------------------------------------------------
            // Commands that need licensing.

            if ( type == "CategoricalMatrix.get" )
                {
                    data_frame_manager().get_categorical_matrix(
                        name, cmd, socket() );
                }
            else if ( type == "DataFrame" )
                {
                    project_manager().add_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.append" )
                {
                    data_frame_manager().append_to_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.delete" )
                {
                    project_manager().delete_data_frame( name, cmd, socket() );
                }
            else if ( type == "DataFrame.load" )
                {
                    project_manager().load_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.get_content" )
                {
                    data_frame_manager().get_data_frame_content(
                        name, cmd, socket() );
                }
            else if ( type == "DataFrame.nbytes" )
                {
                    data_frame_manager().get_nbytes( name, socket() );
                }
            else if ( type == "DataFrame.refresh" )
                {
                    data_frame_manager().refresh( name, socket() );
                }
            else if ( type == "DataFrame.save" )
                {
                    project_manager().save_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.summarize" )
                {
                    data_frame_manager().summarize( name, socket() );
                }
            else if ( type == "delete_all_users" )
                {
                    monitor().send( "deleteallusers", "" );

                    engine::Sender::send_string( socket(), "Success!" );
                }
            else if ( type == "delete_project" )
                {
                    project_manager().delete_project( name, socket() );
                }
            else if ( type == "fit" )
                {
                    model_manager().fit_model( name, cmd, logger_, socket() );
                }
            else if ( type == "is_alive" )
                {
                    return;
                }
            else if ( type == "Matrix.get" )
                {
                    data_frame_manager().get_matrix( name, cmd, socket() );
                }
            else if ( type == "Model" )
                {
                    project_manager().add_model( name, cmd, socket() );
                }
            else if ( type == "Model.copy" )
                {
                    model_manager().copy_model( name, cmd, socket() );
                }
            else if ( type == "Model.delete" )
                {
                    project_manager().delete_model( name, cmd, socket() );
                }
            else if ( type == "Model.load" )
                {
                    project_manager().load_model( name, socket() );
                }
            else if ( type == "Model.refresh" )
                {
                    model_manager().refresh_model( name, socket() );
                }
            else if ( type == "Model.save" )
                {
                    project_manager().save_model( name, socket() );
                }
            else if ( type == "Model.score" )
                {
                    model_manager().score( name, cmd, socket() );
                }
            else if ( type == "refresh" )
                {
                    project_manager().refresh( socket() );
                }
            else if ( type == "set_project" )
                {
                    project_manager().set_project( name, socket() );
                }
            else if ( type == "to_json" )
                {
                    model_manager().to_json( name, socket() );
                }
            else if ( type == "to_sql" )
                {
                    model_manager().to_sql( name, socket() );
                }
            else if ( type == "transform" )
                {
                    model_manager().transform( name, cmd, socket() );
                }
        }
    catch ( std::exception& e )
        {
            engine::Sender::send_string( socket(), e.what() );

            logger_->log( std::string( "Error: " ) + e.what() );
        }
}

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql

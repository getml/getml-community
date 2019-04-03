#include "engine/engine.hpp"

namespace engine
{
namespace srv
{
// ------------------------------------------------------------------------

void RequestHandler::run()
{
    /*  if ( !license_checker->token().currently_active )
          {
              monitor().shutdown();
              return;
          };*/

    try
        {
            if (  //! options_.engine.allow_remote &&
                socket().peerAddress().host().toString() != "127.0.0.1" )
                {
                    throw std::invalid_argument(
                        "Illegal connection attempt from " +
                        socket().peerAddress().toString() +
                        "! Only connections from localhost "
                        "(127.0.0.1) are allowed!" );
                }

            Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, &socket() );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            if ( type == "CategoricalMatrix.get" )
                {
                    data_frame_manager().get_categorical_matrix(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame" )
                {
                    // project_manager().add_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.append" )
                {
                    data_frame_manager().append_to_data_frame(
                        name, &socket() );
                }
            else if ( type == "DataFrame.delete" )
                {
                    // project_manager().delete_data_frame( name, cmd, socket()
                    // );
                }
            else if ( type == "DataFrame.load" )
                {
                    //  project_manager().load_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.get_content" )
                {
                    data_frame_manager().get_data_frame_content(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.nbytes" )
                {
                    data_frame_manager().get_nbytes( name, &socket() );
                }
            else if ( type == "DataFrame.refresh" )
                {
                    data_frame_manager().refresh( name, &socket() );
                }
            else if ( type == "DataFrame.save" )
                {
                    //  project_manager().save_data_frame( name, socket() );
                }
            else if ( type == "DataFrame.summarize" )
                {
                    data_frame_manager().summarize( name, &socket() );
                }
            else if ( type == "delete_all_users" )
                {
                    /*    monitor().send( "deleteallusers", "" );

                        communication::Sender::send_string( socket(), "Success!"
                       );*/
                }
            else if ( type == "delete_project" )
                {
                    //  project_manager().delete_project( name, socket() );
                }
            else if ( type == "fit" )
                {
                    //   model_manager().fit_model( name, cmd, logger_, socket()
                    //   );
                }
            else if ( type == "is_alive" )
                {
                    return;
                }
            else if ( type == "Matrix.get" )
                {
                    data_frame_manager().get_matrix( name, cmd, &socket() );
                }
            else if ( type == "Model" )
                {
                    // project_manager().add_model( name, cmd, socket() );
                }
            else if ( type == "Model.copy" )
                {
                    //   model_manager().copy_model( name, cmd, socket() );
                }
            else if ( type == "Model.delete" )
                {
                    //  project_manager().delete_model( name, cmd, socket() );
                }
            else if ( type == "Model.load" )
                {
                    //  project_manager().load_model( name, socket() );
                }
            else if ( type == "Model.refresh" )
                {
                    //  model_manager().refresh_model( name, socket() );
                }
            else if ( type == "Model.save" )
                {
                    //   project_manager().save_model( name, socket() );
                }
            else if ( type == "Model.score" )
                {
                    // model_manager().score( name, cmd, socket() );
                }
            else if ( type == "refresh" )
                {
                    // project_manager().refresh( socket() );
                }
            else if ( type == "set_project" )
                {
                    // project_manager().set_project( name, socket() );
                }
            else if ( type == "shutdown" )
                {
                    /*    monitor().shutdown();
                     *shutdown_ = true;*/
                }
            else if ( type == "to_json" )
                {
                    // model_manager().to_json( name, socket() );
                }
            else if ( type == "to_sql" )
                {
                    // model_manager().to_sql( name, socket() );
                }
            else if ( type == "transform" )
                {
                    // model_manager().transform( name, cmd, socket() );
                }
        }
    catch ( std::exception& e )
        {
            logger_->log( std::string( "Error: " ) + e.what() );
        }
}

// ------------------------------------------------------------------------
}  // namespace srv
}  // namespace engine

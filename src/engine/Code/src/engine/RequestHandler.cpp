#include "engine/engine.hpp"

namespace engine
{
namespace srv
{
// ------------------------------------------------------------------------

void RequestHandler::run()
{
    try
        {
            if ( options_.engine_.allow_remote_ &&
                 socket().peerAddress().host().toString() != "127.0.0.1" )
                {
                    throw std::invalid_argument(
                        "Illegal connection attempt from " +
                        socket().peerAddress().toString() +
                        "! Only connections from localhost "
                        "(127.0.0.1) are allowed!" );
                }

            const Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, &socket() );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            if ( type == "CategoricalColumn.get" )
                {
                    data_frame_manager().get_categorical_column(
                        cmd, &socket() );
                }
            else if ( type == "Column.get" )
                {
                    data_frame_manager().get_column( cmd, &socket() );
                }
            else if ( type == "Database.drop_table" )
                {
                    database_manager().drop_table( name, &socket() );
                }
            else if ( type == "Database.execute" )
                {
                    database_manager().execute( &socket() );
                }
            else if ( type == "Database.read_csv" )
                {
                    database_manager().read_csv( name, cmd, &socket() );
                }
            else if ( type == "Database.sniff_csv" )
                {
                    database_manager().sniff_csv( name, cmd, &socket() );
                }
            else if ( type == "DataFrame" )
                {
                    project_manager().add_data_frame( name, &socket() );
                }
            else if ( type == "DataFrame.append" )
                {
                    data_frame_manager().append_to_data_frame(
                        name, &socket() );
                }
            else if ( type == "DataFrame.delete" )
                {
                    project_manager().delete_data_frame( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.from_db" )
                {
                    data_frame_manager().from_db( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.load" )
                {
                    project_manager().load_data_frame( name, &socket() );
                }
            else if ( type == "DataFrame.get" )
                {
                    data_frame_manager().get_data_frame( &socket() );
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
                    project_manager().save_data_frame( name, &socket() );
                }
            else if ( type == "DataFrame.summarize" )
                {
                    data_frame_manager().summarize( name, &socket() );
                }
            else if ( type == "delete_all_users" )
                {
                    /*    monitor().send( "deleteallusers", "" );

                        communication::Sender::send_string( "Success!",
                       &socket()
                       );*/
                }
            else if ( type == "delete_project" )
                {
                    project_manager().delete_project( name, &socket() );
                }
            else if ( type == "is_alive" )
                {
                    return;
                }
            else if ( type == "RelboostModel" )
                {
                    project_manager().add_relboost_model(
                        name, cmd, &socket() );
                }
            else if ( type == "RelboostModel.copy" )
                {
                    relboost_model_manager().copy_model( name, cmd, &socket() );
                }
            else if ( type == "RelboostModel.delete" )
                {
                    project_manager().delete_relboost_model(
                        name, cmd, &socket() );
                }
            else if ( type == "RelboostModel.fit" )
                {
                    relboost_model_manager().fit_model( name, cmd, &socket() );
                }
            else if ( type == "RelboostModel.load" )
                {
                    project_manager().load_relboost_model( name, &socket() );
                }
            else if ( type == "RelboostModel.save" )
                {
                    project_manager().save_relboost_model( name, &socket() );
                }
            else if ( type == "RelboostModel.score" )
                {
                    relboost_model_manager().score( name, cmd, &socket() );
                }
            else if ( type == "RelboostModel.to_json" )
                {
                    relboost_model_manager().to_json( name, &socket() );
                }
            else if ( type == "RelboostModel.to_sql" )
                {
                    relboost_model_manager().to_sql( name, &socket() );
                }
            else if ( type == "RelboostModel.transform" )
                {
                    relboost_model_manager().transform( name, cmd, &socket() );
                }
            else if ( type == "refresh" )
                {
                    // project_manager().refresh( socket() );
                }
            else if ( type == "set_project" )
                {
                    project_manager().set_project( name, &socket() );
                }
            else if ( type == "shutdown" )
                {
                    //    monitor().shutdown();
                    *shutdown_ = true;
                }
        }
    catch ( std::exception& e )
        {
            logger_->log( std::string( "Error: " ) + e.what() );

            communication::Sender::send_string( e.what(), &socket() );
        }
}

// ------------------------------------------------------------------------
}  // namespace srv
}  // namespace engine

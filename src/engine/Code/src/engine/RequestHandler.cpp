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
            if ( socket().peerAddress().host().toString() != "127.0.0.1" )
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

            if ( type == "BooleanColumn.get" )
                {
                    data_frame_manager().get_boolean_column(
                        name, cmd, &socket() );
                }
            else if ( type == "BooleanColumn.get_content" )
                {
                    data_frame_manager().get_boolean_column_content(
                        name, cmd, &socket() );
                }
            else if ( type == "BooleanColumn.get_nrows" )
                {
                    data_frame_manager().get_boolean_column_nrows(
                        name, cmd, &socket() );
                }
            else if ( type == "Database.copy_table" )
                {
                    database_manager().copy_table( cmd, &socket() );
                }
            else if ( type == "Database.describe_connection" )
                {
                    database_manager().describe_connection( name, &socket() );
                }
            else if ( type == "Database.drop_table" )
                {
                    database_manager().drop_table( name, cmd, &socket() );
                }
            else if ( type == "Database.execute" )
                {
                    database_manager().execute( name, &socket() );
                }
            else if ( type == "Database.get" )
                {
                    database_manager().get( name, &socket() );
                }
            else if ( type == "Database.get_colnames" )
                {
                    database_manager().get_colnames( name, cmd, &socket() );
                }
            else if ( type == "Database.get_content" )
                {
                    database_manager().get_content( name, cmd, &socket() );
                }
            else if ( type == "Database.get_nrows" )
                {
                    database_manager().get_nrows( name, cmd, &socket() );
                }
            else if ( type == "Database.list_connections" )
                {
                    database_manager().list_connections( &socket() );
                }
            else if ( type == "Database.list_tables" )
                {
                    database_manager().list_tables( name, &socket() );
                }
            else if ( type == "Database.new" )
                {
                    database_manager().new_db( cmd, &socket() );
                }
            else if ( type == "Database.read_csv" )
                {
                    database_manager().read_csv( name, cmd, &socket() );
                }
            else if ( type == "Database.read_s3" )
                {
                    database_manager().read_s3( name, cmd, &socket() );
                }
            else if ( type == "Database.refresh" )
                {
                    database_manager().refresh( &socket() );
                }
            else if ( type == "Database.sniff_csv" )
                {
                    database_manager().sniff_csv( name, cmd, &socket() );
                }
            else if ( type == "Database.sniff_s3" )
                {
                    database_manager().sniff_s3( name, cmd, &socket() );
                }
            else if ( type == "Database.sniff_table" )
                {
                    database_manager().sniff_table( name, cmd, &socket() );
                }
            else if ( type == "DataFrame" )
                {
                    project_manager().add_data_frame( name, &socket() );
                }
            else if ( type == "DataFrame.add_categorical_column" )
                {
                    data_frame_manager().add_string_column(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.add_column" )
                {
                    data_frame_manager().add_float_column(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.append" )
                {
                    data_frame_manager().append_to_data_frame(
                        name, &socket() );
                }
            else if ( type == "DataFrame.calc_categorical_column_plots" )
                {
                    data_frame_manager().calc_categorical_column_plots(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.calc_column_plots" )
                {
                    data_frame_manager().calc_column_plots(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.concat" )
                {
                    data_frame_manager().concat( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.delete" )
                {
                    project_manager().delete_data_frame( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.from_db" )
                {
                    project_manager().add_data_frame_from_db(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.from_json" )
                {
                    project_manager().add_data_frame_from_json(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.from_query" )
                {
                    project_manager().add_data_frame_from_query(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.from_view" )
                {
                    project_manager().add_data_frame_from_view(
                        name, cmd, &socket() );
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
            else if ( type == "DataFrame.get_html" )
                {
                    data_frame_manager().get_data_frame_html(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.get_string" )
                {
                    data_frame_manager().get_data_frame_string(
                        name, &socket() );
                }
            else if ( type == "DataFrame.group_by" )
                {
                    data_frame_manager().group_by( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.join" )
                {
                    data_frame_manager().join( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.nbytes" )
                {
                    data_frame_manager().get_nbytes( name, &socket() );
                }
            else if ( type == "DataFrame.nrows" )
                {
                    data_frame_manager().get_nrows( name, &socket() );
                }
            else if ( type == "DataFrame.read_csv" )
                {
                    project_manager().add_data_frame_from_csv(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.read_s3" )
                {
                    project_manager().add_data_frame_from_s3(
                        name, cmd, &socket() );
                }
            else if ( type == "DataFrame.refresh" )
                {
                    data_frame_manager().refresh( name, &socket() );
                }
            else if ( type == "DataFrame.remove_column" )
                {
                    data_frame_manager().remove_column( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.save" )
                {
                    project_manager().save_data_frame( name, &socket() );
                }
            else if ( type == "DataFrame.summarize" )
                {
                    data_frame_manager().summarize( name, &socket() );
                }
            else if ( type == "DataFrame.to_csv" )
                {
                    data_frame_manager().to_csv( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.to_db" )
                {
                    data_frame_manager().to_db( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.to_s3" )
                {
                    data_frame_manager().to_s3( name, cmd, &socket() );
                }
            else if ( type == "DataFrame.where" )
                {
                    data_frame_manager().where( name, cmd, &socket() );
                }
            else if ( type == "delete_project" )
                {
                    project_manager().delete_project( name, &socket() );
                }
            else if ( type == "FloatColumn" )
                {
                    data_frame_manager().add_float_column( cmd, &socket() );
                }
            else if ( type == "FloatColumn.aggregate" )
                {
                    data_frame_manager().aggregate( name, cmd, &socket() );
                }
            else if ( type == "FloatColumn.get" )
                {
                    data_frame_manager().get_column( name, cmd, &socket() );
                }
            else if ( type == "FloatColumn.get_content" )
                {
                    data_frame_manager().get_float_column_content(
                        name, cmd, &socket() );
                }
            else if ( type == "FloatColumn.get_nrows" )
                {
                    data_frame_manager().get_column_nrows(
                        name, cmd, &socket() );
                }
            else if ( type == "FloatColumn.get_unit" )
                {
                    data_frame_manager().get_unit( name, cmd, &socket() );
                }
            else if ( type == "FloatColumn.set_unit" )
                {
                    data_frame_manager().set_unit( name, cmd, &socket() );
                }
            else if (
                type == "GaussianHyperparameterSearch" ||
                type == "LatinHypercubeSearch" || type == "RandomSearch" )
                {
                    project_manager().add_hyperopt( name, cmd, &socket() );
                }
            else if ( type == "Hyperopt.launch" )
                {
                    hyperopt_manager().launch( name, cmd, &socket() );
                }
            else if ( type == "Hyperopt.load" )
                {
                    project_manager().load_hyperopt( name, &socket() );
                }
            else if ( type == "Hyperopt.refresh" )
                {
                    hyperopt_manager().refresh( name, &socket() );
                }
            else if ( type == "Hyperopt.save" )
                {
                    project_manager().save_hyperopt( name, &socket() );
                }
            else if ( type == "Hyperopt.tune" )
                {
                    hyperopt_manager().tune( name, cmd, &socket() );
                }
            else if ( type == "list_data_frames" )
                {
                    project_manager().list_data_frames( &socket() );
                }
            else if ( type == "list_hyperopts" )
                {
                    project_manager().list_hyperopts( &socket() );
                }
            else if ( type == "list_pipelines" )
                {
                    project_manager().list_pipelines( &socket() );
                }
            else if ( type == "list_projects" )
                {
                    project_manager().list_projects( &socket() );
                }
            else if ( type == "is_alive" )
                {
                    return;
                }
            else if ( type == "monitor_url" )
                {
                    communication::Sender::send_string(
                        options_.monitor().url(), &socket() );
                }
            else if ( type == "Pipeline" )
                {
                    project_manager().add_pipeline( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.check" )
                {
                    pipeline_manager().check( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.column_importances" )
                {
                    pipeline_manager().column_importances(
                        name, cmd, &socket() );
                }
            else if ( type == "Pipeline.copy" )
                {
                    project_manager().copy_pipeline( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.delete" )
                {
                    project_manager().delete_pipeline( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.deploy" )
                {
                    pipeline_manager().deploy( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.feature_correlations" )
                {
                    pipeline_manager().feature_correlations(
                        name, cmd, &socket() );
                }
            else if ( type == "Pipeline.feature_importances" )
                {
                    pipeline_manager().feature_importances(
                        name, cmd, &socket() );
                }
            else if ( type == "Pipeline.fit" )
                {
                    pipeline_manager().fit( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.lift_curve" )
                {
                    pipeline_manager().lift_curve( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.load" )
                {
                    project_manager().load_pipeline( name, &socket() );
                }
            else if ( type == "Pipeline.precision_recall_curve" )
                {
                    pipeline_manager().precision_recall_curve(
                        name, cmd, &socket() );
                }
            else if ( type == "project_name" )
                {
                    project_manager().project_name( &socket() );
                }
            else if ( type == "Pipeline.refresh" )
                {
                    pipeline_manager().refresh( name, &socket() );
                }
            else if ( type == "Pipeline.refresh_all" )
                {
                    pipeline_manager().refresh_all( &socket() );
                }
            else if ( type == "Pipeline.roc_curve" )
                {
                    pipeline_manager().roc_curve( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.save" )
                {
                    project_manager().save_pipeline( name, &socket() );
                }
            else if ( type == "Pipeline.to_sql" )
                {
                    pipeline_manager().to_sql( name, cmd, &socket() );
                }
            else if ( type == "Pipeline.transform" )
                {
                    pipeline_manager().transform( name, cmd, &socket() );
                }
            else if ( type == "set_s3_access_key_id" )
                {
                    database_manager().set_s3_access_key_id( &socket() );
                }
            else if ( type == "set_s3_secret_access_key" )
                {
                    database_manager().set_s3_secret_access_key( &socket() );
                }
            else if ( type == "shutdown" )
                {
                    *shutdown_ = true;
                }
            else if ( type == "StringColumn" )
                {
                    data_frame_manager().add_string_column( cmd, &socket() );
                }
            else if ( type == "StringColumn.get" )
                {
                    data_frame_manager().get_categorical_column(
                        name, cmd, &socket() );
                }
            else if ( type == "StringColumn.get_content" )
                {
                    data_frame_manager().get_categorical_column_content(
                        name, cmd, &socket() );
                }
            else if ( type == "StringColumn.get_nrows" )
                {
                    data_frame_manager().get_categorical_column_nrows(
                        name, cmd, &socket() );
                }
            else if ( type == "StringColumn.get_unit" )
                {
                    data_frame_manager().get_unit_categorical(
                        name, cmd, &socket() );
                }
            else if ( type == "StringColumn.set_unit" )
                {
                    data_frame_manager().set_unit_categorical(
                        name, cmd, &socket() );
                }
            else
                {
                    throw std::invalid_argument(
                        "Unknown command: '" + type + "'" );
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

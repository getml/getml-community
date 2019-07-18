#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void DataFrameManager::add_categorical_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    // ------------------------------------------------------------------------

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    // ------------------------------------------------------------------------

    const auto vec = CatOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    // ------------------------------------------------------------------------

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    // ------------------------------------------------------------------------

    auto col = containers::Column<Int>( vec.size() );

    auto encoding = local_join_keys_encoding;

    if ( role == "categorical" )
        {
            encoding = local_categories;
        }

    for ( size_t i = 0; i < vec.size(); ++i )
        {
            col[i] = ( *encoding )[vec[i]];
        }

    // ------------------------------------------------------------------------

    col.set_name( name );

    col.set_unit( unit );

    // ------------------------------------------------------------------------

    weak_write_lock.upgrade();

    // ------------------------------------------------------------------------

    df.add_int_column( col, role );

    // ------------------------------------------------------------------------

    if ( role == "categorical" )
        {
            categories_->append( *local_categories );
        }
    else
        {
            join_keys_encoding_->append( *local_join_keys_encoding );
        }

    // ------------------------------------------------------------------------

    monitor_->send( "postdataframe", df.to_monitor() );

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_categorical_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    containers::Column<Int> col;

    if ( role == "categorical" )
        {
            col = communication::Receiver::recv_categorical_column(
                categories_.get(), _socket );
        }
    else if ( role == "join_key" )
        {
            col = communication::Receiver::recv_categorical_column(
                join_keys_encoding_.get(), _socket );
        }
    else
        {
            throw std::runtime_error(
                "A categorical column must either have the role categorical or "
                "join key" );
        }

    col.set_name( name );

    col.set_unit( unit );

    _df->add_int_column( col, role );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( &df, _socket );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    df.set_categories( categories_ );

    df.set_join_keys_encoding( join_keys_encoding_ );

    data_frames()[_name] = df;

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    auto col = NumOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    col.set_name( name );

    col.set_unit( unit );

    weak_write_lock.upgrade();

    df.add_float_column( col, role );

    monitor_->send( "postdataframe", df.to_monitor() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    auto col = communication::Receiver::recv_column( _socket );

    col.set_name( name );

    col.set_unit( unit );

    _df->add_float_column( col, role );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::aggregate(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto aggregation = *JSON::get_object( _cmd, "aggregation_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( df_name, data_frames() );

    auto response = containers::Column<Float>( 1 );

    response[0] = AggOpParser::aggregate(
        categories_, join_keys_encoding_, df, aggregation );

    read_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_column( response, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::append_to_data_frame(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Make sure a data frame of this name exists.

    if ( data_frames().find( _name ) == data_frames().end() )
        {
            throw std::invalid_argument(
                "No data frame named '" + _name +
                "' is currently loaded in memory." );
        }

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( &df, _socket );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // Append to data frame

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    data_frames()[_name].append( df );

    data_frames()[_name].create_indices();

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::close(
    const containers::DataFrame& _df, Poco::Net::StreamSocket* _socket )
{
    // license_checker_->check_memory_size( data_frames(), _df );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::from_db(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // Parse the command.

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    const auto categoricals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "categoricals_" ) );

    const auto discretes = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "discretes_" ) );

    const auto join_keys = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "join_keys_" ) );

    const auto numericals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "numericals_" ) );

    const auto targets = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "targets_" ) );

    const auto time_stamps = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_stamps_" ) );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    // --------------------------------------------------------------------
    // Get the data from the data base.

    df.from_db(
        connector(),
        table_name,
        categoricals,
        discretes,
        join_keys,
        numericals,
        targets,
        time_stamps );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    df.set_categories( categories_ );

    df.set_join_keys_encoding( join_keys_encoding_ );

    if ( !_append || data_frames().find( _name ) == data_frames().end() )
        {
            data_frames()[_name] = df;
        }
    else
        {
            data_frames()[_name].append( df );
        }

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::from_json(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // Create the data frame as an object.

    const auto json_str = communication::Receiver::recv_string( _socket );

    const auto obj = *Poco::JSON::Parser()
                          .parse( json_str )
                          .extract<Poco::JSON::Object::Ptr>();

    // --------------------------------------------------------------------
    // Parse the command.

    const auto categoricals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "categoricals_" ) );

    const auto discretes = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "discretes_" ) );

    const auto join_keys = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "join_keys_" ) );

    const auto numericals = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "numericals_" ) );

    const auto targets = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "targets_" ) );

    const auto time_stamps = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_stamps_" ) );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    // --------------------------------------------------------------------
    // Parse the data from the JSON string.

    df.from_json(
        obj,
        time_formats,
        categoricals,
        discretes,
        join_keys,
        numericals,
        targets,
        time_stamps );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    df.set_categories( categories_ );

    df.set_join_keys_encoding( join_keys_encoding_ );

    if ( !_append || data_frames().find( _name ) == data_frames().end() )
        {
            data_frames()[_name] = df;
        }
    else
        {
            data_frames()[_name].append( df );
        }

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( _name, &data_frames() );

    const auto col = BoolOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_boolean_column( col, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( _name, &data_frames() );

    const auto col = CatOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_categorical_column( col, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( _name, &data_frames() );

    const auto col = NumOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_column( col, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<Int>( _cmd, "length_" );

    const auto start = JSON::get_value<Int>( _cmd, "start_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto& df = utils::Getter::get( _name, &data_frames() );

    auto obj = df.get_content( draw, start, length );

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame( Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    while ( true )
        {
            Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type == "CategoricalColumn.get" )
                {
                    get_categorical_column( name, cmd, _socket );
                }
            else if ( type == "Column.get" )
                {
                    get_column( name, cmd, _socket );
                }
            else if ( type == "DataFrame.close" )
                {
                    communication::Sender::send_string( "Success!", _socket );
                    break;
                }
            else
                {
                    break;
                }
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nbytes(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string(
        std::to_string( df.nbytes() ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::group_by(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto aggregations = *JSON::get_array( _cmd, "aggregations_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto join_key_name =
        JSON::get_value<std::string>( _cmd, "join_key_name_" );

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto df = utils::Getter::get( df_name, data_frames() );

    const auto grouped_df = GroupByParser::group_by(
        categories_,
        join_keys_encoding_,
        df,
        _name,
        join_key_name,
        aggregations );

    weak_write_lock.upgrade();

    data_frames()[_name] = grouped_df;

    monitor_->send( "postdataframe", grouped_df.to_monitor() );

    weak_write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::join(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto cols1 = *JSON::get_array( _cmd, "cols1_" );

    const auto cols2 = *JSON::get_array( _cmd, "cols2_" );

    const auto df1_name = JSON::get_value<std::string>( _cmd, "df1_name_" );

    const auto df2_name = JSON::get_value<std::string>( _cmd, "df2_name_" );

    const auto join_key_used =
        JSON::get_value<std::string>( _cmd, "join_key_used_" );

    const auto other_join_key_used =
        JSON::get_value<std::string>( _cmd, "other_join_key_used_" );

    const auto how = JSON::get_value<std::string>( _cmd, "how_" );

    auto where = std::optional<Poco::JSON::Object>();

    if ( _cmd.has( "where_" ) )
        {
            where = std::make_optional<Poco::JSON::Object>(
                *JSON::get_object( _cmd, "where_" ) );
        }

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto& df1 = utils::Getter::get( df1_name, &data_frames() );

    const auto& df2 = utils::Getter::get( df2_name, &data_frames() );

    const auto joined_df = DataFrameJoiner::join(
        _name,
        df1,
        df2,
        cols1,
        cols2,
        join_key_used,
        other_join_key_used,
        how,
        where,
        categories_,
        join_keys_encoding_ );

    weak_write_lock.upgrade();

    data_frames()[_name] = joined_df;

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor() );

    weak_write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::receive_data(
    containers::DataFrame* _df, Poco::Net::StreamSocket* _socket )
{
    while ( true )
        {
            Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            if ( type == "CategoricalColumn" )
                {
                    add_categorical_column( cmd, _df, _socket );
                }
            else if ( type == "Column" )
                {
                    add_column( cmd, _df, _socket );
                }
            else if ( type == "DataFrame.close" )
                {
                    close( *_df, _socket );
                    break;
                }
            else
                {
                    break;
                }
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::refresh(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    Poco::JSON::Object encodings = df.get_colnames();

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( encodings ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::remove_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    multithreading::WriteLock write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    df.remove_column( name, role );

    monitor_->send( "postdataframe", df.to_monitor() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    auto column = df.float_column( _name, role );

    column.set_unit( unit );

    df.add_float_column( column, role );

    monitor_->send( "postdataframe", df.to_monitor() );

    write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit_categorical(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    multithreading::WriteLock write_lock( read_write_lock_ );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    auto column = df.int_column( _name, role );

    column.set_unit( unit );

    df.add_int_column( column, role );

    monitor_->send( "postdataframe", df.to_monitor() );

    write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::summarize(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    auto summary = df.to_monitor();

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( summary ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::where(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    const auto new_df_name = JSON::get_value<std::string>( _cmd, "new_df_" );

    const auto condition_json = *JSON::get_object( _cmd, "condition_" );

    auto df = utils::Getter::get( _name, data_frames() );

    const auto condition = BoolOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, condition_json );

    // --------------------------------------------------------------------

    df.where( condition );

    df.set_name( new_df_name );

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    data_frames()[new_df_name] = df;

    monitor_->send( "postdataframe", df.to_monitor() );

    weak_write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

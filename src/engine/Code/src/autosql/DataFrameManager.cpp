#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

void DataFrameManager::add_categorical_matrix(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const std::string join_key_name = _cmd.AUTOSQL_GET( "join_key_name_" );

    const Int num_join_key = _cmd.AUTOSQL_GET( "num_join_key_" );

    containers::Matrix<Int> mat;

    if ( role == "categorical" )
        {
            mat = engine::Receiver::recv_categorical_matrix(
                _socket, *categories_ );
        }
    else if ( role == "join_key" )
        {
            mat = engine::Receiver::recv_categorical_matrix(
                _socket, *join_keys_encoding_ );
        }
    else
        {
            assert( false );
        }

    mat.name() = join_key_name;

    _df.int_matrix( mat, role, join_key_name, num_join_key );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_data_frame(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df =
        containers::DataFrame( local_categories, local_join_keys_encoding );

    df.name() = _name;

    engine::Sender::send_string( _socket, "Success!" );

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( df, _socket );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to make the
    // actual changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // No problems while creating the data frame - we can store it!

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    df.set_categories( categories_ );

    df.set_join_keys_encoding( join_keys_encoding_ );

    data_frames()[_name] = df;

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_matrix(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const std::string time_stamps_name = _cmd.AUTOSQL_GET( "time_stamps_name_" );

    const AUTOSQL_SIZE num_time_stamps = _cmd.AUTOSQL_GET( "num_time_stamps_" );

    auto mat = engine::Receiver::recv_matrix( _socket, true );

    mat.name() = _df.name();

    _df.float_matrix( mat, role, time_stamps_name, num_time_stamps );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::append_to_data_frame(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    autosql::multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Create the data frame itself.

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    auto df =
        containers::DataFrame( local_categories, local_join_keys_encoding );

    df.name() = _name;

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( df, _socket );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to make the
    // actual changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------
    // Append to data frame

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    data_frames()[_name].append( df );

    data_frames()[_name].create_indices();

    monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name ) );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::categorical_matrix_set_colnames(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_join_key = _cmd.AUTOSQL_GET( "num_join_key_" );

    auto& mat = _df.int_matrix( role, num_join_key );

    auto colnames = JSON::array_to_vector<std::string>(
        _cmd.AUTOSQL_GET_ARRAY( "colnames_" ) );

    mat.set_colnames( colnames );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::categorical_matrix_set_units(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_join_key = _cmd.AUTOSQL_GET( "num_join_key_" );

    auto& mat = _df.int_matrix( role, num_join_key );

    auto units =
        JSON::array_to_vector<std::string>( _cmd.AUTOSQL_GET_ARRAY( "units_" ) );

    mat.set_units( units );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::close(
    containers::DataFrame& _df, Poco::Net::StreamSocket& _socket )
{
    license_checker_->check_memory_size( data_frames(), _df );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_matrix(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_join_key = _cmd.AUTOSQL_GET( "num_join_key_" );

    // Will auto-unlock when destroyed.
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat = engine::Getter::get( data_frames(), _name )
                   .int_matrix( role, num_join_key );

    engine::Sender::send_string( _socket, "Found!" );

    engine::Sender::send_matrix<Int>( _socket, true, mat );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    const std::int32_t draw = _cmd.AUTOSQL_GET( "draw_" );

    const std::int32_t length = _cmd.AUTOSQL_GET( "length_" );

    const std::int32_t start = _cmd.AUTOSQL_GET( "start_" );

    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    const auto& df = engine::Getter::get( data_frames(), _name );

    auto obj = df.get_content( draw, start, length );

    read_lock.unlock();

    engine::Sender::send_string( _socket, JSON::stringify( obj ) );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_matrix(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_time_stamps = _cmd.AUTOSQL_GET( "num_time_stamps_" );

    // Will auto-unlock when destroyed.
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat = engine::Getter::get( data_frames(), _name )
                   .float_matrix( role, num_time_stamps );

    engine::Sender::send_string( _socket, "Found!" );

    engine::Sender::send_matrix<Float>( _socket, true, mat );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nbytes(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    // Will auto-unlock when destroyed.
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = engine::Getter::get( data_frames(), _name );

    engine::Sender::send_string( _socket, "Found!" );

    engine::Sender::send_string( _socket, std::to_string( df.nbytes() ) );
}

// ------------------------------------------------------------------------

void DataFrameManager::matrix_set_colnames(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_time_stamps = _cmd.AUTOSQL_GET( "num_time_stamps_" );

    auto& mat = _df.float_matrix( role, num_time_stamps );

    auto colnames = JSON::array_to_vector<std::string>(
        _cmd.AUTOSQL_GET_ARRAY( "colnames_" ) );

    mat.set_colnames( colnames );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::matrix_set_units(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame& _df,
    Poco::Net::StreamSocket& _socket )
{
    const std::string role = _cmd.AUTOSQL_GET( "role_" );

    const AUTOSQL_SIZE num_time_stamps = _cmd.AUTOSQL_GET( "num_time_stamps_" );

    auto& mat = _df.float_matrix( role, num_time_stamps );

    auto units =
        JSON::array_to_vector<std::string>( _cmd.AUTOSQL_GET_ARRAY( "units_" ) );

    mat.set_units( units );

    engine::Sender::send_string( _socket, "Success!" );
}

// ------------------------------------------------------------------------

void DataFrameManager::receive_data(
    containers::DataFrame& _df, Poco::Net::StreamSocket& _socket )
{
    while ( true )
        {
            Poco::JSON::Object cmd =
                engine::Receiver::recv_cmd( _socket, logger_ );

            std::string type = cmd.AUTOSQL_GET( "type_" );

            std::string name = cmd.AUTOSQL_GET( "name_" );

            if ( name != _df.name() )
                {
                    throw std::invalid_argument(
                        "Something unexpected occurred. The DataFrame names do "
                        "not match: Got '" +
                        name + "' , expected '" + _df.name() + "' !" );
                }

            if ( type == "CategoricalMatrix" )
                {
                    add_categorical_matrix( cmd, _df, _socket );
                }
            else if ( type == "CategoricalMatrix.set_colnames" )
                {
                    categorical_matrix_set_colnames( cmd, _df, _socket );
                }
            else if ( type == "CategoricalMatrix.set_units" )
                {
                    categorical_matrix_set_units( cmd, _df, _socket );
                }
            else if ( type == "DataFrame.close" )
                {
                    close( _df, _socket );
                    break;
                }
            else if ( type == "Matrix" )
                {
                    add_matrix( cmd, _df, _socket );
                }
            else if ( type == "Matrix.set_colnames" )
                {
                    matrix_set_colnames( cmd, _df, _socket );
                }
            else if ( type == "Matrix.set_units" )
                {
                    matrix_set_units( cmd, _df, _socket );
                }
            else
                {
                    break;
                }
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::refresh(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = engine::Getter::get( data_frames(), _name );

    Poco::JSON::Object encodings = df.get_colnames();

    read_lock.unlock();

    engine::Sender::send_string( _socket, JSON::stringify( encodings ) );
}

// ------------------------------------------------------------------------

void DataFrameManager::summarize(
    const std::string& _name, Poco::Net::StreamSocket& _socket )
{
    autosql::multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = engine::Getter::get( data_frames(), _name );

    auto summary = df.to_monitor( _name );

    read_lock.unlock();

    engine::Sender::send_string( _socket, JSON::stringify( summary ) );
}

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql

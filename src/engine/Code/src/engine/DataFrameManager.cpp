#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void DataFrameManager::add_categorical_matrix(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const std::string join_key_name =
        JSON::get_value<std::string>( _cmd, "join_key_name_" );

    const size_t num_join_key =
        JSON::get_value<size_t>( _cmd, "num_join_key_" );

    containers::Matrix<size_t> mat;

    if ( role == "categorical" )
        {
            mat = communication::Receiver::recv_categorical_matrix(
                categories_.get(), _socket );
        }
    else if ( role == "join_key" )
        {
            mat = communication::Receiver::recv_categorical_matrix(
                join_keys_encoding_.get(), _socket );
        }
    else
        {
            assert( false );
        }

    mat.name() = join_key_name;

    _df->int_matrix( mat, role, join_key_name, num_join_key );

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

    auto df =
        containers::DataFrame( local_categories, local_join_keys_encoding );

    df.name() = _name;

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( &df, _socket );

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
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const std::string time_stamps_name =
        JSON::get_value<std::string>( _cmd, "time_stamps_name_" );

    const size_t num_time_stamps =
        JSON::get_value<size_t>( _cmd, "num_time_stamps_" );

    auto mat = communication::Receiver::recv_matrix( _socket );

    mat.name() = _df->name();

    _df->float_matrix( mat, role, time_stamps_name, num_time_stamps );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::append_to_data_frame(
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

    auto df =
        containers::DataFrame( local_categories, local_join_keys_encoding );

    df.name() = _name;

    // --------------------------------------------------------------------
    // Fill the data frame with data. Note that this does not close the socket
    // connection.

    receive_data( &df, _socket );

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

    // monitor_->send( "postdataframe", data_frames()[_name].to_monitor( _name )
    // );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::categorical_matrix_set_colnames(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_join_key =
        JSON::get_value<size_t>( _cmd, "num_join_key_" );

    auto& mat = _df->int_matrix( role, num_join_key );

    auto colnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "colnames_" ) );

    mat.set_colnames( colnames );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::categorical_matrix_set_units(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_join_key =
        JSON::get_value<size_t>( _cmd, "num_join_key_" );

    auto& mat = _df->int_matrix( role, num_join_key );

    auto units =
        JSON::array_to_vector<std::string>( JSON::get_array( _cmd, "units_" ) );

    mat.set_units( units );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::close(
    const containers::DataFrame& _df, Poco::Net::StreamSocket* _socket )
{
    // license_checker_->check_memory_size( data_frames(), _df );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_matrix(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_join_key =
        JSON::get_value<size_t>( _cmd, "num_join_key_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat = utils::Getter::get( _name, &data_frames() )
                   .int_matrix( role, num_join_key );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_matrix( mat, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<ENGINE_INT>( _cmd, "draw_" );

    const auto length = JSON::get_value<ENGINE_INT>( _cmd, "length_" );

    const auto start = JSON::get_value<ENGINE_INT>( _cmd, "start_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto& df = utils::Getter::get( _name, &data_frames() );

    auto obj = df.get_content( draw, start, length );

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_matrix(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_time_stamps =
        JSON::get_value<size_t>( _cmd, "num_time_stamps_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat = utils::Getter::get( _name, &data_frames() )
                   .float_matrix( role, num_time_stamps );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_matrix<ENGINE_FLOAT>( mat, _socket );
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

void DataFrameManager::matrix_set_colnames(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_time_stamps =
        JSON::get_value<size_t>( _cmd, "num_time_stamps_" );

    auto& mat = _df->float_matrix( role, num_time_stamps );

    auto colnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "colnames_" ) );

    mat.set_colnames( colnames );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::matrix_set_units(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const std::string role = JSON::get_value<std::string>( _cmd, "role_" );

    const size_t num_time_stamps =
        JSON::get_value<size_t>( _cmd, "num_time_stamps_" );

    auto& mat = _df->float_matrix( role, num_time_stamps );

    auto units =
        JSON::array_to_vector<std::string>( JSON::get_array( _cmd, "units_" ) );

    mat.set_units( units );

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

            if ( name != _df->name() )
                {
                    throw std::invalid_argument(
                        "Something unexpected occurred. The DataFrame names do "
                        "not match: Got '" +
                        name + "' , expected '" + _df->name() + "' !" );
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
                    close( *_df, _socket );
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
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    Poco::JSON::Object encodings = df.get_colnames();

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( encodings ), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::summarize(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    auto summary = df.to_monitor( _name );

    read_lock.unlock();

    communication::Sender::send_string( JSON::stringify( summary ), _socket );
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

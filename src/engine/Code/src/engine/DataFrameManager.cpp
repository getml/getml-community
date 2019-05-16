#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

void DataFrameManager::add_categorical_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto num = JSON::get_value<size_t>( _cmd, "num_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    containers::Matrix<ENGINE_INT> mat;

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
            throw std::runtime_error(
                "A categorical column must either have the role categorical or "
                "join key" );
        }

    if ( mat.ncols() != 1 )
        {
            throw std::runtime_error(
                "A matrix used as a data frame column must contains exactly "
                "one column!" );
        }

    mat.name() = name;

    mat.set_colnames( {name} );

    mat.set_units( {unit} );

    _df->add_int_column( mat, role, num );

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

void DataFrameManager::add_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto num = JSON::get_value<size_t>( _cmd, "num_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    auto mat = communication::Receiver::recv_matrix( _socket );

    if ( mat.ncols() != 1 )
        {
            throw std::runtime_error(
                "A matrix used as a data frame column must contains exactly "
                "one column!" );
        }

    mat.name() = _df->name();

    mat.set_colnames( {name} );

    mat.set_units( {unit} );

    _df->add_float_column( mat, role, num );

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

void DataFrameManager::close(
    const containers::DataFrame& _df, Poco::Net::StreamSocket* _socket )
{
    // license_checker_->check_memory_size( data_frames(), _df );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::exec_query( Poco::Net::StreamSocket* _socket )
{
    const auto sql = communication::Receiver::recv_string( _socket );

    connector()->exec( sql );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto num = JSON::get_value<size_t>( _cmd, "num_" );

    if ( role != "categorical" && role != "join_key" )
        {
            throw std::runtime_error(
                "Role for a categorical column must be categorical or "
                "join_key!" );
        }

    multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat =
        utils::Getter::get( df_name, &data_frames() ).int_matrix( role, num );

    communication::Sender::send_string( "Found!", _socket );

    if ( role == "categorical" )
        {
            communication::Sender::send_categorical_matrix(
                mat, *categories_, _socket );
        }
    else
        {
            communication::Sender::send_categorical_matrix(
                mat, *join_keys_encoding_, _socket );
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::get_column(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto num = JSON::get_value<size_t>( _cmd, "num_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    auto mat =
        utils::Getter::get( df_name, &data_frames() ).float_matrix( role, num );

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

void DataFrameManager::get_data_frame( Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    while ( true )
        {
            Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            if ( type == "CategoricalColumn.get" )
                {
                    get_categorical_column( cmd, _socket );
                }
            else if ( type == "Column.get" )
                {
                    get_column( cmd, _socket );
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

void DataFrameManager::read_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto fname = JSON::get_value<std::string>( _cmd, "fname_" );

    const auto header = JSON::get_value<bool>( _cmd, "header_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    // --------------------------------------------------------------------

    if ( quotechar.size() != 1 )
        {
            throw std::invalid_argument(
                "The quotechar must consist of exactly one character!" );
        }

    if ( sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator (sep) must consist of exactly one character!" );
        }

    // --------------------------------------------------------------------

    auto reader = csv::Reader( fname, quotechar[0], sep[0] );

    // --------------------------------------------------------------------

    connector()->read_csv( _name, header, &reader );

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
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

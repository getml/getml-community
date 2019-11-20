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

    if ( role == "undefined_string" )
        {
            add_string_column( name, vec, &df, &weak_write_lock, _socket );
            return;
        }

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

    license_checker().check_mem_size( data_frames(), col.nbytes() );

    // ------------------------------------------------------------------------

    weak_write_lock.upgrade();

    // ------------------------------------------------------------------------

    df.add_int_column( col, role );

    // ------------------------------------------------------------------------

    if ( role == "categorical" )
        {
            categories_->append( *local_categories );
        }
    else if ( role == "join_key" )
        {
            join_keys_encoding_->append( *local_join_keys_encoding );
        }
    else
        {
            assert_true( false );
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
    else if ( role == "undefined_string" )
        {
            const auto str_col =
                communication::Receiver::recv_string_column( _socket );
            add_string_column( name, str_col, _df, nullptr, _socket );
            return;
        }
    else
        {
            throw std::runtime_error(
                "A categorical column must either have the role categorical or "
                "join key" );
        }

    col.set_name( name );

    col.set_unit( unit );

    license_checker().check_mem_size( data_frames(), col.nbytes() );

    _df->add_int_column( col, role );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    // ------------------------------------------------------------------------

    auto col = NumOpParser::parse(
        *categories_, *join_keys_encoding_, {df}, json_col );

    col.set_name( name );

    col.set_unit( unit );

    // ------------------------------------------------------------------------

    add_column_to_df( role, col, &df, &weak_write_lock );

    // ------------------------------------------------------------------------

    monitor_->send( "postdataframe", df.to_monitor() );

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
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

    add_column_to_df( role, col, _df, nullptr );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_column_to_df(
    const std::string& _role,
    const containers::Column<Float>& _col,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock )
{
    if ( _role == "undefined_integer" )
        {
            // ---------------------------------------------------------

            auto int_col = containers::Column<Int>( _col.nrows() );

            for ( size_t i = 0; i < _col.nrows(); ++i )
                if ( !std::isnan( _col[i] ) && !std::isinf( _col[i] ) )
                    int_col[i] = static_cast<Int>( _col[i] );

            int_col.set_name( _col.name() );

            // ---------------------------------------------------------

            license_checker().check_mem_size( data_frames(), int_col.nbytes() );

            // ---------------------------------------------------------

            if ( _weak_write_lock ) _weak_write_lock->upgrade();

            _df->add_int_column( int_col, _role );

            // ---------------------------------------------------------
        }
    else
        {
            license_checker().check_mem_size( data_frames(), _col.nbytes() );

            if ( _weak_write_lock ) _weak_write_lock->upgrade();

            _df->add_float_column( _col, _role );
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column(
    const std::string& _name,
    const std::vector<std::string>& _vec,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    auto col = containers::Column<strings::String>( _vec.size() );

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = strings::String( _vec[i] );
        }

    col.set_name( _name );

    // ------------------------------------------------------------------------

    license_checker().check_mem_size( data_frames(), col.nbytes() );

    // ------------------------------------------------------------------------

    if ( _weak_write_lock ) _weak_write_lock->upgrade();

    // ------------------------------------------------------------------------

    _df->add_string_column( col );

    // ------------------------------------------------------------------------

    monitor_->send( "postdataframe", _df->to_monitor() );

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
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

void DataFrameManager::calc_column_plots(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto num_bins = JSON::get_value<size_t>( _cmd, "num_bins_" );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    const auto& df = utils::Getter::get( df_name, data_frames() );

    auto col = containers::Column<Float>();

    if ( role == "undefined_integer" )
        {
            const auto int_col = df.int_column( _name, role );

            col = containers::Column<Float>( int_col.nrows() );

            for ( size_t i = 0; i < col.nrows(); ++i )
                col[i] = static_cast<Float>( int_col[i] );
        }
    else
        {
            col = df.float_column( _name, role );
        }

    const containers::Features features = {col.data_ptr()};

    const auto obj = metrics::Summarizer::calculate_feature_plots(
        features, col.nrows(), 1, num_bins, {} );

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::close(
    const containers::DataFrame& _df, Poco::Net::StreamSocket* _socket )
{
    license_checker().check_mem_size( data_frames(), _df.nbytes() );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::from_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // Parse the command.

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

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

    const auto undefined_floats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_floats_" ) );

    const auto undefined_integers = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_integers_" ) );

    const auto undefined_strings = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_strings_" ) );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    // --------------------------------------------------------------------

    auto df = containers::DataFrame(
        _name, local_categories, local_join_keys_encoding );

    df.from_csv(
        fnames,
        quotechar,
        sep,
        time_formats,
        categoricals,
        discretes,
        join_keys,
        numericals,
        targets,
        time_stamps,
        undefined_floats,
        undefined_integers,
        undefined_strings );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

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

    const auto undefined_floats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_floats_" ) );

    const auto undefined_integers = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_integers_" ) );

    const auto undefined_strings = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "undefined_strings_" ) );

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
        time_stamps,
        undefined_floats,
        undefined_integers,
        undefined_strings );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

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
    // Parse the command. Note that the JSON column from the HTTP endpoint
    // does not necessarily include undefined columns - so we make the optional.

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

    const auto undefined_floats =
        _cmd.has( "undefined_floats_" )
            ? JSON::array_to_vector<std::string>(
                  JSON::get_array( _cmd, "undefined_floats_" ) )
            : std::vector<std::string>();

    const auto undefined_integers =
        _cmd.has( "undefined_integers_" )
            ? JSON::array_to_vector<std::string>(
                  JSON::get_array( _cmd, "undefined_integers_" ) )
            : std::vector<std::string>();

    const auto undefined_strings =
        _cmd.has( "undefined_strings_" )
            ? JSON::array_to_vector<std::string>(
                  JSON::get_array( _cmd, "undefined_strings_" ) )
            : std::vector<std::string>();

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
        time_stamps,
        undefined_floats,
        undefined_integers,
        undefined_strings );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

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

void DataFrameManager::from_query(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // Parse the command.

    const auto query = JSON::get_value<std::string>( _cmd, "query_" );

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

    df.from_query(
        connector(),
        query,
        categoricals,
        discretes,
        join_keys,
        numericals,
        targets,
        time_stamps );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

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

void DataFrameManager::get_nrows(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    auto& df = utils::Getter::get( _name, &data_frames() );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( std::to_string( df.nrows() ), _socket );
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

    license_checker().check_mem_size( data_frames(), joined_df.nbytes() );

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

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    multithreading::WriteLock write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    const bool success = df.remove_column( name );

    if ( !success )
        {
            throw std::invalid_argument(
                "Could not remove column. Column named '" + _name +
                "' not found." );
        }

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

void DataFrameManager::to_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto fname = JSON::get_value<std::string>( _cmd, "fname_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Set up the DataFrameReader.

    const auto& df = utils::Getter::get( _name, data_frames() );

    // We are using the bell character (\a) as the quotechar. It is least likely
    // to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    // --------------------------------------------------------------------
    // Set up the CSVWriter.

    auto writer = csv::CSVWriter( fname, reader.colnames(), quotechar, sep );

    // --------------------------------------------------------------------
    // Write data to the file.

    writer.write( &reader );

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::to_db(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------
    // Parse the command.

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Set up the DataFrameReader.

    const auto& df = utils::Getter::get( _name, data_frames() );

    // We are using the bell character (\a) as the quotechar. It is least likely
    // to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    // --------------------------------------------------------------------
    // Create the table.

    const auto statement = csv::StatementMaker::make_statement(
        table_name,
        connector()->dialect(),
        reader.colnames(),
        reader.coltypes() );

    logger().log( statement );

    connector()->execute( statement );

    // --------------------------------------------------------------------
    // Write data to the data base.

    connector()->read( table_name, false, 0, &reader );

    database_manager_->post_tables();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
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

    license_checker().check_mem_size( data_frames(), df.nbytes() );

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

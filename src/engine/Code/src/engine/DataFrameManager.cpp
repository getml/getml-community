#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
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

    receive_data( local_categories, local_join_keys_encoding, &df, _socket );

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

void DataFrameManager::add_float_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    // ------------------------------------------------------------------------

    const auto parser =
        NumOpParser( categories_, join_keys_encoding_, data_frames_ );

    const auto column_view = parser.parse( json_col );

    // ------------------------------------------------------------------------

    const auto make_col = [this, column_view, parser, name, unit, _socket](
                              const std::optional<size_t> _nrows ) {
        auto col = column_view.to_column( 0, _nrows, true );

        col.set_name( name );

        col.set_unit( unit );

        parser.check( col, logger_, _socket );

        return col;
    };

    // ------------------------------------------------------------------------

    auto [df, exists] = utils::Getter::get_if_exists( df_name, &data_frames() );

    if ( exists )
        {
            const auto col = make_col( df->nrows() );

            add_float_column_to_df( role, col, df, &weak_write_lock );

            monitor_->send_tcp(
                "postdataframe",
                df->to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }
    else
        {
            if ( column_view.is_infinite() )
                {
                    throw std::invalid_argument(
                        "Column length could not be inferred! This is because "
                        "you have "
                        "tried to add an infinite length ColumnView to a data "
                        "frame "
                        "that does not contain any columns yet." );
                }

            const auto col = make_col( std::nullopt );

            auto new_df = containers::DataFrame(
                df_name, categories_, join_keys_encoding_ );

            add_float_column_to_df( role, col, &new_df, &weak_write_lock );

            data_frames()[df_name] = new_df;

            data_frames()[df_name].create_indices();

            monitor_->send_tcp(
                "postdataframe",
                new_df.to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }

    // ------------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_float_column(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    auto [df, exists] = utils::Getter::get_if_exists( df_name, &data_frames() );

    // ------------------------------------------------------------------------

    if ( exists )
        {
            recv_and_add_float_column( _cmd, df, &weak_write_lock, _socket );

            monitor_->send_tcp(
                "postdataframe",
                df->to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }
    else
        {
            auto new_df = containers::DataFrame(
                df_name, categories_, join_keys_encoding_ );

            recv_and_add_float_column(
                _cmd, &new_df, &weak_write_lock, _socket );

            data_frames()[df_name] = new_df;

            data_frames()[df_name].create_indices();

            monitor_->send_tcp(
                "postdataframe",
                new_df.to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }

    // ------------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_float_column_to_df(
    const std::string& _role,
    const containers::Column<Float>& _col,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock ) const
{
    license_checker().check_mem_size( data_frames(), _col.nbytes() );

    if ( _weak_write_lock ) _weak_write_lock->upgrade();

    _df->add_float_column( _col, _role );
}

// ------------------------------------------------------------------------

void DataFrameManager::add_int_column_to_df(
    const std::string& _name,
    const std::string& _role,
    const std::string& _unit,
    const std::vector<std::string>& _vec,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    // ------------------------------------------------------------------------

    auto col = containers::Column<Int>( _vec.size() );

    auto encoding = local_join_keys_encoding;

    if ( _role == containers::DataFrame::ROLE_CATEGORICAL )
        {
            encoding = local_categories;
        }

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = ( *encoding )[_vec[i]];
        }

    // ------------------------------------------------------------------------

    col.set_name( _name );

    col.set_unit( _unit );

    // ------------------------------------------------------------------------

    license_checker().check_mem_size( data_frames(), col.nbytes() );

    // ------------------------------------------------------------------------

    assert_true( _weak_write_lock );

    _weak_write_lock->upgrade();

    // ------------------------------------------------------------------------

    _df->add_int_column( col, _role );

    // ------------------------------------------------------------------------

    if ( _role == containers::DataFrame::ROLE_CATEGORICAL )
        {
            categories_->append( *local_categories );
        }
    else if ( _role == containers::DataFrame::ROLE_JOIN_KEY )
        {
            join_keys_encoding_->append( *local_join_keys_encoding );
        }
    else
        {
            assert_true( false );
        }

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_int_column_to_df(
    const std::string& _name,
    const std::string& _role,
    const std::string& _unit,
    const std::vector<std::string>& _vec,
    const std::shared_ptr<containers::Encoding>& _local_categories,
    const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df ) const
{
    // ------------------------------------------------------------------------

    auto col = containers::Column<Int>( _vec.size() );

    auto encoding = _local_join_keys_encoding;

    if ( _role == containers::DataFrame::ROLE_CATEGORICAL )
        {
            encoding = _local_categories;
        }

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = ( *encoding )[_vec[i]];
        }

    // ------------------------------------------------------------------------

    col.set_name( _name );

    col.set_unit( _unit );

    // ------------------------------------------------------------------------

    _df->add_int_column( col, _role );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    auto [df, exists] = utils::Getter::get_if_exists( df_name, &data_frames() );

    // ------------------------------------------------------------------------

    if ( exists )
        {
            recv_and_add_string_column( _cmd, df, &weak_write_lock, _socket );

            monitor_->send_tcp(
                "postdataframe",
                df->to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }
    else
        {
            auto new_df = containers::DataFrame(
                df_name, categories_, join_keys_encoding_ );

            recv_and_add_string_column(
                _cmd, &new_df, &weak_write_lock, _socket );

            data_frames()[df_name] = new_df;

            data_frames()[df_name].create_indices();

            monitor_->send_tcp(
                "postdataframe",
                new_df.to_monitor(),
                communication::Monitor::TIMEOUT_ON );
        }

    // ------------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    // ------------------------------------------------------------------------

    const auto parser =
        CatOpParser( categories_, join_keys_encoding_, data_frames_ );

    // ------------------------------------------------------------------------

    auto new_df =
        containers::DataFrame( df_name, categories_, join_keys_encoding_ );

    auto [df, exists] = utils::Getter::get_if_exists( df_name, &data_frames() );

    if ( !exists )
        {
            df = &new_df;
        }

    // ------------------------------------------------------------------------

    const auto column_view = parser.parse( json_col );

    // ------------------------------------------------------------------------

    const auto make_vec = [this, column_view, parser, name, unit, _socket](
                              const std::optional<size_t> _nrows ) {
        const auto vec = column_view.to_vector( 0, _nrows, true );

        assert_true( vec );

        parser.check( *vec, name, logger_, _socket );

        return *vec;
    };

    // ------------------------------------------------------------------------

    if ( !exists && column_view.is_infinite() )
        {
            throw std::invalid_argument(
                "Column length could not be inferred! This is because you have "
                "tried to add an infinite length ColumnView to a data frame "
                "that does not contain any columns yet." );
        }

    const auto vec =
        exists ? make_vec( df->nrows() ) : make_vec( std::nullopt );

    // ------------------------------------------------------------------------

    if ( role == containers::DataFrame::ROLE_UNUSED ||
         role == containers::DataFrame::ROLE_UNUSED_STRING ||
         role == containers::DataFrame::ROLE_TEXT )
        {
            add_string_column_to_df(
                name, role, unit, vec, df, &weak_write_lock );
        }
    else
        {
            add_int_column_to_df(
                name, role, unit, vec, df, &weak_write_lock, _socket );
        }

    // ------------------------------------------------------------------------

    if ( !exists )
        {
            data_frames()[df_name] = new_df;

            data_frames()[df_name].create_indices();
        }

    // ------------------------------------------------------------------------

    monitor_->send_tcp(
        "postdataframe", df->to_monitor(), communication::Monitor::TIMEOUT_ON );

    communication::Sender::send_string( "Success!", _socket );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column_to_df(
    const std::string& _name,
    const std::string& _role,
    const std::string& _unit,
    const std::vector<std::string>& _vec,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock ) const
{
    // ------------------------------------------------------------------------

    auto col = containers::Column<strings::String>( _vec.size() );

    for ( size_t i = 0; i < _vec.size(); ++i )
        {
            col[i] = strings::String( _vec[i] );
        }

    col.set_name( _name );

    col.set_unit( _unit );

    // ------------------------------------------------------------------------

    license_checker().check_mem_size( data_frames(), col.nbytes() );

    // ------------------------------------------------------------------------

    if ( _weak_write_lock ) _weak_write_lock->upgrade();

    // ------------------------------------------------------------------------

    _df->add_string_column( col, _role );

    // ------------------------------------------------------------------------
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

    check_nrows( aggregation, df_name, df.nrows() );

    auto response = containers::Column<Float>( 1 );

    response[0] =
        AggOpParser(
            categories_, join_keys_encoding_, data_frames_, df.nrows() )
            .aggregate( aggregation );

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

    receive_data( local_categories, local_join_keys_encoding, &df, _socket );

    // --------------------------------------------------------------------
    // Now we upgrade the weak write lock to a strong write lock to commit
    // the changes.

    weak_write_lock.upgrade();

    // --------------------------------------------------------------------

    auto [old_df, exists] =
        utils::Getter::get_if_exists( _name, &data_frames() );

    if ( exists )
        {
            old_df->append( df );
        }
    else
        {
            data_frames()[_name] = df;
        }

    data_frames()[_name].create_indices();

    // --------------------------------------------------------------------
    // Update categories

    categories_->append( *local_categories );

    join_keys_encoding_->append( *local_join_keys_encoding );

    // --------------------------------------------------------------------

    monitor_->send_tcp(
        "postdataframe",
        data_frames()[_name].to_monitor(),
        communication::Monitor::TIMEOUT_ON );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::calc_categorical_column_plots(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto num_bins = JSON::get_value<size_t>( _cmd, "num_bins_" );

    const auto target_name =
        JSON::get_value<std::string>( _cmd, "target_name_" );

    const auto target_role =
        JSON::get_value<std::string>( _cmd, "target_role_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    const auto& df = utils::Getter::get( df_name, data_frames() );

    auto vec = std::vector<strings::String>();

    if ( role == containers::DataFrame::ROLE_CATEGORICAL )
        {
            const auto col = df.int_column( _name, role );

            vec.resize( col.nrows() );

            for ( size_t i = 0; i < col.nrows(); ++i )
                {
                    vec[i] = categories()[col[i]];
                }
        }
    else if ( role == containers::DataFrame::ROLE_JOIN_KEY )
        {
            const auto col = df.int_column( _name, role );

            vec.resize( col.nrows() );

            for ( size_t i = 0; i < col.nrows(); ++i )
                {
                    vec[i] = join_keys_encoding()[col[i]];
                }
        }
    else if ( role == containers::DataFrame::ROLE_TEXT )
        {
            const auto col = df.text( _name );

            vec.resize( col.nrows() );

            for ( size_t i = 0; i < col.nrows(); ++i )
                {
                    vec[i] = col[i];
                }
        }
    else if (
        role == containers::DataFrame::ROLE_UNUSED ||
        role == containers::DataFrame::ROLE_UNUSED_STRING )
        {
            const auto col = df.unused_string( _name );

            vec.resize( col.nrows() );

            for ( size_t i = 0; i < col.nrows(); ++i )
                {
                    vec[i] = col[i];
                }
        }
    else
        {
            throw std::invalid_argument( "Role '" + role + "' not known!" );
        }

    // --------------------------------------------------------------------

    std::vector<Float> targets;

    if ( target_name != "" )
        {
            const auto target_col = df.float_column( target_name, target_role );

            targets =
                std::vector<Float>( target_col.begin(), target_col.end() );
        }

    // --------------------------------------------------------------------

    read_lock.unlock();

    // --------------------------------------------------------------------

    Poco::JSON::Object obj;

    if ( targets.size() == vec.size() )
        {
            obj = metrics::Summarizer::calc_categorical_column_plot(
                num_bins, vec, targets );
        }
    else
        {
            obj = metrics::Summarizer::calc_categorical_column_plot(
                num_bins, vec );
        }

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );

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

    const auto target_name =
        JSON::get_value<std::string>( _cmd, "target_name_" );

    const auto target_role =
        JSON::get_value<std::string>( _cmd, "target_role_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    const auto& df = utils::Getter::get( df_name, data_frames() );

    const auto col = df.float_column( _name, role );

    // --------------------------------------------------------------------

    std::vector<const Float*> targets;

    auto target_col = containers::Column<Float>();

    if ( target_name != "" )
        {
            target_col = df.float_column( target_name, target_role );

            targets.push_back( target_col.data() );
        }

    // --------------------------------------------------------------------

    const containers::Features features = { col.data_ptr() };

    const auto obj = metrics::Summarizer::calculate_feature_plots(
        features, col.nrows(), 1, num_bins, targets );

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

std::pair<size_t, bool> DataFrameManager::check_nrows(
    const Poco::JSON::Object& _obj,
    const std::string& _cmp_df_name,
    const size_t _cmp_nrows ) const
{
    // ------------------------------------------------------------------------

    auto df_name = _cmp_df_name;

    auto nrows = _cmp_nrows;

    auto any_df_found = ( _cmp_df_name != "" );

    // ------------------------------------------------------------------------

    if ( _obj.has( "df_name_" ) )
        {
            df_name = JSON::get_value<std::string>( _obj, "df_name_" );

            if ( df_name == "" )
                {
                    throw std::invalid_argument(
                        "df_name_ cannot be an empty string!" );
                }

            if ( df_name != _cmp_df_name )
                {
                    const auto df =
                        utils::Getter::get( df_name, data_frames() );

                    nrows = df.nrows();

                    any_df_found = true;

                    if ( _cmp_df_name != "" && _cmp_nrows != nrows )
                        {
                            throw std::invalid_argument(
                                "Cannot execute binary operation: '" +
                                _cmp_df_name + "' has " +
                                std::to_string( _cmp_nrows ) + " rows, but '" +
                                df_name + "' has " + std::to_string( nrows ) +
                                " rows." );
                        }
                }
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "operand1_" ) )
        {
            std::tie( nrows, any_df_found ) = check_nrows(
                *JSON::get_object( _obj, "operand1_" ), df_name, nrows );
        }

    if ( _obj.has( "operand2_" ) )
        {
            std::tie( nrows, any_df_found ) = check_nrows(
                *JSON::get_object( _obj, "operand2_" ), df_name, nrows );
        }

    if ( _obj.has( "col_" ) )
        {
            std::tie( nrows, any_df_found ) = check_nrows(
                *JSON::get_object( _obj, "col_" ), df_name, nrows );
        }

    // ------------------------------------------------------------------------

    if ( _obj.has( "aggregations_" ) )
        {
            assert_true( any_df_found );

            const auto arr = JSON::get_array( _obj, "aggregations_" );

            for ( size_t i = 0; i < arr->size(); ++i )
                {
                    const auto ptr = arr->getObject( i );

                    if ( !ptr )
                        {
                            throw std::invalid_argument(
                                "Could not parse aggregations!" );
                        }

                    check_nrows( *ptr, df_name, nrows );
                }
        }

    // ------------------------------------------------------------------------

    return std::make_pair( nrows, any_df_found );

    // ------------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::concat(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------------------------------

    const auto df_names = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "df_names_" ) );

    if ( df_names.size() == 0 )
        {
            throw std::invalid_argument(
                "You should provide at least one data frame to concatenate!" );
        }

    // ------------------------------------------------------------------------

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // ------------------------------------------------------------------------

    auto df_list = std::vector<containers::DataFrame>();

    for ( const auto& name : df_names )
        {
            df_list.push_back( utils::Getter::get( name, data_frames() ) );
        }

    // ------------------------------------------------------------------------

    auto df = df_list.at( 0 ).clone( _name );

    for ( size_t i = 1; i < df_list.size(); ++i )
        {
            df.append( df_list.at( i ) );
        }

    df.create_indices();

    df.check_plausibility();

    // ------------------------------------------------------------------------

    weak_write_lock.upgrade();

    // ------------------------------------------------------------------------

    data_frames()[_name] = df;

    // ------------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    monitor_->send_tcp(
        "postdataframe", df.to_monitor(), communication::Monitor::TIMEOUT_ON );

    // ------------------------------------------------------------------------
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

    auto colnames = std::optional<std::vector<std::string>>();

    if ( _cmd.has( "colnames_" ) )
        {
            colnames = JSON::array_to_vector<std::string>(
                JSON::get_array( _cmd, "colnames_" ) );
        }

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto num_lines_read =
        JSON::get_value<size_t>( _cmd, "num_lines_read_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto skip = JSON::get_value<size_t>( _cmd, "skip_" );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    const auto schema = containers::Schema::from_json( _cmd );

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
        colnames,
        fnames,
        quotechar,
        sep,
        num_lines_read,
        skip,
        time_formats,
        schema );

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

    const auto conn_id = JSON::get_value<std::string>( _cmd, "conn_id_" );

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    const auto schema = containers::Schema::from_json( _cmd );

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

    df.from_db( connector( conn_id ), table_name, schema );

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
    // does not necessarily include unused columns - so we make the optional.

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    const auto schema = containers::Schema::from_json( _cmd );

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

    df.from_json( obj, time_formats, schema );

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

    const auto conn_id = JSON::get_value<std::string>( _cmd, "conn_id_" );

    const auto query = JSON::get_value<std::string>( _cmd, "query_" );

    const auto schema = containers::Schema::from_json( _cmd );

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

    df.from_query( connector( conn_id ), query, schema );

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

void DataFrameManager::from_s3(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    throw std::invalid_argument( "S3 is not supported on Windows!" );
#else

    // --------------------------------------------------------------------
    // Parse the command.

    const auto bucket = JSON::get_value<std::string>( _cmd, "bucket_" );

    auto colnames = std::optional<std::vector<std::string>>();

    if ( _cmd.has( "colnames_" ) )
        {
            colnames = JSON::array_to_vector<std::string>(
                JSON::get_array( _cmd, "colnames_" ) );
        }

    const auto keys =
        JSON::array_to_vector<std::string>( JSON::get_array( _cmd, "keys_" ) );

    const auto region = JSON::get_value<std::string>( _cmd, "region_" );

    const auto num_lines_read =
        JSON::get_value<size_t>( _cmd, "num_lines_read_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto skip = JSON::get_value<size_t>( _cmd, "skip_" );

    const auto time_formats = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "time_formats_" ) );

    const auto schema = containers::Schema::from_json( _cmd );

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

    df.from_s3(
        bucket,
        colnames,
        keys,
        region,
        sep,
        num_lines_read,
        skip,
        time_formats,
        schema );

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

#endif
}

// ------------------------------------------------------------------------

void DataFrameManager::from_view(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    const bool _append,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto view = *JSON::get_object( _cmd, "view_" );

    // --------------------------------------------------------------------
    // We need the weak write lock for the categories and join keys encoding.

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    // --------------------------------------------------------------------

    auto local_categories =
        std::make_shared<containers::Encoding>( categories_ );

    auto local_join_keys_encoding =
        std::make_shared<containers::Encoding>( join_keys_encoding_ );

    // --------------------------------------------------------------------

    auto df =
        ViewParser( local_categories, local_join_keys_encoding, data_frames_ )
            .parse( view )
            .clone( _name );

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

    const auto column_view =
        BoolOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    if ( column_view.is_infinite() )
        {
            throw std::invalid_argument(
                "The length of the column view is infinite! You can look at "
                "the column view, "
                "but you cannot retrieve it." );
        }

    const auto data_ptr = column_view.to_vector( 0, std::nullopt, false );

    assert_true( data_ptr );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_boolean_column( *data_ptr, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<size_t>( _cmd, "length_" );

    const auto start = JSON::get_value<size_t>( _cmd, "start_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        BoolOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    const auto data_ptr = column_view.to_vector( start, length, false );

    assert_true( data_ptr );

    const auto nrows = std::holds_alternative<size_t>( column_view.nrows() )
                           ? std::get<size_t>( column_view.nrows() )
                           : length;

    const auto col_str = make_column_string<bool>(
        draw, nrows, data_ptr->begin(), data_ptr->end() );

    communication::Sender::send_string( col_str, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column_nrows(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        BoolOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    read_lock.unlock();

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( column_view.nrows_to_str(), _socket );
}
// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        CatOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    if ( column_view.is_infinite() )
        {
            throw std::invalid_argument(
                "The length of the column view is infinite! You can look at "
                "the column view, "
                "but you cannot retrieve it." );
        }

    const auto data_ptr = column_view.to_vector( 0, std::nullopt, false );

    assert_true( data_ptr );

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_categorical_column( *data_ptr, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<size_t>( _cmd, "length_" );

    const auto start = JSON::get_value<size_t>( _cmd, "start_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        CatOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    const auto data_ptr = column_view.to_vector( start, length, false );

    assert_true( data_ptr );

    const auto nrows = std::holds_alternative<size_t>( column_view.nrows() )
                           ? std::get<size_t>( column_view.nrows() )
                           : length;

    const auto col_str = make_column_string<std::string>(
        draw, nrows, data_ptr->begin(), data_ptr->end() );

    communication::Sender::send_string( col_str, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column_nrows(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        CatOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    read_lock.unlock();

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( column_view.nrows_to_str(), _socket );
}
// ------------------------------------------------------------------------

void DataFrameManager::get_column(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        NumOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    if ( column_view.is_infinite() )
        {
            throw std::invalid_argument(
                "The length of the column view is infinite! You can look at "
                "the column view, "
                "but you cannot retrieve it." );
        }

    const auto col = column_view.to_column( 0, std::nullopt, false );

    read_lock.unlock();

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

void DataFrameManager::get_column_nrows(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        NumOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    read_lock.unlock();

    communication::Sender::send_string( "Found!", _socket );

    communication::Sender::send_string( column_view.nrows_to_str(), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_html(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto max_rows = JSON::get_value<std::int32_t>( _cmd, "max_rows_" );

    const auto border = JSON::get_value<std::int32_t>( _cmd, "border_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto& df = utils::Getter::get( _name, &data_frames() );

    const auto str = df.get_html( max_rows, border );

    read_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( str, _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_string(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto& df = utils::Getter::get( _name, &data_frames() );

    const auto str = df.get_string( 20 );

    read_lock.unlock();

    communication::Sender::send_string( str, _socket );
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

            if ( type == "StringColumn.get" )
                {
                    get_categorical_column( name, cmd, _socket );
                }
            else if ( type == "FloatColumn.get" )
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

void DataFrameManager::get_float_column_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<size_t>( _cmd, "length_" );

    const auto start = JSON::get_value<size_t>( _cmd, "start_" );

    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        NumOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    const auto col = column_view.to_column( start, length, false );

    const auto nrows = std::holds_alternative<size_t>( column_view.nrows() )
                           ? std::get<size_t>( column_view.nrows() )
                           : length;

    const auto col_str =
        make_column_string<Float>( draw, nrows, col.begin(), col.end() );

    communication::Sender::send_string( col_str, _socket );
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

void DataFrameManager::get_unit(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        NumOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    read_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( column_view.unit(), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::get_unit_categorical(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto json_col = *JSON::get_object( _cmd, "col_" );

    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto column_view =
        CatOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( json_col );

    read_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( column_view.unit(), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::group_by(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto aggregations = *JSON::get_array( _cmd, "aggregations_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto key_name = JSON::get_value<std::string>( _cmd, "key_name_" );

    multithreading::WeakWriteLock weak_write_lock( read_write_lock_ );

    const auto df = utils::Getter::get( df_name, data_frames() );

    check_nrows( _cmd, df_name, df.nrows() );

    const auto grouped_df =
        GroupByParser( categories_, join_keys_encoding_, { df } )
            .group_by( _name, key_name, aggregations );

    weak_write_lock.upgrade();

    data_frames()[_name] = grouped_df;

    monitor_->send_tcp(
        "postdataframe",
        grouped_df.to_monitor(),
        communication::Monitor::TIMEOUT_ON );

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

    monitor_->send_tcp(
        "postdataframe",
        data_frames()[_name].to_monitor(),
        communication::Monitor::TIMEOUT_ON );

    weak_write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::last_change(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( _name, data_frames() );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( df.last_change(), _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::receive_data(
    const std::shared_ptr<containers::Encoding>& _local_categories,
    const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket ) const
{
    while ( true )
        {
            Poco::JSON::Object cmd =
                communication::Receiver::recv_cmd( logger_, _socket );

            const auto type = JSON::get_value<std::string>( cmd, "type_" );

            const auto name = JSON::get_value<std::string>( cmd, "name_" );

            if ( type == FLOAT_COLUMN )
                {
                    recv_and_add_float_column( cmd, _df, nullptr, _socket );

                    communication::Sender::send_string( "Success!", _socket );
                }
            else if ( type == STRING_COLUMN )
                {
                    recv_and_add_string_column(
                        cmd,
                        _local_categories,
                        _local_join_keys_encoding,
                        _df,
                        _socket );

                    communication::Sender::send_string( "Success!", _socket );
                }
            else if ( type == "DataFrame.close" )
                {
                    license_checker().check_mem_size(
                        data_frames(), _df->nbytes() );

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

void DataFrameManager::recv_and_add_float_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket ) const
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    auto col = communication::Receiver::recv_column( _socket );

    col.set_name( name );

    add_float_column_to_df( role, col, _df, _weak_write_lock );
}

// ------------------------------------------------------------------------

void DataFrameManager::recv_and_add_string_column(
    const Poco::JSON::Object& _cmd,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket )
{
    assert_true( _weak_write_lock );

    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto str_col = communication::Receiver::recv_string_column( _socket );

    if ( role == containers::DataFrame::ROLE_UNUSED ||
         role == containers::DataFrame::ROLE_UNUSED_STRING ||
         role == containers::DataFrame::ROLE_TEXT )
        {
            add_string_column_to_df(
                name, role, "", str_col, _df, _weak_write_lock );
        }
    else
        {
            add_int_column_to_df(
                name, role, "", str_col, _df, _weak_write_lock, _socket );
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::recv_and_add_string_column(
    const Poco::JSON::Object& _cmd,
    const std::shared_ptr<containers::Encoding>& _local_categories,
    const std::shared_ptr<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df,
    Poco::Net::StreamSocket* _socket ) const
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto name = JSON::get_value<std::string>( _cmd, "name_" );

    const auto str_col = communication::Receiver::recv_string_column( _socket );

    if ( role == containers::DataFrame::ROLE_UNUSED ||
         role == containers::DataFrame::ROLE_UNUSED_STRING ||
         role == containers::DataFrame::ROLE_TEXT )
        {
            add_string_column_to_df( name, role, "", str_col, _df, nullptr );
        }
    else
        {
            add_int_column_to_df(
                name,
                role,
                "",
                str_col,
                _local_categories,
                _local_join_keys_encoding,
                _df );
        }
}

// ------------------------------------------------------------------------

void DataFrameManager::refresh(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    multithreading::ReadLock read_lock( read_write_lock_ );

    const auto df = utils::Getter::get( _name, data_frames() );

    Poco::JSON::Object encodings = df.refresh();

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

    monitor_->send_tcp(
        "postdataframe", df.to_monitor(), communication::Monitor::TIMEOUT_ON );

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    multithreading::WriteLock write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    auto column = df.float_column( _name, role );

    column.set_unit( unit );

    df.add_float_column( column, role );

    monitor_->send_tcp(
        "postdataframe", df.to_monitor(), communication::Monitor::TIMEOUT_ON );

    write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit_categorical(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto role = JSON::get_value<std::string>( _cmd, "role_" );

    const auto df_name = JSON::get_value<std::string>( _cmd, "df_name_" );

    const auto unit = JSON::get_value<std::string>( _cmd, "unit_" );

    multithreading::WriteLock write_lock( read_write_lock_ );

    auto& df = utils::Getter::get( df_name, &data_frames() );

    if ( role == containers::DataFrame::ROLE_UNUSED ||
         role == containers::DataFrame::ROLE_UNUSED_STRING )
        {
            auto column = df.unused_string( _name );
            column.set_unit( unit );
            df.add_string_column( column, role );
        }
    else if ( role == containers::DataFrame::ROLE_TEXT )
        {
            auto column = df.text( _name );
            column.set_unit( unit );
            df.add_string_column( column, role );
        }
    else
        {
            auto column = df.int_column( _name, role );
            column.set_unit( unit );
            df.add_int_column( column, role );
        }

    monitor_->send_tcp(
        "postdataframe", df.to_monitor(), communication::Monitor::TIMEOUT_ON );

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

    const auto batch_size = JSON::get_value<size_t>( _cmd, "batch_size_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Set up the DataFrameReader.

    const auto& df = utils::Getter::get( _name, data_frames() );

    // We are using the bell character (\a) as the quotechar. It is least
    // likely to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    const auto colnames = reader.colnames();

    // --------------------------------------------------------------------

    size_t fnum = 0;

    while ( !reader.eof() )
        {
            auto fnum_str = std::to_string( ++fnum );

            if ( fnum_str.size() < 5 )
                {
                    fnum_str =
                        std::string( 5 - fnum_str.size(), '0' ) + fnum_str;
                }

            const auto current_fname = batch_size == 0
                                           ? fname + ".csv"
                                           : fname + "-" + fnum_str + ".csv";

            auto writer = io::CSVWriter(
                current_fname, batch_size, colnames, quotechar, sep );

            writer.write( &reader );
        }

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

    const auto conn_id = JSON::get_value<std::string>( _cmd, "conn_id_" );

    const auto table_name = JSON::get_value<std::string>( _cmd, "table_name_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Set up the DataFrameReader.

    const auto& df = utils::Getter::get( _name, data_frames() );

    // We are using the bell character (\a) as the quotechar. It is least
    // likely to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    // --------------------------------------------------------------------
    // Create the table.

    const auto conn = connector( conn_id );

    assert_true( conn );

    const auto statement = io::StatementMaker::make_statement(
        table_name,
        conn->dialect(),
        conn->describe(),
        reader.colnames(),
        reader.coltypes() );

    logger().log( statement );

    conn->execute( statement );

    // --------------------------------------------------------------------
    // Write data to the data base.

    conn->read( table_name, 0, &reader );

    database_manager_->post_tables();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------

void DataFrameManager::to_s3(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    throw std::invalid_argument( "S3 is not supported on Windows!" );
#else

    // --------------------------------------------------------------------

    const auto batch_size = JSON::get_value<size_t>( _cmd, "batch_size_" );

    const auto bucket = JSON::get_value<std::string>( _cmd, "bucket_" );

    const auto key = JSON::get_value<std::string>( _cmd, "key_" );

    const auto region = JSON::get_value<std::string>( _cmd, "region_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    // --------------------------------------------------------------------

    multithreading::ReadLock read_lock( read_write_lock_ );

    // --------------------------------------------------------------------
    // Set up the DataFrameReader.

    const auto& df = utils::Getter::get( _name, data_frames() );

    // We are using the bell character (\a) as the quotechar. It is least
    // likely to appear in any field.
    auto reader = containers::DataFrameReader(
        df, categories_, join_keys_encoding_, '\a', '|' );

    const auto colnames = reader.colnames();

    // --------------------------------------------------------------------

    size_t fnum = 0;

    while ( !reader.eof() )
        {
            auto tfile = Poco::TemporaryFile( options_.temp_dir() );

            auto writer =
                io::CSVWriter( tfile.path(), batch_size, colnames, "\"", sep );

            writer.write( &reader );

            auto fnum_str = std::to_string( ++fnum );

            if ( fnum_str.size() < 5 )
                {
                    fnum_str =
                        std::string( 5 - fnum_str.size(), '0' ) + fnum_str;
                }

            const auto current_key =
                batch_size == 0 ? key + ".csv" : key + "-" + fnum_str + ".csv";

            goutils::S3::upload_file(
                tfile.path(), bucket, current_key, region );
        }

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------

#endif
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

    const auto condition =
        BoolOpParser( categories_, join_keys_encoding_, data_frames_ )
            .parse( condition_json )
            .to_vector( 0, df.nrows(), true );

    assert_true( condition );

    // --------------------------------------------------------------------

    df.where( *condition );

    df.set_name( new_df_name );

    license_checker().check_mem_size( data_frames(), df.nbytes() );

    // --------------------------------------------------------------------

    weak_write_lock.upgrade();

    data_frames()[new_df_name] = df;

    monitor_->send_tcp(
        "postdataframe", df.to_monitor(), communication::Monitor::TIMEOUT_ON );

    weak_write_lock.unlock();

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

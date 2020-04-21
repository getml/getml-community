#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

void DatabaseManager::drop_table(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    connector()->drop_table( _name );

    post_tables();

    communication::Sender::send_string( "Success!", _socket );
}

// ----------------------------------------------------------------------------

void DatabaseManager::execute( Poco::Net::StreamSocket* _socket )
{
    const auto query = communication::Receiver::recv_string( _socket );

    connector()->execute( query );

    post_tables();

    communication::Sender::send_string( "Success!", _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto query = communication::Receiver::recv_string( _socket );

    auto db_iterator = connector()->select( query );

    const auto colnames = db_iterator->colnames();

    std::vector<Poco::JSON::Array> columns( colnames.size() );

    while ( !db_iterator->end() )
        {
            for ( auto& col : columns )
                {
                    col.add( db_iterator->get_string() );
                }
        }

    Poco::JSON::Object obj;

    for ( size_t i = 0; i < columns.size(); ++i )
        {
            obj.set( colnames[i], columns[i] );
        }

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_colnames(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const auto colnames = connector()->get_colnames( _name );

    std::string array = "[";

    for ( auto& col : colnames )
        {
            array += std::string( "\"" ) + col + "\",";
        }

    if ( array.size() > 1 )
        {
            array.back() = ']';
        }
    else
        {
            array += ']';
        }

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( array, _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_content(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    const auto draw = JSON::get_value<Int>( _cmd, "draw_" );

    const auto length = JSON::get_value<Int>( _cmd, "length_" );

    const auto start = JSON::get_value<Int>( _cmd, "start_" );

    auto obj = connector()->get_content( _name, draw, start, length );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( JSON::stringify( obj ), _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::get_nrows(
    const std::string& _name, Poco::Net::StreamSocket* _socket )
{
    const std::int32_t nrows = connector()->get_nrows( _name );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send( sizeof( std::int32_t ), &nrows, _socket );
}

// ------------------------------------------------------------------------

void DatabaseManager::list_tables( Poco::Net::StreamSocket* _socket )
{
    const auto array = post_tables();

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( array, _socket );
}

// ----------------------------------------------------------------------------

void DatabaseManager::new_db(
    const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
{
    const auto password = communication::Receiver::recv_string( _socket );

    multithreading::WriteLock write_lock( read_write_lock_ );

    connector_ = database::DatabaseParser::parse( _cmd, password );

    write_lock.unlock();

    post_tables();

    communication::Sender::send_string( "Success!", _socket );
}

// ----------------------------------------------------------------------------

std::string DatabaseManager::post_tables()
{
    const auto tables = connector()->list_tables();

    std::string array = "[";

    for ( auto& table : tables )
        {
            array += std::string( "\"" ) + table + "\",";
        }

    if ( array.size() > 1 )
        {
            array.back() = ']';
        }
    else
        {
            array += ']';
        }

    monitor_->send( "postdatabasetables", array );

    return array;
}

// ----------------------------------------------------------------------------

void DatabaseManager::read_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

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

    auto limit = num_lines_read > 0 ? num_lines_read + skip : num_lines_read;

    if ( !colnames && limit > 0 )
        {
            ++limit;
        }

    // --------------------------------------------------------------------

    for ( const auto& fname : fnames )
        {
            auto reader =
                io::CSVReader( colnames, fname, limit, quotechar[0], sep[0] );

            connector()->read( _name, skip, &reader );

            logger().log( "Read '" + fname + "'." );
        }

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DatabaseManager::read_s3(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket )
{
    // --------------------------------------------------------------------

    const auto bucket = JSON::get_value<std::string>( _cmd, "bucket_" );

    auto colnames = std::optional<std::vector<std::string>>();

    if ( _cmd.has( "colnames_" ) )
        {
            colnames = JSON::array_to_vector<std::string>(
                JSON::get_array( _cmd, "colnames_" ) );
        }

    const auto keys =
        JSON::array_to_vector<std::string>( JSON::get_array( _cmd, "keys_" ) );

    const auto num_lines_read =
        JSON::get_value<size_t>( _cmd, "num_lines_read_" );

    const auto region = JSON::get_value<std::string>( _cmd, "region_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto skip = JSON::get_value<size_t>( _cmd, "skip_" );

    // --------------------------------------------------------------------

    if ( sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator (sep) must consist of exactly one character!" );
        }

    auto limit = num_lines_read > 0 ? num_lines_read + skip : num_lines_read;

    if ( !colnames && limit > 0 )
        {
            ++limit;
        }

    // --------------------------------------------------------------------

    for ( const auto& key : keys )
        {
            auto reader =
                io::S3Reader( bucket, colnames, key, limit, region, sep[0] );

            connector()->read( _name, skip, &reader );

            logger().log( "Read '" + key + "'." );
        }

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_csv(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    auto colnames = std::optional<std::vector<std::string>>();

    if ( _cmd.has( "colnames_" ) )
        {
            colnames = JSON::array_to_vector<std::string>(
                JSON::get_array( _cmd, "colnames_" ) );
        }

    const auto fnames = JSON::array_to_vector<std::string>(
        JSON::get_array( _cmd, "fnames_" ) );

    const auto num_lines_sniffed =
        JSON::get_value<size_t>( _cmd, "num_lines_sniffed_" );

    const auto quotechar = JSON::get_value<std::string>( _cmd, "quotechar_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto skip = JSON::get_value<size_t>( _cmd, "skip_" );

    const auto dialect = _cmd.has( "dialect_" )
                             ? JSON::get_value<std::string>( _cmd, "dialect_" )
                             : connector()->dialect();

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

    auto sniffer = io::CSVSniffer(
        colnames,
        dialect,
        fnames,
        num_lines_sniffed,
        quotechar[0],
        sep[0],
        skip,
        _name );

    const auto create_table_statement = sniffer.sniff();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( create_table_statement, _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_s3(
    const std::string& _name,
    const Poco::JSON::Object& _cmd,
    Poco::Net::StreamSocket* _socket ) const
{
    // --------------------------------------------------------------------

    const auto bucket = JSON::get_value<std::string>( _cmd, "bucket_" );

    auto colnames = std::optional<std::vector<std::string>>();

    if ( _cmd.has( "colnames_" ) )
        {
            colnames = JSON::array_to_vector<std::string>(
                JSON::get_array( _cmd, "colnames_" ) );
        }

    const auto keys =
        JSON::array_to_vector<std::string>( JSON::get_array( _cmd, "keys_" ) );

    const auto num_lines_sniffed =
        JSON::get_value<size_t>( _cmd, "num_lines_sniffed_" );

    const auto region = JSON::get_value<std::string>( _cmd, "region_" );

    const auto sep = JSON::get_value<std::string>( _cmd, "sep_" );

    const auto skip = JSON::get_value<size_t>( _cmd, "skip_" );

    const auto dialect = _cmd.has( "dialect_" )
                             ? JSON::get_value<std::string>( _cmd, "dialect_" )
                             : connector()->dialect();

    // --------------------------------------------------------------------

    if ( sep.size() != 1 )
        {
            throw std::invalid_argument(
                "The separator (sep) must consist of exactly one character!" );
        }

    // --------------------------------------------------------------------

    auto sniffer = io::S3Sniffer(
        bucket,
        colnames,
        dialect,
        keys,
        num_lines_sniffed,
        region,
        sep[0],
        skip,
        _name );

    const auto create_table_statement = sniffer.sniff();

    // --------------------------------------------------------------------

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( create_table_statement, _socket );

    // --------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_table(
    const std::string& _table_name, Poco::Net::StreamSocket* _socket ) const
{
    if ( !connector_ )
        {
            throw std::invalid_argument( "No connector set!" );
        }

    const auto kwargs =
        database::DatabaseSniffer::sniff( connector_, _table_name );

    communication::Sender::send_string( "Success!", _socket );

    communication::Sender::send_string( kwargs, _socket );
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

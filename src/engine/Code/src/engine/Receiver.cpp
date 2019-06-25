#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

containers::Column<Int> Receiver::recv_categorical_column(
    containers::Encoding *_encoding, Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Init Column

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    if ( std::get<1>( shape ) != 1 )
        {
            throw std::runtime_error(
                "Data must contain only a single column!" );
        }

    containers::Column<Int> col( static_cast<size_t>( std::get<0>( shape ) ) );

    // ------------------------------------------------
    // Receive strings and map to int Column

    for ( size_t i = 0; i < col.nrows(); ++i )
        {
            col[i] = ( *_encoding )[Receiver::recv_string( _socket )];
        }

    // ------------------------------------------------

    return col;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object Receiver::recv_cmd(
    const std::shared_ptr<const monitoring::Logger> &_logger,
    Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Receive string

    const auto str = Receiver::recv_string( _socket );

    // ------------------------------------------------
    // Print on screen

    if ( _logger )
        {
            std::stringstream cmd_log;

            cmd_log << "Command sent by " << _socket->peerAddress().toString()
                    << ":" << std::endl
                    << str;

            _logger->log( cmd_log.str() );
        }

    // ------------------------------------------------
    // Interpret command string - note that all
    // commands are always JSON form.

    Poco::JSON::Parser parser;

    auto obj = parser.parse( str ).extract<Poco::JSON::Object::Ptr>();

    return *obj;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

containers::Column<Float> Receiver::recv_column(
    Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Init Column

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    if ( std::get<1>( shape ) != 1 )
        {
            throw std::runtime_error(
                "Data must contain only a single column!" );
        }

    auto col = containers::Column<Float>(
        static_cast<size_t>( std::get<0>( shape ) ) );

    // ------------------------------------------------
    // Fill with data

    recv<Float>(
        sizeof( Float ) * static_cast<ULong>( std::get<0>( shape ) ) *
            static_cast<ULong>( std::get<1>( shape ) ),
        _socket,
        col.data() );

    // ------------------------------------------------

    return col;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

std::string Receiver::recv_string( Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Init string

    std::vector<Int> str_length( 1 );

    Receiver::recv<Int>( sizeof( Int ), _socket, str_length.data() );

    std::string str( str_length[0], '0' );

    const auto str_length_long = static_cast<ULong>( str_length[0] );

    // ------------------------------------------------
    // Receive string content from the client

    Receiver::recv<char>(
        str_length_long,  // sizeof(char) = 1 by definition
        _socket,
        &str[0] );

    // ------------------------------------------------

    return str;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

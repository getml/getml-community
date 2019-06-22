#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

containers::Matrix<Int> Receiver::recv_categorical_matrix(
    containers::Encoding *_encoding, Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    if ( std::get<1>( shape ) < 0 )
        {
            throw std::runtime_error(
                "Number of columns can not be negative!" );
        }

    containers::Matrix<Int> matrix(
        static_cast<size_t>( std::get<0>( shape ) ),
        static_cast<size_t>( std::get<1>( shape ) ) );

    // ------------------------------------------------
    // Receive strings and map to int matrix

    for ( size_t i = 0; i < matrix.nrows(); ++i )
        {
            for ( size_t j = 0; j < matrix.ncols(); ++j )
                {
                    matrix( i, j ) =
                        ( *_encoding )[Receiver::recv_string( _socket )];
                }
        }

    // ------------------------------------------------

    return matrix;

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

containers::Matrix<Float> Receiver::recv_matrix(
    Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    if ( std::get<1>( shape ) < 0 )
        {
            throw std::runtime_error(
                "Number of columns can not be negative!" );
        }

    containers::Matrix<Float> matrix(
        static_cast<size_t>( std::get<0>( shape ) ),
        static_cast<size_t>( std::get<1>( shape ) ) );

    // ------------------------------------------------
    // Fill with data

    recv<Float>(
        sizeof( Float ) *
            static_cast<ULong>( std::get<0>( shape ) ) *
            static_cast<ULong>( std::get<1>( shape ) ),
        _socket,
        matrix.data() );

    // ------------------------------------------------

    return matrix;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

std::string Receiver::recv_string( Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Init string

    std::vector<Int> str_length( 1 );

    Receiver::recv<Int>(
        sizeof( Int ), _socket, str_length.data() );

    std::string str( str_length[0], '0' );

    const auto str_length_long =
        static_cast<ULong>( str_length[0] );

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

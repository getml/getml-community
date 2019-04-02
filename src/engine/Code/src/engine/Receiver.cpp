#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

/*containers::Matrix<SQLNET_INT> Receiver::recv_categorical_matrix(
    Poco::Net::StreamSocket& _socket, containers::Encoding& _encoding )
{
    // ------------------------------------------------
    // Receive shape

    std::array<SQLNET_INT, 2> shape;

    recv<SQLNET_INT>( _socket, sizeof( SQLNET_INT ) * 2, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( shape[0] <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    containers::Matrix<SQLNET_INT> matrix(
        std::get<0>( shape ), std::get<1>( shape ) );

    // ------------------------------------------------
    // Receive strings and map to int matrix

    for ( SQLNET_INT i = 0; i < matrix.nrows(); ++i )
        {
            for ( SQLNET_INT j = 0; j < matrix.ncols(); ++j )
                {
                    matrix( i, j ) =
                        _encoding[Receiver::recv_string( _socket )];
                }
        }

    // ------------------------------------------------

    return matrix;

    // ------------------------------------------------
}*/

// ------------------------------------------------------------------------

Poco::JSON::Object Receiver::recv_cmd(
    Poco::Net::StreamSocket* _socket/*,
    const std::shared_ptr<const logging::Logger>& _logger,
    const bool _log*/ )
{
    // ------------------------------------------------
    // Receive string

    const auto str = Receiver::recv_string( _socket );

    // ------------------------------------------------
    // Print on screen

    /* if ( _log )
         {
             std::stringstream cmd_log;

             cmd_log << "Command sent by " << _socket.peerAddress().toString()
                     << ":" << std::endl
                     << str;

             _logger->log( cmd_log.str() );
         }*/

    // ------------------------------------------------
    // Interpret command string - note that all
    // commands are always JSON form

    Poco::JSON::Parser parser;

    auto obj = parser.parse( str ).extract<Poco::JSON::Object::Ptr>();

    return *obj;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

/*containers::Matrix<SQLNET_FLOAT> Receiver::recv_matrix(
    Poco::Net::StreamSocket& _socket, bool _scatter )
{
    // ------------------------------------------------
    // Receive shape

    std::array<SQLNET_INT, 2> shape;

    recv<SQLNET_INT>( _socket, sizeof( SQLNET_INT ) * 2, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( shape[0] <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    containers::Matrix<SQLNET_FLOAT> matrix(
        std::get<0>( shape ), std::get<1>( shape ) );

    // ------------------------------------------------
    // Fill with data

    recv<SQLNET_FLOAT>(
        _socket,
        sizeof( SQLNET_FLOAT ) *
            static_cast<SQLNET_UNSIGNED_LONG>( std::get<0>( shape ) ) *
            static_cast<SQLNET_UNSIGNED_LONG>( std::get<1>( shape ) ),
        matrix.data() );

    // ------------------------------------------------

    return matrix;
}*/

// ------------------------------------------------------------------------

std::string Receiver::recv_string( Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Init string

    std::vector<ENGINE_INT> str_length( 1 );

    Receiver::recv<ENGINE_INT>(
        sizeof( ENGINE_INT ), _socket, str_length.data() );

    std::string str( str_length[0], '0' );

    const auto str_length_long =
        static_cast<ENGINE_UNSIGNED_LONG>( str_length[0] );

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

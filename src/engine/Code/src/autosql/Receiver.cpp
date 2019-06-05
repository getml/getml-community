#include "engine/engine.hpp"

namespace autosql
{
namespace engine
{
// ------------------------------------------------------------------------

containers::Matrix<AUTOSQL_INT> Receiver::recv_categorical_matrix(
    Poco::Net::StreamSocket& _socket, containers::Encoding& _encoding )
{
    // ------------------------------------------------
    // Receive shape

    std::array<AUTOSQL_INT, 2> shape;

    recv<AUTOSQL_INT>( _socket, sizeof( AUTOSQL_INT ) * 2, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( shape[0] <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    containers::Matrix<AUTOSQL_INT> matrix(
        std::get<0>( shape ), std::get<1>( shape ) );

    // ------------------------------------------------
    // Receive strings and map to int matrix

    for ( AUTOSQL_INT i = 0; i < matrix.nrows(); ++i )
        {
            for ( AUTOSQL_INT j = 0; j < matrix.ncols(); ++j )
                {
                    matrix( i, j ) =
                        _encoding[Receiver::recv_string( _socket )];
                }
        }

    // ------------------------------------------------

    return matrix;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

Poco::JSON::Object Receiver::recv_cmd(
    Poco::Net::StreamSocket& _socket,
    const std::shared_ptr<const logging::Logger>& _logger,
    const bool _log )
{
    // ------------------------------------------------
    // Receive string

    const auto str = Receiver::recv_string( _socket );

    // ------------------------------------------------
    // Print on screen

    if ( _log )
        {
            std::stringstream cmd_log;

            cmd_log << "Command sent by " << _socket.peerAddress().toString()
                    << ":" << std::endl
                    << str;

            _logger->log( cmd_log.str() );
        }

#ifdef AUTOSQL_MULTINODE_MPI

    // ------------------------------------------------
    // If there are other processes, broadcast the command
    // string to them

    int num_processes;

    MPI_Comm_size( MPI_COMM_WORLD, &num_processes );

    for ( int dest = 1; dest < num_processes; ++dest )
        {
            MPI_Send(
                &str[0],        // buf
                str_length[0],  // count
                MPI_CHAR,       // datatype
                dest,           // dest
                0,              // tag
                MPI_COMM_WORLD  // comm
            );
        }

    MPI_Barrier( MPI_COMM_WORLD );

#endif /* AUTOSQL_MULTINODE_MPI */

    // ------------------------------------------------
    // Interpret command string - note that all
    // commands are always JSON form

    Poco::JSON::Parser parser;

    auto obj = parser.parse( str ).extract<Poco::JSON::Object::Ptr>();

    return *obj;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

#ifdef AUTOSQL_MULTINODE_MPI

Poco::JSON::Object Receiver::recv_cmd()
{
    // ------------------------------------------------
    // This is a 'sleeping barrier'. It simply waits for a
    // command string to come in. Once a string arrives,
    // it checks the length of the string and then receives
    // it from the root process.

    int length;

    while ( true )
        {
            std::this_thread::sleep_for( std::chrono::milliseconds( 10 ) );

            int flag = 0;

            MPI_Status status;

            MPI_Iprobe(
                0,               // source
                0,               // tag
                MPI_COMM_WORLD,  // comm
                &flag,           // flag
                &status          // status
            );

            if ( flag )
                {
                    MPI_Get_count( &status, MPI_CHAR, &length );

                    break;
                }
        }

    // ------------------------------------------------
    // MPI_Get_count determines the length of the string
    // to come in. Now, we can receive the actual string.

    MPI_Status status;

    std::string str( length, '0' );

    MPI_Recv(
        &str[0],         // buf
        length,          // count
        MPI_CHAR,        // datatype
        0,               // source
        0,               // tag
        MPI_COMM_WORLD,  // comm
        &status          // status
    );

    MPI_Barrier( MPI_COMM_WORLD );

    // ------------------------------------------------
    // Interpret command string - note that all
    // commands are always JSON form

    Poco::JSON::Parser parser;

    auto obj = parser.parse( str ).extract<Poco::JSON::Object::Ptr>();

    return *obj;
}

#endif /* AUTOSQL_MULTINODE_MPI */

// ------------------------------------------------------------------------

containers::Matrix<AUTOSQL_FLOAT> Receiver::recv_matrix(
    Poco::Net::StreamSocket& _socket, bool _scatter )
{
    // ------------------------------------------------
    // Receive shape

    std::array<AUTOSQL_INT, 2> shape;

    recv<AUTOSQL_INT>( _socket, sizeof( AUTOSQL_INT ) * 2, shape.data() );

    // ------------------------------------------------
    // Init matrix

    if ( shape[0] <= 0 )
        {
            throw std::runtime_error(
                "Your data frame must contain at least one row!" );
        }

    containers::Matrix<AUTOSQL_FLOAT> matrix(
        std::get<0>( shape ), std::get<1>( shape ) );

    // ------------------------------------------------
    // Fill with data

    recv<AUTOSQL_FLOAT>(
        _socket,
        sizeof( AUTOSQL_FLOAT ) *
            static_cast<AUTOSQL_UNSIGNED_LONG>( std::get<0>( shape ) ) *
            static_cast<AUTOSQL_UNSIGNED_LONG>( std::get<1>( shape ) ),
        matrix.data() );

#ifdef AUTOSQL_MULTINODE_MPI

    // ------------------------------------------------
    // Scatter matrix, if applicable

    if ( _scatter )
        {
            matrix = matrix.scatter();
        }

#endif /* AUTOSQL_MULTINODE_MPI */

    // ------------------------------------------------

    return matrix;
}

// ------------------------------------------------------------------------

std::string Receiver::recv_string( Poco::Net::StreamSocket& _socket )
{
    // ------------------------------------------------
    // Init string

    std::vector<AUTOSQL_INT> str_length( 1 );

    Receiver::recv<AUTOSQL_INT>(
        _socket, sizeof( AUTOSQL_INT ), str_length.data() );

    std::string str( str_length[0], '0' );

    const auto str_length_long =
        static_cast<AUTOSQL_UNSIGNED_LONG>( str_length[0] );

    // ------------------------------------------------
    // Receive string content from the client

    Receiver::recv<char>(
        _socket,
        str_length_long,  // sizeof(char) = 1 by definition
        &str[0] );

    // ------------------------------------------------

    return str;

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace engine
}  // namespace autosql

#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// -----------------------------------------------------------------------------

Poco::JSON::Object Receiver::recv_cmd(
    const std::shared_ptr<const communication::Logger> &_logger,
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

    const auto obj = parser.parse( str ).extract<Poco::JSON::Object::Ptr>();

    return *obj;

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------

containers::Features Receiver::recv_features( Poco::Net::StreamSocket *_socket )
{
    // -------------------------------------------------------------------------
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    const auto nrows = std::get<0>( shape );
    const auto ncols = std::get<1>( shape );

    // -------------------------------------------------------------------------
    // Init features

    auto features = containers::Features( ncols );

    for ( auto &f : features )
        {
            f = std::make_shared<std::vector<Float>>( nrows );
        }

    // -------------------------------------------------------------------------
    // Recv actual data

    const ULong size = nrows * ncols;

    constexpr ULong len = 16384;

    ULong ix = 0;

    auto buffer = std::array<Float, len>();

    while ( true )
        {
            // -----------------------------------------------------------------
            // Recv into buffer.

            const ULong current_len = std::min( len, size - ix );

            if ( current_len == 0 )
                {
                    break;
                }

            Receiver::recv<Float>(
                current_len * sizeof( Float ), _socket, buffer.data() );

            // -----------------------------------------------------------------
            // Copy from buffer into features.

            for ( ULong ix2 = 0; ix2 < current_len; ++ix, ++ix2 )
                {
                    const ULong i = ix / ncols;
                    const ULong j = ix % ncols;

                    ( *features[j] )[i] = buffer[ix2];
                }

            // -----------------------------------------------------------------
        }

    // -------------------------------------------------------------------------

    return features;

    // -------------------------------------------------------------------------
}

// -----------------------------------------------------------------------------

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

// -----------------------------------------------------------------------------

std::vector<std::string> Receiver::recv_string_column(
    Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------

    std::uint64_t nbytes = 0;

    recv<std::uint64_t>( sizeof( std::uint64_t ), _socket, &nbytes );

    if ( nbytes == 0 )
        {
            return {};
        }

    // ------------------------------------------------

    std::string str( static_cast<size_t>( nbytes ), '0' );

    // ------------------------------------------------

    recv<char>( static_cast<ULong>( nbytes ), _socket, &( str[0] ) );

    // ------------------------------------------------

    return helpers::StringSplitter::split( str, GETML_SEP );

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

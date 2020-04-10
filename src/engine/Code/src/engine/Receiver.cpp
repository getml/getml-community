#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// -----------------------------------------------------------------------------

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

std::vector<std::string> Receiver::recv_encoding(
    Poco::Net::StreamSocket *_socket )
{
    // ------------------------------------------------
    // Recv shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Check plausibility

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your encoding must contain at least one row!" );
        }

    if ( std::get<1>( shape ) < 0 )
        {
            throw std::runtime_error(
                "Encoding data volume cannot be negative!" );
        }

    // ------------------------------------------------
    // Receive raw data

    auto raw_data = std::vector<char>( std::get<1>( shape ) );

    recv<char>(
        static_cast<ULong>( std::get<1>( shape ) ), _socket, raw_data.data() );

    // ------------------------------------------------
    // Interpret raw data and read into col

    std::vector<std::string> encoding( std::get<0>( shape ) );

    const bool little_endian = utils::Endianness::is_little_endian();

    auto ptr = raw_data.data();

    for ( size_t i = 0; i < encoding.size(); ++i )
        {
            assert_true( ptr < raw_data.data() + raw_data.size() );

            auto length = *reinterpret_cast<Int *>( ptr );

            if ( little_endian )
                {
                    utils::Endianness::reverse_byte_order( &length );
                }

            ptr += sizeof( Int );

            auto str = std::string( length, '0' );

            std::copy( ptr, ptr + length, str.data() );

            ptr += length;

            encoding[i] = std::move( str );
        }

    assert_true( ptr == raw_data.data() + raw_data.size() );

    // ------------------------------------------------

    return encoding;

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
    // Receive shape

    std::array<Int, 2> shape;

    recv<Int>( sizeof( Int ) * 2, _socket, shape.data() );

    // ------------------------------------------------
    // Check shape

    if ( std::get<0>( shape ) <= 0 )
        {
            throw std::runtime_error(
                "Your data must contain at least one row!" );
        }

    if ( std::get<1>( shape ) != 1 )
        {
            throw std::runtime_error( "Must be a column!" );
        }

    // ------------------------------------------------
    // Recv integers

    containers::Column<Int> integers(
        static_cast<size_t>( std::get<0>( shape ) ) );

    recv<Int>(
        sizeof( Int ) * static_cast<ULong>( integers.nrows() ),
        _socket,
        integers.data() );

    // ------------------------------------------------
    // Recv local_encoding

    const auto local_encoding = recv_encoding( _socket );

    // ------------------------------------------------
    // Decode integers

    std::vector<std::string> col( integers.nrows() );

    for ( size_t i = 0; i < col.size(); ++i )
        {
            const auto ix = integers[i];

            assert_true( ix >= 0 );
            assert_true( static_cast<size_t>( ix ) < local_encoding.size() );

            col[i] = local_encoding[ix];
        }

    // ------------------------------------------------

    return col;

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

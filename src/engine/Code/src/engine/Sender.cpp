#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

void Sender::send_categorical_matrix(
    const containers::Matrix<Int>& _matrix,
    const containers::Encoding& _encoding,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<Int, 2> shape;

    std::get<0>( shape ) = static_cast<Int>( _matrix.nrows() );
    std::get<1>( shape ) = static_cast<Int>( _matrix.ncols() );

    Sender::send<Int>( 2 * sizeof( Int ), shape.data(), _socket );

    // ------------------------------------------------
    // Map to string and send.

    for ( size_t i = 0; i < _matrix.nrows(); ++i )
        {
            for ( size_t j = 0; j < _matrix.ncols(); ++j )
                {
                    const auto str = _encoding[_matrix( i, j )];

                    send_string( str, _socket );
                }
        }

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

void Sender::send_features(
    const containers::Features& _features, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Get nrows and ncols.

    const ULong ncols = static_cast<ULong>( _features.size() );
    const ULong nrows =
        ( ncols > 0 ) ? ( static_cast<ULong>( _features[0]->size() ) ) : ( 0 );

    for ( auto& f : _features )
        {
            assert( f->size() == nrows );
        }

    // ------------------------------------------------
    // Send dimensions of features.

    std::array<Int, 2> shape;

    std::get<0>( shape ) = static_cast<Int>( nrows );
    std::get<1>( shape ) = static_cast<Int>( ncols );

    Sender::send<Int>( 2 * sizeof( Int ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    const ULong size = nrows * ncols;

    const ULong len = 16384;

    ULong ix = 0;

    auto buffer = std::vector<Float>( len );

    while ( true )
        {
            // ---------------------------------------------------------------
            // Copy to buffer

            const ULong current_len = std::min( len, size - ix );

            if ( current_len == 0 )
                {
                    break;
                }

            for ( ULong ix2 = 0; ix2 < current_len; ++ix, ++ix2 )
                {
                    const ULong i = ix / ncols;
                    const ULong j = ix % ncols;

                    buffer[ix2] = ( *_features[j] )[i];
                }

            // ---------------------------------------------------------------

            Sender::send<Float>(
                current_len * sizeof( Float ), buffer.data(), _socket );

            // ---------------------------------------------------------------
        }

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

void Sender::send_matrix(
    const containers::Matrix<Float>& _matrix, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<Int, 2> shape;

    std::get<0>( shape ) = static_cast<Int>( _matrix.nrows() );
    std::get<1>( shape ) = static_cast<Int>( _matrix.ncols() );

    Sender::send<Int>( 2 * sizeof( Int ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    Sender::send<Float>(
        _matrix.size() * sizeof( Float ), _matrix.data(), _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

void Sender::send_string(
    const std::string& _string, Poco::Net::StreamSocket* _socket )
{
    std::vector<char> buf;

    // ------------------------------------------------
    // Send size of string

    const Int str_size = static_cast<Int>( _string.size() );

    Sender::send<Int>( sizeof( Int ), &str_size, _socket );

    // ------------------------------------------------
    // Send string itself

    Sender::send<char>(
        static_cast<ULong>( _string.length() ), &( _string[0] ), _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

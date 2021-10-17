#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

void Sender::send_categorical_column(
    const std::vector<std::string>& _col, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------

    const auto get_str_len = []( const size_t _init,
                                 const std::string& _str ) -> size_t {
        return _init + _str.size();
    };

    const auto nbytes =
        _col.size() != 0
            ? static_cast<std::uint64_t>(
                  SEP_SIZE * ( _col.size() - 1 ) +
                  std::accumulate( _col.begin(), _col.end(), 0, get_str_len ) )
            : static_cast<std::uint64_t>( 0 );

    // ------------------------------------------------

    Sender::send<std::uint64_t>( sizeof( std::uint64_t ), &nbytes, _socket );

    // ------------------------------------------------

    auto data = std::vector<char>( nbytes );

    std::uint64_t i = 0;

    for ( size_t j = 0; j < _col.size(); ++j )
        {
            if ( j != 0 ) [[likely]]
                {
                    std::copy(
                        GETML_SEP, GETML_SEP + SEP_SIZE, data.data() + i );

                    i += SEP_SIZE;
                }

            const auto& str = _col[j];

            std::copy( str.begin(), str.end(), data.data() + i );

            i += static_cast<std::uint64_t>( str.size() );

            assert_msg(
                i <= nbytes,
                "i out of range! i: " + std::to_string( i ) +
                    ", nbytes: " + std::to_string( nbytes ) );
        }

    // ------------------------------------------------

    Sender::send<char>( static_cast<ULong>( nbytes ), data.data(), _socket );

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------

void Sender::send_column(
    const containers::Column<Float>& _col, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<Int, 2> shape;

    std::get<0>( shape ) = static_cast<Int>( _col.nrows() );
    std::get<1>( shape ) = static_cast<Int>( 1 );

    Sender::send<Int>( 2 * sizeof( Int ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    Sender::send<Float>( _col.nrows() * sizeof( Float ), _col.data(), _socket );

    // ------------------------------------------------
}

// -----------------------------------------------------------------------------

void Sender::send_features(
    const containers::Features& _features, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Get nrows and ncols.

    const ULong ncols = static_cast<ULong>( _features.size() );
    const ULong nrows =
        ( ncols > 0 ) ? ( static_cast<ULong>( _features[0]->size() ) ) : ( 0 );

#ifndef NDEBUG
    for ( auto& f : _features )
        {
            assert_true( f->size() == nrows );
        }
#endif

    // ------------------------------------------------
    // Send dimensions of features.

    std::array<Int, 2> shape;

    std::get<0>( shape ) = static_cast<Int>( nrows );
    std::get<1>( shape ) = static_cast<Int>( ncols );

    Sender::send<Int>( 2 * sizeof( Int ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    const ULong size = nrows * ncols;

    constexpr ULong len = 16384;

    ULong ix = 0;

    auto buffer = std::array<Float, len>();

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

void Sender::send_string(
    const std::string& _string, Poco::Net::StreamSocket* _socket )
{
    std::vector<char> buf;

    // ------------------------------------------------

    const Int str_size = static_cast<Int>( _string.size() );

    Sender::send<Int>( sizeof( Int ), &str_size, _socket );

    // ------------------------------------------------

    Sender::send<char>(
        static_cast<ULong>( _string.length() ), &( _string[0] ), _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

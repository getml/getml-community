#include "engine/communication/communication.hpp"

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

void Sender::send_categorical_matrix(
    const containers::Matrix<ENGINE_INT>& _matrix,
    const containers::Encoding& _encoding,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<ENGINE_INT, 2> shape;

    std::get<0>( shape ) = static_cast<ENGINE_INT>( _matrix.nrows() );
    std::get<1>( shape ) = static_cast<ENGINE_INT>( _matrix.ncols() );

    Sender::send<ENGINE_INT>( 2 * sizeof( ENGINE_INT ), shape.data(), _socket );

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

void Sender::send_matrix(
    const containers::Matrix<ENGINE_FLOAT>& _matrix,
    Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<ENGINE_INT, 2> shape;

    std::get<0>( shape ) = static_cast<ENGINE_INT>( _matrix.nrows() );
    std::get<1>( shape ) = static_cast<ENGINE_INT>( _matrix.ncols() );

    Sender::send<ENGINE_INT>( 2 * sizeof( ENGINE_INT ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    Sender::send<ENGINE_FLOAT>(
        _matrix.size() * sizeof( ENGINE_FLOAT ), _matrix.data(), _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------

void Sender::send_string(
    const std::string& _string, Poco::Net::StreamSocket* _socket )
{
    std::vector<char> buf;

    // ------------------------------------------------
    // Send size of string

    const ENGINE_INT str_size = static_cast<ENGINE_INT>( _string.size() );

    Sender::send<ENGINE_INT>( sizeof( ENGINE_INT ), &str_size, _socket );

    // ------------------------------------------------
    // Send string itself

    Sender::send<char>(
        static_cast<ENGINE_UNSIGNED_LONG>( _string.length() ),
        &( _string[0] ),
        _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

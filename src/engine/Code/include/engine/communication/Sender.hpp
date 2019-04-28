#ifndef ENGINE_COMMUNICATION_SENDER_HPP_
#define ENGINE_COMMUNICATION_SENDER_HPP_

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

struct Sender
{
    /// Sends data of any kind to the client
    template <class T>
    static void send(
        const ENGINE_UNSIGNED_LONG _size,
        const T* _data,
        Poco::Net::StreamSocket* _socket );

    /// Sends matrix to the client
    template <class T>
    static void send_matrix(
        const containers::Matrix<T>& _matrix,
        Poco::Net::StreamSocket* _socket );

    /// Sends a string to the client
    static void send_string(
        const std::string& _string, Poco::Net::StreamSocket* _socket );
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
void Sender::send(
    const ENGINE_UNSIGNED_LONG _size,
    const T* _data,
    Poco::Net::StreamSocket* _socket )
{
    const ENGINE_UNSIGNED_LONG len = 4096;

    const bool is_little_endian = utils::Endianness::is_little_endian();

    ENGINE_UNSIGNED_LONG j = 0;

    std::vector<char> buf( len );

    // -----------------------------------------------------------------------
    // Send len bytes at most into the buffer and then send to socket.

    while ( true )
        {
            // ---------------------------------------------------------------
            // Copy to buffer

            const ENGINE_UNSIGNED_LONG current_len = std::min( len, _size - j );

            if ( current_len == 0 )
                {
                    break;
                }

            if ( current_len != len )
                {
                    buf.resize( current_len );
                }

            for ( ENGINE_UNSIGNED_LONG i = 0; i < current_len; ++i, ++j )
                {
                    buf.data()[i] = reinterpret_cast<const char*>( _data )[j];
                }

            // ---------------------------------------------------------------
            // Handle endianness issues, which only apply for numeric types.
            // The only non-numeric type we ever come across is char.
            // By default, numeric data sent over the socket is big endian
            // (also referred to as network-byte-order)!

            // is_arithmetic includes numeric values and char.
            // http://en.cppreference.com/w/cpp/types/is_arithmetic
            static_assert(
                std::is_arithmetic<T>::value,
                "Only arithmetic types allowed for Sender::send<T>(...)!" );

            if ( !std::is_same<T, char>::value && is_little_endian )
                {
                    T* buf_begin = reinterpret_cast<T*>( buf.data() );

                    T* buf_end = buf_begin + buf.size() / sizeof( T );

                    std::for_each( buf_begin, buf_end, []( T& _val ) {
                        utils::Endianness::reverse_byte_order( &_val );
                    } );
                }

            // ---------------------------------------------------------------
            // Send via socket

            for ( size_t num_bytes_sent = 0; num_bytes_sent < buf.size(); )
                {
                    const auto nbytes = _socket->sendBytes(
                        buf.data() + num_bytes_sent,
                        static_cast<int>( buf.size() - num_bytes_sent ) );

                    if ( nbytes <= 0 )
                        {
                            throw std::runtime_error(
                                "Broken pipe while attempting to send data." );
                        }

                    num_bytes_sent += static_cast<size_t>( nbytes );
                }

            // -------------------------------------------------
        }

    assert( j == _size );
}

// ------------------------------------------------------------------------

template <class T>
void Sender::send_matrix(
    const containers::Matrix<T>& _matrix, Poco::Net::StreamSocket* _socket )
{
    // ------------------------------------------------
    // Send dimensions of matrix

    std::array<ENGINE_INT, 2> shape;

    std::get<0>( shape ) = static_cast<ENGINE_INT>( _matrix.nrows() );
    std::get<1>( shape ) = static_cast<ENGINE_INT>( _matrix.ncols() );

    Sender::send<ENGINE_INT>( 2 * sizeof( ENGINE_INT ), shape.data(), _socket );

    // ------------------------------------------------
    // Send actual data

    Sender::send<T>( _matrix.size() * sizeof( T ), _matrix.data(), _socket );

    // ------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_SENDER_HPP_

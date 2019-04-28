#ifndef ENGINE_COMMUNICATION_RECEIVER_HPP_
#define ENGINE_COMMUNICATION_RECEIVER_HPP_

namespace engine
{
namespace communication
{
// ------------------------------------------------------------------------

class Receiver
{
   public:
    /// Receives data of any type from the client
    template <class T>
    static void recv(
        const ENGINE_UNSIGNED_LONG _size,
        Poco::Net::StreamSocket *_socket,
        T *_data );

    /// Receives a categorical matrix from the client
    static containers::Matrix<ENGINE_INT> recv_categorical_matrix(
        containers::Encoding *_encoding, Poco::Net::StreamSocket *_socket );

    /// Receives a command from the client
    static Poco::JSON::Object recv_cmd(
        const std::shared_ptr<const logging::Logger> &_logger,
        Poco::Net::StreamSocket *_socket );

    /// Receives a matrix from the client
    static containers::Matrix<ENGINE_FLOAT> recv_matrix(
        Poco::Net::StreamSocket *_socket );

   private:
    /// Receives a string - useful for commands and categorical matrices
    static std::string recv_string( Poco::Net::StreamSocket *_socket );
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <class T>
void Receiver::recv(
    const ENGINE_UNSIGNED_LONG _size,
    Poco::Net::StreamSocket *_socket,
    T *_data )
{
    const ENGINE_UNSIGNED_LONG len = 4096;

    ENGINE_UNSIGNED_LONG j = 0;

    // -------------------------------------------------------------------
    // Receive len bytes at most, write them into the buffer and then
    // copy to the output data.

    // This assumes that T* has enough data allocated.

    // We also assume that the size of the data to be sent is known.

    std::vector<char> buf( len );

    while ( true )
        {
            const ENGINE_UNSIGNED_LONG current_len = std::min( len, _size - j );

            if ( current_len == 0 )
                {
                    break;
                }

            if ( current_len != len )
                {
                    buf.resize( current_len );
                }

            for ( size_t num_bytes_received = 0;
                  num_bytes_received < buf.size(); )
                {
                    const auto nbytes = _socket->receiveBytes(
                        buf.data() + num_bytes_received,
                        static_cast<int>( buf.size() - num_bytes_received ) );

                    if ( nbytes <= 0 )
                        {
                            throw std::runtime_error(
                                "Broken pipe while attempting to receive "
                                "data." );
                        }

                    num_bytes_received += static_cast<size_t>( nbytes );
                }

            for ( size_t i = 0; i < buf.size(); ++i, ++j )
                {
                    reinterpret_cast<char *>( _data )[j] = buf.data()[i];
                }
        }

    assert( j == _size );

    // -------------------------------------------------------------------
    // Handle endianness issues, which only apply for numeric types.
    // The only non-numeric type we ever come across is char.
    // By default, numeric data sent over the socket is big endian
    // (also referred to as network-byte-order)!

    // is_arithmetic includes numeric values and char.
    // http://en.cppreference.com/w/cpp/types/is_arithmetic
    static_assert(
        std::is_arithmetic<T>::value,
        "Only arithmetic types allowed for recv<T>(...)!" );

    if ( !std::is_same<T, char>::value &&
         utils::Endianness::is_little_endian() )
        {
            std::for_each(
                _data,
                _data + static_cast<size_t>( _size ) / sizeof( T ),
                []( T &_val ) {
                    utils::Endianness::reverse_byte_order( &_val );
                } );
        }

    // -------------------------------------------------------------------
}

// ------------------------------------------------------------------------
}  // namespace communication
}  // namespace engine

#endif  // ENGINE_COMMUNICATION_RECEIVER_HPP_

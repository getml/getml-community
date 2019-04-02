#ifndef ENGINE_UTILS_ENDIANNESS_HPP_
#define ENGINE_UTILS_ENDIANNESS_HPP_

namespace engine
{
namespace utils
{
// ------------------------------------------------------------------------

struct Endianness
{
    // --------------------------------------------------------------------

    /// Determines endianness of the system at runtime
    static bool is_little_endian()
    {
        unsigned int one = 1;

        char *c = reinterpret_cast<char *>( &one );

        return *c;
    }

    // --------------------------------------------------------------------

    /// Reverses the byteorder of the value passed to it
    template <typename T>
    static void reverse_byte_order( T &_val )
    {
        char *v = reinterpret_cast<char *>( &_val );

        std::reverse( v, v + sizeof( T ) );
    }

    // --------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

#endif  // ENGINE_UTILS_ENDIANNESS_HPP_

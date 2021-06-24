#ifndef STL_IOTA_HPP_
#define STL_IOTA_HPP_

namespace stl
{
// -------------------------------------------------------------------------

/// Necessary work-around, as iota is not supported on Windows yet.
template <class T, class T1, class T2>
inline auto iota( T1 _begin, T2 _end )
{
#if ( defined( _WIN32 ) || defined( _WIN64 ) )
    return IotaRange<T>( static_cast<T>( _begin ), static_cast<T>( _end ) );
#else
    return std::views::iota( static_cast<T>( _begin ), static_cast<T>( _end ) );
#endif
}

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_IOTA_HPP_

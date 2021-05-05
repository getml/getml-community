#ifndef STL_JOIN_HPP_
#define STL_JOIN_HPP_

namespace stl
{
// -------------------------------------------------------------------------

/// Necessary work-around, as join is not supported on Windows yet.
template<class T>
inline std::vector<T> join( const std::vector<std::vector<T>>& _input ) {
  std::vector<T> result;

  for(const auto& in: _input ) {
    result.insert( result.end(), in.begin(), in.end() );
  }

  return result;
}

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_JOIN_HPP_

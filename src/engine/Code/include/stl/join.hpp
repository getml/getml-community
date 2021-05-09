#ifndef STL_JOIN_HPP_
#define STL_JOIN_HPP_

namespace stl
{
// -------------------------------------------------------------------------

/// Necessary work-around, as join is not supported on Windows yet.
struct join
{
    template <class T, class RangeType>
    static std::vector<T> vector( RangeType _range )
    {
        std::vector<T> result;

        for ( const auto& r : _range )
            {
                result.insert( result.end(), r.begin(), r.end() );
            }

        return result;
    }

    template <class T>
    static std::vector<T> vector(
        const std::initializer_list<std::vector<T>>& _range )
    {
        std::vector<T> result;

        for ( const auto& r : _range )
            {
                result.insert( result.end(), r.begin(), r.end() );
            }

        return result;
    }
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_JOIN_HPP_

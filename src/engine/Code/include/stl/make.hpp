#ifndef STL_MAKE_HPP_
#define STL_MAKE_HPP_

namespace stl
{
// -------------------------------------------------------------------------

struct make
{
    /// Generates a vector from a range
    template <class T, class RangeType>
    static std::vector<T> vector( RangeType range )
    {
        auto vec = std::vector<T>();

        for ( const auto& val : range )
            {
                vec.push_back( val );
            }

        return vec;
    }

    /// Generates a set from a range
    template <class T, class RangeType>
    static std::set<T> set( RangeType range )
    {
        auto s = std::set<T>();

        for ( const T& val : range )
            {
                s.insert( val );
            }

        return s;
    }
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_MAKE_HPP_

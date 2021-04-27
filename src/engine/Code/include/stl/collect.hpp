#ifndef STL_COLLECT_HPP_
#define STL_COLLECT_HPP_

namespace stl
{
// -------------------------------------------------------------------------

struct collect
{
    /// Generates a string from a range
    template <class RangeType>
    static std::string string( RangeType range )
    {
        std::stringstream stream;

        for ( const auto& val : range )
            {
                stream << val;
            }

        return stream.str();
    }

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

#endif  // STL_COLLECT_HPP_

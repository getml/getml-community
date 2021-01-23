#ifndef HELPERS_STL_HPP_
#define HELPERS_STL_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

struct STL
{
    /// Generates a vector from a range
    template <class T, class RangeType>
    static std::vector<T> make_vector( RangeType range )
    {
        auto vec = std::vector<T>();

        for ( const auto& val : range )
            {
                vec.push_back( val );
            }

        return vec;
    }
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_STL_HPP_

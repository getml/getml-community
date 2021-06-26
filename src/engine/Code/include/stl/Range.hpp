#ifndef STL_RANGE_HPP_
#define STL_RANGE_HPP_

namespace stl
{
// -------------------------------------------------------------------------

template <class iterator>
class Range
{
   public:
    Range( iterator _begin, iterator _end ) : begin_( _begin ), end_( _end ) {}

    ~Range() = default;

    /// Trivial (const) accessor.
    iterator begin() const { return begin_; }

    /// Trivial (const) accessor.
    iterator end() const { return end_; }

   private:
    /// Iterator to the beginning of the rownums
    iterator begin_;

    /// Iterator to the end of the rownums
    iterator end_;
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_RANGE_HPP_

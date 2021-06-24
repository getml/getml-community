#ifndef STL_IOTARANGE_HPP_
#define STL_IOTARANGE_HPP_

namespace stl
{
// -------------------------------------------------------------------------

template <class T>
class IotaRange
{
   public:
    IotaRange( T _begin, T _end )
        : begin_( IotaIterator<T>( _begin ) ), end_( IotaIterator<T>( _end ) )
    {
    }

    ~IotaRange() = default;

    /// Returns iterator to beginning.
    IotaIterator<T> begin() const { return begin_; }

    /// Returns iterator to end.
    IotaIterator<T> end() const { return end_; }

   private:
    /// Iterator to beginning.
    IotaIterator<T> begin_;

    /// Iterator to end.
    IotaIterator<T> end_;
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_IOTARANGE_HPP_

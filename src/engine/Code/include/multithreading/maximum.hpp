#ifndef MULTITHREADING_MAXIMUM_HPP_
#define MULTITHREADING_MAXIMUM_HPP_

namespace multithreading
{
// ----------------------------------------------------------------------------

template <class T>
struct maximum
{
   public:
    maximum() {}

    ~maximum() = default;

    // -----------------------------------------

    T operator()( const T& _elem1, const T& _elem2 )
    {
        return ( ( _elem1 > _elem2 ) ? ( _elem1 ) : ( _elem2 ) );
    }
};

// ----------------------------------------------------------------------------
}  // namespace multithreading

#endif  // MULTITHREADING_MAXIMUM_HPP_

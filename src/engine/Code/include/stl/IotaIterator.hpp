#ifndef STL_IOTAITERATOR_HPP_
#define STL_IOTAITERATOR_HPP_

namespace stl
{
// -------------------------------------------------------------------------

template <class T>
struct IotaIterator
{
    using iterator_category = std::bidirectional_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;

    /// Default constructor.
    IotaIterator() : ptr_( std::make_unique<value_type>( 0 ) ){};

    /// Construct by value.
    IotaIterator( const value_type& _value )
        : ptr_( std::make_unique<value_type>( _value ) )
    {
    }

    /// Copy constructor.
    IotaIterator( const IotaIterator<T>& _other )
        : ptr_( std::make_unique<value_type>( *_other ) )
    {
    }

    /// Move constructor.
    IotaIterator( IotaIterator<T>&& _other ) noexcept
        : ptr_( std::make_unique<value_type>( *_other ) )
    {
    }

    /// Destructor.
    ~IotaIterator() = default;

    /// Copy assignment operator.
    IotaIterator<T>& operator=( const IotaIterator<T>& _other )
    {
        ptr_ = std::make_unique<value_type>( *_other );
        return *this;
    }

    /// Move assignment operator.
    IotaIterator<T>& operator=( IotaIterator<T>&& _other ) noexcept
    {
        ptr_ = std::make_unique<value_type>( *_other );
        return *this;
    }

    /// Tricky: operator()* must be const, but provide a non-const reference.
    reference operator*() const { return *ptr_; }

    /// Returns a pointer to value_.
    pointer operator->() { return ptr_.get(); }

    /// Prefix incrementor
    IotaIterator<T>& operator++()
    {
        ++( *ptr_ );
        return *this;
    }

    /// Postfix incrementor.
    IotaIterator<T> operator++( int )
    {
        IotaIterator<T> tmp = *this;
        ++( *this );
        return tmp;
    }

    /// Prefix decrementor
    IotaIterator<T>& operator--()
    {
        --( *ptr_ );
        return *this;
    }

    /// Postfix decrementor.
    IotaIterator<T> operator--( int )
    {
        IotaIterator<T> tmp = *this;
        --( *this );
        return tmp;
    }

    /// Check equality.
    friend bool operator==(
        const IotaIterator<T>& _a, const IotaIterator<T>& _b )
    {
        return *_a.ptr_ == *_b.ptr_;
    };

    /// Check inequality.
    friend bool operator!=(
        const IotaIterator<T>& _a, const IotaIterator<T>& _b )
    {
        return *_a.ptr_ != *_b.ptr_;
    };

   private:
    /// Pointer to the value held.
    std::unique_ptr<value_type> ptr_;
};

// -------------------------------------------------------------------------
}  // namespace stl

#endif  // STL_IOTAITERATOR_HPP_

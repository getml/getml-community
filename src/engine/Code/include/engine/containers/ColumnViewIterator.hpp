#ifndef ENGINE_CONTAINERS_COLUMNVIEWITERATOR_HPP_
#define ENGINE_CONTAINERS_COLUMNVIEWITERATOR_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class ColumnViewIterator
{
   public:
    using iterator_category = std::bidirectional_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;

    typedef std::function<std::optional<T>( size_t )> ValueFunc;

    /// Iterator to the beginning.
    ColumnViewIterator( const ValueFunc _value_func )
        : i_( 0 ),
          value_( _value_func( 0 ) ),
          value_func_( _value_func ),
          value_ptr_( &value_ ){};

    /// Iterator to the end.
    ColumnViewIterator()
        : i_( 0 ),
          value_( std::nullopt ),
          value_func_( std::nullopt ),
          value_ptr_( &value_ ){};

    /// Copy constructor.
    ColumnViewIterator( const ColumnViewIterator<T>& _other )
        : i_( _other.i_ ),
          value_( _other.value_ ),
          value_func_( _other.value_func_ ),
          value_ptr_( &value_ ){};

    /// Move constructor.
    ColumnViewIterator( ColumnViewIterator<T>&& _other ) noexcept
        : i_( _other.i_ ),
          value_( std::move( _other.value_ ) ),
          value_func_( std::move( _other.value_func_ ) ),
          value_ptr_( &value_ ){};

    /// Destructor.
    ~ColumnViewIterator() = default;

    /// Copy assignment operator.
    ColumnViewIterator<T>& operator=( const ColumnViewIterator<T>& _other )
    {
        i_ = _other.i_;
        value_ = _other.value_;
        value_func_ = _other.value_func_;
        return *this;
    }

    /// Move assignment operator.
    ColumnViewIterator<T>& operator=( ColumnViewIterator<T>&& _other ) noexcept
    {
        i_ = _other.i_;
        value_ = std::move( _other.value_ );
        value_func_ = std::move( _other.value_func_ );
        return *this;
    }

    /// Tricky: operator()* must be const, but provide a non-const reference.
    inline reference operator*() const { return **value_ptr_; }

    /// Returns a pointer to value_.
    inline pointer operator->() { return &( **value_ptr_ ); }

    /// Prefix incrementor
    inline ColumnViewIterator<T>& operator++()
    {
        ++i_;
        if ( value_func_ )
            {
                value_ = ( *value_func_ )( i_ );
            }
        return *this;
    }

    /// Postfix incrementor.
    inline ColumnViewIterator<T> operator++( int )
    {
        ColumnViewIterator<T> tmp = *this;
        ++( *this );
        return tmp;
    }

    /// Prefix decrementor
    inline ColumnViewIterator<T>& operator--()
    {
        --i_;
        if ( value_func_ )
            {
                value_ = ( *value_func_ )( i_ );
            }
        return *this;
    }

    /// Postfix decrementor.
    inline ColumnViewIterator<T> operator--( int )
    {
        ColumnViewIterator<T> tmp = *this;
        --( *this );
        return tmp;
    }

    /// Check equality.
    friend inline bool operator==(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return ( !_a.value_ && !_b.value_ ) ||
               ( _a.value_ && _b.value_ && _a.i_ == _b.i_ );
    };

    /// Check inequality.
    friend inline bool operator!=(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return !( _a == _b );
    };

   private:
    /// The current index.
    size_t i_;

    /// The current value.
    std::optional<T> value_;

    /// The function returning the actual data point.
    std::optional<ValueFunc> value_func_;

    /// A pointer to the value_;
    std::optional<T>* const value_ptr_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_COLUMNVIEWITERATOR_HPP_


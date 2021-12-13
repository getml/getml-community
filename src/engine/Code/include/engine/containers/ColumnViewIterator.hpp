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
    using iterator_category = std::random_access_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = T;
    using pointer = value_type*;
    using reference = value_type&;

    typedef std::function<std::optional<T>( size_t )> ValueFunc;

    /// Iterator to the beginning.
    ColumnViewIterator( const ValueFunc _value_func )
        : i_( 0 ),
          subiterator_( std::make_shared<ColumnViewIterator<T>>() ),
          value_( _value_func( 0 ) ),
          value_func_( _value_func ),
          value_ptr_( &value_ ){};

    /// Iterator to the end.
    ColumnViewIterator()
        : i_( 0 ),
          subiterator_( nullptr ),
          value_( std::nullopt ),
          value_func_( std::nullopt ),
          value_ptr_( &value_ ){};

    /// Copy constructor.
    ColumnViewIterator( const ColumnViewIterator<T>& _other )
        : i_( _other.i_ ),
          subiterator_( std::make_shared<ColumnViewIterator<T>>() ),
          value_( _other.value_ ),
          value_func_( _other.value_func_ ),
          value_ptr_( &value_ ){};

    /// Move constructor.
    ColumnViewIterator( ColumnViewIterator<T>&& _other ) noexcept
        : i_( _other.i_ ),
          subiterator_( std::move( _other.subiterator_ ) ),
          value_( std::move( _other.value_ ) ),
          value_func_( std::move( _other.value_func_ ) ),
          value_ptr_( &value_ ){};

    /// Destructor.
    ~ColumnViewIterator() = default;

    /// Copy assignment operator.
    ColumnViewIterator<T>& operator=( const ColumnViewIterator<T>& _other )
    {
        i_ = _other.i_;
        subiterator_ = _other.subiterator_;
        value_ = _other.value_;
        value_func_ = _other.value_func_;
        return *this;
    }

    /// Move assignment operator.
    ColumnViewIterator<T>& operator=( ColumnViewIterator<T>&& _other ) noexcept
    {
        i_ = _other.i_;
        subiterator_ = std::move( _other.subiterator_ );
        value_ = std::move( _other.value_ );
        value_func_ = std::move( _other.value_func_ );
        return *this;
    }

    /// Tricky: operator()* must be const, but provide a non-const reference.
    inline reference operator*() const
    {
        assert_true( value_ );
        assert_true( *value_ptr_ );
        return **value_ptr_;
    }

    /// Returns a pointer to value_.
    inline pointer operator->()
    {
        assert_true( value_ );
        assert_true( *value_ptr_ );
        return &( **value_ptr_ );
    }

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

    /// Incrementor
    inline ColumnViewIterator<T>& operator+=( difference_type _j )
    {
        const auto i = static_cast<difference_type>( i_ ) + _j;
        assert_true( i >= 0 );
        i_ = static_cast<size_t>( i );
        if ( value_func_ )
            {
                value_ = ( *value_func_ )( i_ );
            }
        return *this;
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

    /// Decrementor
    inline ColumnViewIterator<T>& operator-=( size_t _j )
    {
        const auto i = static_cast<difference_type>( i_ ) - _j;
        assert_true( i >= 0 );
        i_ = static_cast<size_t>( i );
        if ( value_func_ )
            {
                value_ = ( *value_func_ )( i_ );
            }
        return *this;
    }

    /// operator[], needed for ranges support.
    inline reference operator[]( difference_type _i ) const
    {
        assert_true( subiterator_ );
        *subiterator_ = *this + _i;
        return **subiterator_;
    }

    /// Check equality
    friend inline bool operator==(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return ( !_a.value_ && !_b.value_ ) ||
               ( _a.value_ && _b.value_ && _a.i_ == _b.i_ );
    };

    /// Smaller than operator.
    friend inline bool operator<(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return ( _a.value_ && !_b.value_ ) || !( !_a.value_ && _b.value_ ) ||
               ( _a.value_ && _b.value_ && _a.i_ < _b.i_ );
    };

    /// Inequality operator.
    friend inline bool operator!=(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return !( _a == _b );
    }

    /// Greater than opeator
    friend inline bool operator>(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return _b < _a;
    }

    /// Smaller-equal operator.
    friend inline bool operator<=(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return !( _b < _a );
    }

    friend inline bool operator>=(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        return !( _a < _b );
    }

    /// Add value
    friend inline ColumnViewIterator<T> operator+(
        const ColumnViewIterator<T>& _a, const difference_type _j )
    {
        ColumnViewIterator<T> tmp = _a;
        tmp += _j;
        return tmp;
    }

    /// Add value
    friend inline ColumnViewIterator<T> operator+(
        const difference_type _j, const ColumnViewIterator<T>& _a )
    {
        return _a + _j;
    }

    /// Substract value
    friend inline ColumnViewIterator<T> operator-(
        const ColumnViewIterator<T>& _a, const difference_type _j )
    {
        ColumnViewIterator<T> tmp = _a;
        tmp -= _j;
        return tmp;
    }

    /// Get difference between two iterators
    friend inline difference_type operator-(
        const ColumnViewIterator<T>& _a, const ColumnViewIterator<T>& _b )
    {
        const auto i_a = static_cast<difference_type>( _a.i_ );
        const auto i_b = static_cast<difference_type>( _b.i_ );
        return i_a - i_b;
    }

   private:
    /// The current index.
    size_t i_;

    /// The subiterator is needed for the operator[].
    std::shared_ptr<ColumnViewIterator<T>> subiterator_;

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


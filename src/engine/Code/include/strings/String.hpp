#ifndef STRINGS_STRING_HPP_
#define STRINGS_STRING_HPP_

namespace strings
{
// ----------------------------------------------------------------------------

// String is an implementation of a string class that has
// no memory overhead over a standard C-string.
class String
{
    // -------------------------------------------------------

   public:
    String() : chars_( std::make_unique<char[]>( 1 ) )
    {
        chars_.get()[0] = '\0';
    }

    String( const std::string& _str )
        : chars_( std::make_unique<char[]>( _str.size() + 1 ) )
    {
        std::copy( _str.begin(), _str.end(), chars_.get() );
        chars_.get()[_str.size()] = '\0';
    }

    String( const String& _other )
        : chars_( std::make_unique<char[]>( _other.size() + 1 ) )
    {
        strcpy( chars_.get(), _other.c_str() );
    }

    String( String&& _other ) : chars_( std::move( _other.chars_ ) ) {}

    ~String() = default;

    // -------------------------------------------------------

   public:
    /// Returns a pointer to the underlying C-String.
    const char* c_str() const
    {
        assert_true( chars_ );
        return chars_.get();
    }

    /// Copy assignment operator.
    String& operator=( const String& _other )
    {
        String temp( _other );
        *this = std::move( temp );
        return *this;
    }

    /// Move assignment operator
    String& operator=( String&& _other ) noexcept
    {
        if ( this == &_other ) return *this;
        chars_ = std::move( _other.chars_ );
        return *this;
    }

    /// Equal to operator
    bool operator==( const String& _other ) const
    {
        return ( strcmp( c_str(), _other.c_str() ) == 0 );
    }

    /// Less than operator
    bool operator<( const String& _other ) const
    {
        return ( strcmp( c_str(), _other.c_str() ) < 0 );
    }

    /// Returns the size of the underlying string.
    size_t size() const
    {
        assert_true( chars_ );
        return static_cast<size_t>( strlen( chars_.get() ) );
    }

    /// Returns a std::string created from the underlying data.
    std::string str() const
    {
        assert_true( chars_ );
        return std::string( chars_.get() );
    }

    // -------------------------------------------------------

   private:
    /// The underlying data.
    std::unique_ptr<char[]> chars_;
};

// ----------------------------------------------------------------------------
}  // namespace strings

#endif  // STRINGS_STRING_HPP_

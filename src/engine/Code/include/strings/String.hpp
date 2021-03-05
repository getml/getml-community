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

    String( const char* _str )
        : chars_( std::make_unique<char[]>( strlen( _str ) + 1 ) )
    {
        strcpy( chars_.get(), _str );
    }

    String( const String& _other )
        : chars_( std::make_unique<char[]>( _other.size() + 1 ) )
    {
        strcpy( chars_.get(), _other.c_str() );
    }

    String( String&& _other ) noexcept : chars_( std::move( _other.chars_ ) ) {}

    ~String() = default;

   public:
    /// Checks whether the string contains another string.
    bool contains( const strings::String& _other ) const
    {
        return ( strstr( c_str(), _other.c_str() ) != NULL );
    }

    /// Returns a pointer to the underlying C-String.
    const char* c_str() const
    {
        assert_true( chars_ );
        return chars_.get();
    }

    /// Calculates the hash function of this string.
    /// This is useful for std::unordered_map.
    size_t hash() const
    {
        return std::hash<std::string_view>()(
            std::string_view( c_str(), size() ) );
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

    /// Equal to operator
    bool operator==( const char* _other ) const
    {
        return ( strcmp( c_str(), _other ) == 0 );
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

    /// Returns a lower case version of this string.
    String to_lower() const
    {
        const auto tolower = []( const char c ) { return std::tolower( c ); };
        auto lower = String( *this );
        std::transform(
            lower.c_str(),
            lower.c_str() + lower.size(),
            lower.chars_.get(),
            tolower );
        return lower;
    }

    /// Returns a upper case version of this string.
    String to_upper() const
    {
        const auto toupper = []( const char c ) { return std::toupper( c ); };
        auto upper = String( *this );
        std::transform(
            upper.c_str(),
            upper.c_str() + upper.size(),
            upper.chars_.get(),
            toupper );
        return upper;
    }

    // -------------------------------------------------------

   private:
    /// The underlying data.
    std::unique_ptr<char[]> chars_;
};

// ----------------------------------------------------------------------------
}  // namespace strings

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

inline std::ostream& operator<<(
    std::ostream& _os, const strings::String& _str )
{
    _os << _str.c_str();
    return _os;
}

// ----------------------------------------------------------------------------
#endif  // STRINGS_STRING_HPP_

#ifndef ENGINE_CONTAINERS_ENCODING_HPP_
#define ENGINE_CONTAINERS_ENCODING_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class Encoding
{
   public:
    Encoding(
        const std::shared_ptr<const Encoding> _subencoding =
            std::shared_ptr<const Encoding>() )
        : null_value_( "" ),
          subencoding_( _subencoding ),
          subsize_( _subencoding ? _subencoding->size() : 0 ),
          vector_( std::make_shared<std::vector<strings::String>>( 0 ) )
    {
    }

    ~Encoding() = default;

    // -------------------------------

    /// Appends all elements of a different encoding.
    void append( const Encoding& _other, bool _include_subencoding = false );

    /// Returns the string mapped to an integer.
    template <typename T>
    strings::String operator[]( const T _i ) const;

    /// Returns the integer mapped to a string.
    Int operator[]( const strings::String& _val );

    /// Returns the integer mapped to a string (const version).
    Int operator[]( const strings::String& _val ) const;

    /// Copies a vector
    Encoding& operator=( const std::vector<std::string>& _vector );

    // -------------------------------

    /// Returns beginning of unique integers
    std::vector<strings::String>::const_iterator begin() const
    {
        return vector_->cbegin();
    }

    /// Deletes all entries
    void clear()
    {
        map_ = std::map<strings::String, Int>();
        *vector_ = std::vector<strings::String>();
    }

    /// Returns end of unique integers
    std::vector<strings::String>::const_iterator end() const
    {
        return vector_->cend();
    }

    /// Returns the integer mapped to a string.
    Int operator[]( const std::string& _val )
    {
        return ( *this )[strings::String( _val )];
    }

    /// Returns the integer mapped to a string (const version).
    Int operator[]( const std::string& _val ) const
    {
        return ( *this )[strings::String( _val )];
    }

    /// Number of encoded elements
    size_t size() const { return subsize_ + vector_->size(); }

    /// Get the vector containing the names.
    inline const std::shared_ptr<const std::vector<strings::String>> vector()
        const
    {
        return vector_;
    }

    // -------------------------------

   private:
    /// Adds an integer to map_ and vector_, assuming it is not already included
    Int insert( const strings::String& _val );

    // -------------------------------

   private:
    /// For fast lookup
    std::map<strings::String, Int> map_;

    /// The null value (needed because strings are returned by reference).
    const std::string null_value_;

    /// A subencoding can be used to separate the existing encoding from new
    /// data. Under some circumstance, we want to avoid the global encoding
    /// being edited, such as when we process requests in parallel.
    std::shared_ptr<const Encoding> subencoding_;

    // The size of the subencoding at the time this encoding was created.
    const size_t subsize_;

    /// Maps integers to strings
    const std::shared_ptr<std::vector<strings::String>> vector_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <typename T>
strings::String Encoding::operator[]( const T _i ) const
{
    static_assert( std::is_integral<T>::value, "Integral required." );

    assert_true( size() > 0 );

    assert_true( _i < 0 || static_cast<size_t>( _i ) < size() );

    if ( _i < 0 || static_cast<size_t>( _i ) >= size() )
        {
            return null_value_;
        }

    if ( subencoding_ )
        {
            if ( _i < subsize_ )
                {
                    return ( *subencoding_ )[_i];
                }
            else
                {
                    return ( *vector_ )[_i - subsize_];
                }
        }
    else
        {
            return ( *vector_ )[_i];
        }
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ENCODING_HPP_

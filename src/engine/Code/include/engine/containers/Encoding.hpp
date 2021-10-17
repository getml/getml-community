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
        : null_value_( "NULL" ),
          subencoding_( _subencoding ),
          subsize_( _subencoding ? _subencoding->size() : 0 ),
          vector_( std::make_shared<std::vector<strings::String>>( 0 ) )
    {
    }

    ~Encoding() = default;

    // -------------------------------

    /// Appends all elements of a different encoding.
    void append( const Encoding& _other, bool _include_subencoding = false );

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
        map_ =
            std::unordered_map<strings::String, Int, strings::StringHasher>();
        *vector_ = std::vector<strings::String>();
    }

    /// Returns end of unique integers
    std::vector<strings::String>::const_iterator end() const
    {
        return vector_->cend();
    }

    /// Returns the integer mapped to a string.
    template <class T>
    std::conditional<
        std::is_same<T, std::string>::value ||
            std::is_same<T, strings::String>::value,
        Int,
        const strings::String&>::type
    operator[]( const T& _val )
    {
        if constexpr ( std::is_same<T, std::string>() )
            {
                return string_to_int( strings::String( _val ) );
            }

        if constexpr ( std::is_same<T, strings::String>() )
            {
                return string_to_int( _val );
            }

        if constexpr (
            !std::is_same<T, std::string>() &&
            !std::is_same<T, strings::String>() )
            {
                return int_to_string( _val );
            }
    }

    /// Returns the integer mapped to a string (const version).
    template <class T>
    std::conditional<
        std::is_same<T, std::string>::value ||
            std::is_same<T, strings::String>::value,
        Int,
        const strings::String&>::type
    operator[]( const T& _val ) const
    {
        if constexpr ( std::is_same<T, std::string>() )
            {
                return string_to_int( strings::String( _val ) );
            }

        if constexpr ( std::is_same<T, strings::String>() )
            {
                return string_to_int( _val );
            }

        if constexpr (
            !std::is_same<T, std::string>() &&
            !std::is_same<T, strings::String>() )
            {
                return int_to_string( _val );
            }
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

    /// Returns the string mapped to an integer.
    const strings::String& int_to_string( const Int _i ) const;

    /// Returns the integer mapped to a string.
    Int string_to_int( const strings::String& _val );

    /// Returns the integer mapped to a string (const version).
    Int string_to_int( const strings::String& _val ) const;

    // -------------------------------

   private:
    /// For fast lookup
    std::unordered_map<strings::String, Int, strings::StringHasher> map_;

    /// The null value (needed because strings are returned by reference).
    const strings::String null_value_;

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
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ENCODING_HPP_

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
          subsize_( _subencoding ? _subencoding->size() : 0 )
    {
    }

    ~Encoding() = default;

    // -------------------------------

    /// Appends all elements of a different encoding.
    void append( const Encoding& _other, bool _include_subencoding = false );

    /// Returns the string mapped to an integer.
    template <typename T>
    const std::string& operator[]( const T _i ) const;

    /// Returns the integer mapped to a string.
    ENGINE_INT operator[]( const std::string& _val );

    /// Returns the integer mapped to a string (const version).
    ENGINE_INT operator[]( const std::string& _val ) const;

    /// Copies a vector
    Encoding& operator=( std::vector<std::string>&& _vector ) noexcept;

    // -------------------------------

    /// Returns beginning of unique integers
    std::vector<std::string>::const_iterator begin() const
    {
        return vector_.cbegin();
    }

    /// Deletes all entries
    void clear()
    {
        map_.clear();
        vector_.clear();
    }

    /// Returns end of unique integers
    std::vector<std::string>::const_iterator end() const
    {
        return vector_.cend();
    }

    /// Number of encoded elements
    size_t size() const { return subsize_ + vector_.size(); }

    /// Get the vector containing the names.
    inline const std::vector<std::string>& vector() const { return vector_; }

    // -------------------------------

   private:
    /// Adds an integer to map_ and vector_, assuming it is not already included
    ENGINE_INT insert( const std::string& _val );

    // -------------------------------

   private:
    /// For fast lookup
    std::unordered_map<std::string, ENGINE_INT> map_;

    /// The null value (needed because strings are returned by reference).
    const std::string null_value_;

    /// A subencoding can be used to separate the existing encoding from new
    /// data. Under some circumstance, we want to avoid the global encoding
    /// being edited, such as when we process requests in parallel.
    std::shared_ptr<const Encoding> subencoding_;

    // The size of the subencoding at the time this encoding was created.
    const size_t subsize_;

    /// Maps integers to strings
    std::vector<std::string> vector_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <typename T>
const std::string& Encoding::operator[]( const T _i ) const
{
    assert( size() > 0 );

    assert( _i < 0 || static_cast<size_t>( _i ) < size() );

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
                    return vector_[_i - subsize_];
                }
        }
    else
        {
            return vector_[_i];
        }
}

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ENCODING_HPP_

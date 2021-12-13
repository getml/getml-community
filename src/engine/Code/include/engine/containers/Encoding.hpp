#ifndef ENGINE_CONTAINERS_ENCODING_HPP_
#define ENGINE_CONTAINERS_ENCODING_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class Encoding
{
    using InMemoryType = std::shared_ptr<InMemoryEncoding>;
    using MemoryMappedType = std::shared_ptr<MemoryMappedEncoding>;

    using ConstInMemoryType = std::shared_ptr<const InMemoryEncoding>;
    using ConstMemoryMappedType = std::shared_ptr<const MemoryMappedEncoding>;

   public:
    Encoding(
        const std::shared_ptr<memmap::Pool>& _pool,
        const std::shared_ptr<const Encoding> _subencoding =
            std::shared_ptr<const Encoding>() )
    {
        if ( _pool )
            {
                init<MemoryMappedEncoding>( _pool, _subencoding );
            }
        else
            {
                init<InMemoryEncoding>( _pool, _subencoding );
            }
    }

    ~Encoding() = default;

   public:
    /// Appends all elements of a different encoding.
    void append( const Encoding& _other, bool _include_subencoding = false )
    {
        bool success = append<InMemoryType>( _other, _include_subencoding );
        success =
            success || append<MemoryMappedType>( _other, _include_subencoding );
        assert_true( success );
    }

    /// Deletes all entries
    void clear()
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const auto pimpl = std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                pimpl->clear();
                return;
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const auto pimpl = std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        pimpl->clear();
    }

    /// Copies a vector
    Encoding& operator=( const std::vector<std::string>& _vector )
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const auto pimpl = std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                *pimpl = _vector;
                return *this;
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const auto pimpl = std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        *pimpl = _vector;
        return *this;
    }

    /// Returns the integer mapped to a string or the string mapped to an
    /// integer, updates the mapping, if necessary.
    template <class T>
    auto operator[]( const T& _val )
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const auto pimpl = std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                return ( *pimpl )[_val];
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const auto pimpl = std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        return ( *pimpl )[_val];
    }

    /// Returns the integer mapped to a string or the string mapped to an
    /// integer (const version).
    template <class T>
    auto operator[]( const T& _val ) const
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const ConstInMemoryType pimpl =
                    std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                return ( *pimpl )[_val];
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const ConstMemoryMappedType pimpl =
            std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        return ( *pimpl )[_val];
    }

    /// Number of encoded elements
    size_t size() const
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const ConstInMemoryType pimpl =
                    std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                return pimpl->size();
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const ConstMemoryMappedType pimpl =
            std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        return pimpl->size();
    }

    /// The temporary directory (only relevant for the MemoryMappedEncoding)
    std::optional<std::string> temp_dir() const
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                return std::nullopt;
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const ConstMemoryMappedType pimpl =
            std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        return pimpl->temp_dir();
    }

    /// Get the vector containing the strings.
    inline helpers::StringIterator strings() const
    {
        if ( std::holds_alternative<InMemoryType>( pimpl_ ) )
            {
                const ConstInMemoryType pimpl =
                    std::get<InMemoryType>( pimpl_ );
                assert_true( pimpl );
                const auto func =
                    [pimpl]( const size_t _i ) -> strings::String {
                    return ( *pimpl )[_i];
                };
                return helpers::StringIterator( func, pimpl->size() );
            }

        assert_true( std::holds_alternative<MemoryMappedType>( pimpl_ ) );
        const ConstMemoryMappedType pimpl =
            std::get<MemoryMappedType>( pimpl_ );
        assert_true( pimpl );
        const auto func = [pimpl]( const size_t _i ) -> strings::String {
            return ( *pimpl )[_i];
        };
        return helpers::StringIterator( func, pimpl->size() );
    }

   private:
    /// Appends to the encoding.
    template <class PtrType>
    bool append( const Encoding& _other, bool _include_subencoding = false )
    {
        if ( std::holds_alternative<PtrType>( pimpl_ ) )
            {
                assert_true( std::holds_alternative<PtrType>( _other.pimpl_ ) );
                const auto pimpl = std::get<PtrType>( pimpl_ );
                const auto other = std::get<PtrType>( _other.pimpl_ );
                assert_true( other );
                pimpl->append( *other, _include_subencoding );
                return true;
            }
        return false;
    }

    /// Initializes the encoding.
    template <class EncodingType>
    void init(
        const std::shared_ptr<memmap::Pool>& _pool,
        const std::shared_ptr<const Encoding> _subencoding )
    {
        using PtrType = std::shared_ptr<EncodingType>;

        assert_true(
            !_subencoding ||
            std::holds_alternative<PtrType>( _subencoding->pimpl_ ) );

        const auto subencoding = _subencoding
                                     ? std::get<PtrType>( _subencoding->pimpl_ )
                                     : PtrType();

        if constexpr ( std::is_same<PtrType, InMemoryType>() )
            {
                pimpl_ = std::make_shared<EncodingType>( subencoding );
            }

        if constexpr ( std::is_same<PtrType, MemoryMappedType>() )
            {
                pimpl_ = std::make_shared<EncodingType>( _pool, subencoding );
            }
    }

   private:
    /// Abstracts over an InMemoryEncoding and MemoryMappedEncoding.
    std::variant<InMemoryType, MemoryMappedType> pimpl_;
};

// -------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_ENCODING_HPP_

#ifndef ENGINE_CONTAINERS_INDEX_HPP_
#define ENGINE_CONTAINERS_INDEX_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T, class Hash = std::hash<T>>
class Index
{
   public:
    typedef std::unordered_map<T, std::vector<size_t>, Hash> MapType;

   public:
    Index() : begin_( 0 ), map_( std::make_shared<MapType>() ) {}

    ~Index() = default;

    // -------------------------------

    /// Recalculates the index.
    void calculate( const Column<T>& _key );

    // -------------------------------

    /// Returns a const copy to the underlying map.
    std::shared_ptr<MapType> map() const { return map_; }

    // -------------------------------

   private:
    // Determines whether this is a NULL value
    bool is_null( const T& _val ) const;

    // -------------------------------

   private:
    /// Stores the first row number for which we do not have an index.
    size_t begin_;

    /// Performs the role of an "index" over the keys
    std::shared_ptr<MapType> map_;

    // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T, class Hash>
void Index<T, Hash>::calculate( const Column<T>& _key )
{
    if ( _key.size() < begin_ )
        {
            map_->clear();
            begin_ = 0;
        }

    for ( size_t i = begin_; i < _key.nrows(); ++i )
        {
            if ( !is_null( _key[i] ) )
                {
                    auto it = map_->find( _key[i] );

                    if ( it == map_->end() )
                        {
                            ( *map_ )[_key[i]] = { i };
                        }
                    else
                        {
                            it->second.push_back( i );
                        }
                }
        }

    begin_ = _key.nrows();
}

// -------------------------------------------------------------------------

template <class T, class Hash>
bool Index<T, Hash>::is_null( const T& _val ) const
{
    if constexpr ( std::is_same<T, Int>() )
        {
            return _val < 0;
        }

    if constexpr ( std::is_same<T, Float>() )
        {
            return ( std::isnan( _val ) || std::isinf( _val ) );
        }

    if constexpr ( std::is_same<T, strings::String>() )
        {
            return utils::NullChecker::is_null( _val );
        }

    return false;
}

// -------------------------------------------------------------------------

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_INDEX_HPP_

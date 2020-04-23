#ifndef ENGINE_CONTAINERS_INDEX_HPP_
#define ENGINE_CONTAINERS_INDEX_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

template <class T>
class Index
{
   public:
    typedef std::unordered_map<T, std::vector<size_t>> MapType;

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
    /// Stores the first row number for which we do not have an index.
    size_t begin_;

    /// Performs the role of an "index" over the keys
    std::shared_ptr<MapType> map_;

    // -------------------------------
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
void Index<T>::calculate( const Column<T>& _key )
{
    if ( _key.size() < begin_ )
        {
            map_->clear();
            begin_ = 0;
        }

    for ( size_t i = begin_; i < _key.nrows(); ++i )
        {
            if ( _key[i] >= 0 )
                {
                    auto it = map_->find( _key[i] );

                    if ( it == map_->end() )
                        {
                            ( *map_ )[_key[i]] = {i};
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

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_INDEX_HPP_

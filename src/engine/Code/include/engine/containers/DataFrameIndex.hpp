#ifndef ENGINE_CONTAINERS_DATAFRAMEINDEX_HPP_
#define ENGINE_CONTAINERS_DATAFRAMEINDEX_HPP_

namespace engine
{
namespace containers
{
// -------------------------------------------------------------------------

class DataFrameIndex
{
   public:
    DataFrameIndex() : begin_( 0 ), map_( std::make_shared<Index>() ) {}

    ~DataFrameIndex() = default;

    // -------------------------------

    /// Recalculates the index.
    void calculate( const Matrix<ENGINE_INT>& _join_key );

    // -------------------------------

    /// Returns a const copy to the underlying map.
    std::shared_ptr<Index> map() const { return map_; }

    // -------------------------------

   private:
    /// Stores the first row number for which we do not have an index.
    size_t begin_;

    /// Performs the role of an "index" over the join keys/
    std::shared_ptr<Index> map_;

    // -------------------------------
};

// -------------------------------------------------------------------------

}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINERS_DATAFRAMEINDEX_HPP_

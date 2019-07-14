#ifndef ENGINE_HANDLERS_DATAFRAMEJOINER_HPP_
#define ENGINE_HANDLERS_DATAFRAMEJOINER_HPP_

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

class DataFrameJoiner
{
    // ------------------------------------------------------------------------

   public:
    /// Returns a new table, that results from joining the other two.
    static containers::DataFrame join(
        const std::string& _name,
        const containers::DataFrame& _df1,
        const containers::DataFrame& _df2,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const std::string& _how,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding );

    // ------------------------------------------------------------------------

   private:
    /// Returns the join key and index signified by _name.
    static std::pair<
        const containers::Column<Int>,
        const containers::DataFrameIndex>
    find_join_key( const containers::DataFrame& _df, const std::string& _name );

    /// Creates the row indices, which indicate which rows in _df1 and _df2 are
    /// taken.
    static std::pair<std::vector<size_t>, std::vector<size_t>> make_row_indices(
        const containers::DataFrame& _df1,
        const containers::DataFrame& _df2,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const std::string& _how );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATAFRAMEJOINER_HPP_

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
        const Poco::JSON::Array& _cols1,
        const Poco::JSON::Array& _cols2,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const std::string& _how,
        const std::optional<const Poco::JSON::Object>& _where,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding );

    // ------------------------------------------------------------------------

   private:
    /// Adds all columns from _df to _joined_df, sorted by _rindices.
    static void add_all(
        const containers::DataFrame& _df,
        const std::vector<size_t>& _rindices,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        containers::DataFrame* _joined_df );

    /// Adds column _col from _df to _joined_df, sorted by _rindices.
    static void add_col(
        const containers::DataFrame& _df,
        const std::vector<size_t>& _rindices,
        const std::string& _name,
        const std::string& _role,
        const std::string& _as,
        containers::DataFrame* _joined_df );

    /// Adds columns in _cols from _df to _joined_df, sorted by _rindices.
    static void add_cols(
        const containers::DataFrame& _df,
        const std::vector<size_t>& _rindices,
        const Poco::JSON::Array& _cols,
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        containers::DataFrame* _joined_df );

    static void build_temp_dfs(
        const containers::DataFrame& _df1,
        const containers::DataFrame& _df2,
        const std::vector<size_t>& _rindices1,
        const std::vector<size_t>& _rindices2,
        const Poco::JSON::Object& _col,
        containers::DataFrame* _temp_df1,
        containers::DataFrame* _temp_df2 );

    static void filter(
        const containers::DataFrame& _df1,
        const containers::DataFrame& _df2,
        const std::vector<size_t>& _rindices1,
        const std::vector<size_t>& _rindices2,
        const Poco::JSON::Object& _where,
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
        containers::DataFrame* _temp_df );

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
        const std::string& _how,
        size_t* _begin );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATAFRAMEJOINER_HPP_

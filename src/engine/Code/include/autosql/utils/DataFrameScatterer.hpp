#ifndef AUTOSQL_UTILS_DATAFRAMESCATTERER_HPP_
#define AUTOSQL_UTILS_DATAFRAMESCATTERER_HPP_

namespace autosql
{
namespace utils
{
// ----------------------------------------------------------------------------

class DataFrameScatterer
{
    // ------------------------------------------------------------------------

   public:
    /// Returns a vector of the same length as the keys that signifies
    /// the thread to which each row belongs.
    static const std::pair<std::vector<size_t>, size_t> build_thread_nums(
        const std::vector<containers::Column<Int> >& _keys,
        const size_t _num_threads );

    /// Returns a subview on the data frame.
    static containers::DataFrameView scatter_data_frame(
        const containers::DataFrame& _df,
        const std::vector<size_t>& _thread_nums,
        const size_t _thread_num );

    // ------------------------------------------------------------------------

   private:
    /// Returns a vector of the same length as the keys that signifies
    /// the thread to which each row belongs.
    static const std::pair<std::vector<size_t>, size_t> build_thread_nums(
        const std::map<Int, size_t>& _min_keys_map,
        const containers::Column<Int>& min_join_key );

    /// Checks the plausibility of the input.
    static void check_plausibility(
        const std::vector<containers::Column<Int> >& _keys,
        const size_t _num_threads );

    /// Identifies the key with the minimal number of keys and maps
    /// a thread id to each of them
    static void scatter_keys(
        const std::vector<containers::Column<Int> >& _keys,
        const size_t _num_threads,
        size_t* _ix_min_keys_map,
        std::map<Int, size_t>* _min_keys_map );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace autosql

#endif  // AUTOSQL_UTILS_DATAFRAMESCATTERER_HPP_

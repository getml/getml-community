#ifndef ENGINE_PIPELINES_MANYTOONEJOINER_HPP_
#define ENGINE_PIPELINES_MANYTOONEJOINER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class ManyToOneJoiner
{
   public:
    /// Parses the joined names to execute the many-to-one joins required in the
    /// data model.
    static void join_tables(
        const bool _use_timestamps,
        const std::vector<std::string>& _origin_peripheral_names,
        const std::string& _joined_population_name,
        const std::vector<std::string>& _joined_peripheral_names,
        containers::DataFrame* _population_df,
        std::vector<containers::DataFrame>* _peripheral_dfs );

    static std::vector<std::string> split_joined_name(
        const std::string& _joined_name );

    static std::tuple<
        std::string,
        std::string,
        std::string,
        std::string,
        std::string,
        std::string>
    parse_splitted( const std::string& _splitted );

   private:
    static containers::DataFrame find_peripheral(
        const std::string& _name,
        const std::vector<std::string>& _peripheral_names,
        const std::vector<containers::DataFrame>& _peripheral_dfs );

    static std::string get_param(
        const std::string& _splitted, const std::string& _key );

    static containers::DataFrame join_all(
        const bool _use_timestamps,
        const bool _is_population,
        const std::string& _joined_name,
        const std::vector<std::string>& _origin_peripheral_names,
        const containers::DataFrame& _population_df,
        const std::vector<containers::DataFrame>& _peripheral_dfs );

    static containers::DataFrame join_one(
        const bool _use_timestamps,
        const std::string& _splitted,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral_dfs,
        const std::vector<std::string>& _peripheral_names );

    static std::vector<size_t> make_index(
        const bool _use_timestamps,
        const std::string& _join_key,
        const std::string& _other_join_key,
        const std::string& _time_stamp,
        const std::string& _other_time_stamp,
        const std::string& _upper_time_stamp,
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral );

    static std::pair<size_t, bool> retrieve_index(
        const size_t _nrows,
        const Int _jk,
        const Float _ts,
        const containers::DataFrameIndex::MapType& _peripheral_index,
        const std::optional<containers::Column<Float>>& _other_time_stamp,
        const std::optional<containers::Column<Float>>& _upper_time_stamp );

   private:
    static std::optional<containers::Column<Float>> extract_time_stamp(
        const containers::DataFrame& _df, const std::string& _name )
    {
        if ( _name == "" )
            {
                return std::nullopt;
            }

        return _df.time_stamp( _name );
    }
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_MANYTOONEJOINER_HPP_


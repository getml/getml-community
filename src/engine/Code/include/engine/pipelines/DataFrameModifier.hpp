#ifndef ENGINE_PIPELINES_DATAFRAMEMODIFIER_HPP_
#define ENGINE_PIPELINES_DATAFRAMEMODIFIER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class DataFrameModifier
{
   public:
    /// Adds a constant join keys. This is needed for when the user has not
    /// explicitly passed a join key.
    static void add_join_keys(
        const Poco::JSON::Object& _population_placeholder,
        const std::vector<std::string>& _peripheral_names,
        containers::DataFrame* _population_df,
        std::vector<containers::DataFrame>* _peripheral_dfs );

    /// Extracts upper time stamps from the memory parameter. (Memory is just
    /// syntactic sugar for upper time stamps. The feature learners don't know
    /// about this concept).
    static std::vector<containers::DataFrame> add_time_stamps(
        const Poco::JSON::Object& _population_placeholder,
        const std::vector<std::string>& _peripheral_names,
        const std::vector<containers::DataFrame>& _peripheral_dfs );

   private:
    /// Adds a constant join key. This is needed for when the user has not
    /// explicitly passed a join key.
    static void add_jk( containers::DataFrame* _df );

    /// Adds lower and upper time stamps to the data frame.
    static void add_ts(
        const Poco::JSON::Object& _joined_table,
        const std::string& _ts_used,
        const std::string& _upper_ts_used,
        const Float _horizon,
        const Float _memory,
        const std::vector<std::string>& _peripheral_names,
        std::vector<containers::DataFrame>* _peripheral_dfs );

    /// Extracts a vector named _name of size _expected_size from the
    /// _population_placeholder
    template <typename T>
    static std::vector<T> extract_vector(
        const Poco::JSON::Object& _population_placeholder,
        const std::string& _name,
        const size_t _expected_size );

    /// Returns a pointer to the peripheral data frame referenced by
    /// _joined_table.
    static containers::DataFrame* find_data_frame(
        const Poco::JSON::Object& _joined_table,
        const std::vector<std::string>& _peripheral_names,
        std::vector<containers::DataFrame>* _peripheral_dfs );

   private:
    /// Generates the name for the upper time stamp that is produced using
    /// memory.
    static std::string make_ts_name(
        const std::string& _ts_used, const Float _diff )
    {
        return PlaceholderMaker::make_ts_name( _ts_used, _diff );
    }
};

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

template <typename T>
std::vector<T> DataFrameModifier::extract_vector(
    const Poco::JSON::Object& _population_placeholder,
    const std::string& _name,
    const size_t _expected_size )
{
    // ------------------------------------------------------------------------

    if ( !_population_placeholder.getArray( _name ) )
        {
            throw std::invalid_argument(
                "The placeholder has no array named '" + _name + "'!" );
        }

    const auto vec =
        JSON::array_to_vector<T>( _population_placeholder.getArray( _name ) );

    // ------------------------------------------------------------------------

    if ( vec.size() != _expected_size )
        {
            throw std::invalid_argument(
                "Size of '" + _name + "' unexpected. Expected " +
                std::to_string( _expected_size ) + ", got " +
                std::to_string( vec.size() ) + "." );
        }

    // ------------------------------------------------------------------------

    return vec;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_DATAFRAMEMODIFIER_HPP_

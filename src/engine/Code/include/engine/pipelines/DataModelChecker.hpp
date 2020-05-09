#ifndef ENGINE_PIPELINES_DATAMODELCHECKER_HPP_
#define ENGINE_PIPELINES_DATAMODELCHECKER_HPP_

// ----------------------------------------------------------------------------

namespace engine
{
namespace pipelines
{
// ----------------------------------------------------------------------------

class DataModelChecker
{
   public:
    /// Generates warnings, if there are obvious issues in the data model.
    static void check(
        const std::shared_ptr<Poco::JSON::Object> _population_placeholder,
        const std::shared_ptr<std::vector<std::string>> _peripheral_names,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        const std::shared_ptr<const monitoring::Logger>& _logger,
        Poco::Net::StreamSocket* _socket );

   private:
    /// Checks the plausibility of a categorical column.
    static void check_categorical_column(
        const containers::Column<Int>& _col,
        const std::string& _df_name,
        communication::Warner* _warner );

    /// Checks all of the columns in the data frame make sense.
    static void check_df(
        const containers::DataFrame& _df, communication::Warner* _warner );

    /// Checks the plausibility of a float column.
    static void check_float_column(
        const containers::Column<Float>& _col,
        const std::string& _df_name,
        communication::Warner* _warner );

    /// Recursively checks the plausibility of the joins.
    static void check_join(
        const Poco::JSON::Object& _population_placeholder,
        const std::vector<std::string>& _peripheral_names,
        const containers::DataFrame& _population,
        const std::vector<containers::DataFrame>& _peripheral,
        communication::Warner* _warner );

    /// Raises a warning if there is something wrong with the matches.
    static void check_matches(
        const std::string& _join_key_used,
        const std::string& _other_join_key_used,
        const containers::DataFrame& _population,
        const containers::DataFrame& _peripheral,
        communication::Warner* _warner );

    /// Checks whether all non-NULL elements in _col are equal to each other
    static bool is_all_equal( const containers::Column<Float>& _col );
};

//  ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_DATAMODELCHECKER_HPP_

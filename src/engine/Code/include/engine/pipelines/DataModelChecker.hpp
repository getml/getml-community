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

   public:
    /// Checks all of the columns in the data frame make sense.
    static void check_df(
        const containers::DataFrame& _df, communication::Warner* _warner );

    /// Checks the plausibility of a float column.
    static void check_float_column(
        const containers::Column<Float>& _col,
        const std::string& _df_name,
        communication::Warner* _warner );
};

//  ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

// ----------------------------------------------------------------------------
#endif  // ENGINE_PIPELINES_DATAMODELCHECKER_HPP_

#ifndef DFS_CONTAINERS_SQLMAKER_HPP_
#define DFS_CONTAINERS_SQLMAKER_HPP_

// ----------------------------------------------------------------------------

namespace dfs
{
namespace containers
{
// ------------------------------------------------------------------------

class SQLMaker
{
   public:
    /// Creates a condition.
    static std::string condition(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const Condition& _condition,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
    /// AGGREGATED)).
    static std::string select_statement(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

   private:
    /// Returns the column name signified by _column_used and _data_used.
    static std::string get_name(
        const std::string& _feature_prefix,
        const enums::DataUsed _data_used,
        const size_t _peripheral,
        const size_t _input_col,
        const size_t _output_col,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Returns the column names signified by _column_used and _data_used, for
    /// same_units_...
    static std::pair<std::string, std::string> get_same_units(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Returns the select statement for AVG_TIME_BETWEEN.
    static std::string select_avg_time_between( const Placeholder& _input );

    /// Creates the value to be aggregated (for instance a column name or the
    /// difference between two columns)
    static std::string value_to_be_aggregated(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );
};

// ------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs

// ----------------------------------------------------------------------------

#endif  // DFS_CONTAINERS_SQLMAKER_HPP_

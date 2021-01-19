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
    /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
    /// AGGREGATED)).
    static std::string select_statement(
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

   private:
    /// Returns the column name signified by _column_used and _data_used.
    static std::string get_name(
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Returns the column names signified by _column_used and _data_used, for
    /// same_units_...
    static std::string get_same_units(
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Returns the column name signified by _column_used and _data_used as a
    /// time stamp
    static std::string get_ts_name(
        const size_t _column_used,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

    /// Extracts the proper name from a same units struct.
    /*std::pair<std::string, std::string> get_ts_names(
        const Placeholder& _input,
        const Placeholder& _output,
        const std::shared_ptr<const descriptors::SameUnitsContainer>
            _same_units,
        const size_t _column_used ) const;*/

    /// Transforms the time stamps diff into SQLite-compliant code.
    /*std::string make_time_stamp_diff(
        const Placeholder& _input,
        const Placeholder& _output,
        const std::shared_ptr<const descriptors::SameUnitsContainer>
            _same_units,
        const size_t _column_used,
        const Float _diff,
        const bool _is_greater ) const;

    /// Transforms the time stamps diff into SQLite-compliant given the
    /// colnames.
    std::string make_time_stamp_diff(
        const std::string& _colname1,
        const std::string& _colname2,
        const bool _is_greater ) const;

    /// Makes a window
    std::string make_time_stamp_window(
        const Placeholder& _input,
        const Placeholder& _output,
        const Float _diff,
        const bool _is_greater ) const;*/

    /// Creates the value to be aggregated (for instance a column name or the
    /// difference between two columns)
    static std::string value_to_be_aggregated(
        const std::string& _feature_prefix,
        const AbstractFeature& _abstract_feature,
        const Placeholder& _input,
        const Placeholder& _output );

   private:
    /// Returns the timediff string for time comparisons
    static std::string make_diffstr(
        const Float _timediff, const std::string _timeunit )
    {
        return ( _timediff >= 0.0 )
                   ? "'+" + std::to_string( _timediff ) + " " + _timeunit + "'"
                   : "'" + std::to_string( _timediff ) + " " + _timeunit + "'";
    }
};

// ------------------------------------------------------------------------
}  // namespace containers
}  // namespace dfs

// ----------------------------------------------------------------------------

#endif  // DFS_CONTAINERS_SQLMAKER_HPP_

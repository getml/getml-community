#ifndef RELBOOST_UTILS_CONDITIONMAKER_HPP_
#define RELBOOST_UTILS_CONDITIONMAKER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ------------------------------------------------------------------------

class ConditionMaker
{
   public:
    ConditionMaker( const Float _lag, const size_t _peripheral_used )
        : lag_( _lag ), peripheral_used_( _peripheral_used )
    {
    }

    ~ConditionMaker() = default;

    /// Identifies matches between population table and peripheral tables.
    std::string condition_greater(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

    std::string condition_smaller(
        const std::vector<strings::String>& _categories,
        const std::string& _feature_prefix,
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

   private:
    /// Returns a list of the categories.
    std::string list_categories(
        const std::vector<strings::String>& _categories,
        const containers::Split& _split ) const;

    /// Transforms the time stamps diff into SQLite-compliant code.
    std::string make_time_stamp_diff(
        const std::string& _ts1,
        const std::string& _ts2,
        const Float _diff,
        const bool _is_greater ) const;

    /// Transforms the time stamps diff into SQLite-compliant code,
    /// when the colnames are already known.
    std::string make_time_stamp_diff(
        const std::string& _colname1,
        const std::string& _colname2,
        const bool _is_greater ) const;

    /// Transforms the time stamps windows into SQLite-compliant code.
    std::string make_time_stamp_window(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const Float _diff,
        const bool _is_greater ) const;

   private:
    /// Generates the column name to insert into the conditions.
    std::string make_colname(
        const std::string& _colname, const std::string& _alias ) const
    {
        return _alias + ".\"" +
               helpers::SQLGenerator::make_colname( _colname ) + "\"";
    }

    /// Returns the timediff string for time comparisons
    std::string make_diffstr(
        const Float _timediff, const std::string _timeunit ) const
    {
        return ( _timediff >= 0.0 )
                   ? "'+" + std::to_string( _timediff ) + " " + _timeunit + "'"
                   : "'" + std::to_string( _timediff ) + " " + _timeunit + "'";
    }

   private:
    /// The lag variable used for the moving time window.
    const Float lag_;

    /// The number of the peripheral table used.
    const size_t peripheral_used_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_CONDITIONMAKER_HPP_

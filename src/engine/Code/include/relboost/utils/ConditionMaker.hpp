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
    ConditionMaker(
        const std::shared_ptr<const std::vector<strings::String>>& _encoding,
        const Float _lag,
        const size_t _peripheral_used )
        : encoding_( _encoding ),
          lag_( _lag ),
          peripheral_used_( _peripheral_used )
    {
        assert_true( encoding_ );
    }

    ~ConditionMaker() = default;

    /// Identifies matches between population table and peripheral tables.
    std::string condition_greater(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

    std::string condition_smaller(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

   private:
    /// Returns a list of the categories.
    std::string list_categories( const containers::Split& _split ) const;

    /// Transforms the time stamps diff into SQLite-compliant code.
    std::string make_time_stamp_diff(
        const std::string& _ts1,
        const std::string& _ts2,
        const Float _diff,
        const bool _is_greater ) const;

   private:
    /// Trivial accessor.
    strings::String encoding( size_t _i ) const
    {
        assert_true( encoding_ );

        if ( _i < encoding_->size() )
            {
                return ( *encoding_ )[_i];
            }
        else
            {
                assert_true( false && "Encoding out of range!" );
                return strings::String( "" );
            }
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
    /// Encoding for the categorical data, maps integers to underlying category.
    const std::shared_ptr<const std::vector<strings::String>> encoding_;

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

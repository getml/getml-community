#ifndef MULTIREL_UTILS_SQLMAKER_HPP_
#define MULTIREL_UTILS_SQLMAKER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ------------------------------------------------------------------------

class SQLMaker
{
   public:
    SQLMaker(
        const std::shared_ptr<const std::vector<strings::String>>& _encoding,
        const Float _lag,
        const size_t _peripheral_used,
        const descriptors::SameUnits& _same_units )
        : encoding_( _encoding ),
          lag_( _lag ),
          peripheral_used_( _peripheral_used ),
          same_units_( _same_units )
    {
        assert_true( encoding_ );
    }

    ~SQLMaker() = default;

    /// Creates a condition that must be greater than a critical value.
    std::string condition_greater(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const descriptors::Split& _split ) const;

    /// Creates a condition that must be smaller than a critical value.
    std::string condition_smaller(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const descriptors::Split& _split ) const;

    /// Creates a select statement (SELECT AGGREGATION(VALUE TO TO BE
    /// AGGREGATED)).
    std::string select_statement(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const size_t _column_used,
        const enums::DataUsed& _data_used,
        const std::string& _agg_type ) const;

   private:
    /// Returns the column name signified by _column_used and _data_used.
    std::string get_name(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const size_t _column_used,
        const enums::DataUsed& _data_used ) const;

    /// Extracts the proper name from a same units struct.
    std::pair<std::string, std::string> get_names(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const std::shared_ptr<const descriptors::SameUnitsContainer>
            _same_units,
        const size_t _column_used ) const;

    /// Returns a list of the categories.
    std::string list_categories( const descriptors::Split& _split ) const;

    /// Transforms the time stamps diff into SQLite-compliant code.
    std::string make_time_stamp_diff(
        const std::string& _ts1,
        const std::string& _ts2,
        const Float _diff,
        const bool _is_greater ) const;

    /// Creates the value to be aggregated (for instance a column name or the
    /// difference between two columns)
    std::string value_to_be_aggregated(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const size_t _column_used,
        const enums::DataUsed& _data_used ) const;

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

    /// Contains information on the same units
    const descriptors::SameUnits same_units_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

// ----------------------------------------------------------------------------

#endif  // MULTIREL_UTILS_SQLMAKER_HPP_

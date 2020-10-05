#ifndef RELBOOSTXX_UTILS_CONDITIONMAKER_HPP_
#define RELBOOSTXX_UTILS_CONDITIONMAKER_HPP_

// ----------------------------------------------------------------------------

namespace relcit
{
namespace utils
{
// ------------------------------------------------------------------------

class ConditionMaker
{
   public:
    ConditionMaker(
        const Float _lag,
        const size_t _peripheral_used,
        const std::shared_ptr<const StandardScaler>& _input_scaler,
        const std::shared_ptr<const StandardScaler>& _output_scaler )
        : input_scaler_( _input_scaler ),
          lag_( _lag ),
          output_scaler_( _output_scaler ),
          peripheral_used_( _peripheral_used )
    {
    }

    ~ConditionMaker() = default;

    /// Generates a condition for when the column is greater than the critical
    /// value.
    std::string condition_greater(
        const std::vector<strings::String>& _categories,
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

    /// Generates a condition for when the column is smaller than the critical
    /// value.
    std::string condition_smaller(
        const std::vector<strings::String>& _categories,
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const containers::Split& _split ) const;

    /// Generates the equation at the end of the condition.
    std::string make_equation(
        const containers::Placeholder& _input,
        const containers::Placeholder& _output,
        const std::vector<Float>& _weights ) const;

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

    /// The weights were trained on the rescaled columns. This rescales the
    /// weights such that they are appropriate for the original columns.
    std::vector<Float> rescale( const std::vector<Float>& _weights ) const;

   private:
    /// Trivial (const) accessor
    const utils::StandardScaler& input_scaler() const
    {
        assert_true( input_scaler_ );
        return *input_scaler_;
    }

    /// Returns the timediff string for time comparisons
    std::string make_diffstr(
        const Float _timediff, const std::string _timeunit ) const
    {
        return ( _timediff >= 0.0 )
                   ? "'+" + std::to_string( _timediff ) + " " + _timeunit + "'"
                   : "'" + std::to_string( _timediff ) + " " + _timeunit + "'";
    }

    /// Trivial (const) accessor
    const utils::StandardScaler& output_scaler() const
    {
        assert_true( output_scaler_ );
        return *output_scaler_;
    }

   private:
    /// The scaler used for the output table.
    const std::shared_ptr<const StandardScaler> input_scaler_;

    /// The lag variable used for the moving time window.
    const Float lag_;

    /// The scaler used for the output table.
    const std::shared_ptr<const StandardScaler> output_scaler_;

    /// The number of the peripheral table used.
    const size_t peripheral_used_;
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relcit

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_UTILS_CONDITIONMAKER_HPP_

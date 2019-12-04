#ifndef RELBOOST_UTILS_CRITICALVALUES_HPP_
#define RELBOOST_UTILS_CRITICALVALUES_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ------------------------------------------------------------------------

class CriticalValues
{
    // --------------------------------------------------------------------

   public:
    /// Calculates the critical values for categorical columns.
    static const std::shared_ptr<const std::vector<Int>> calc_categorical(
        const enums::DataUsed _data_used,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );

    /// Calculates the critical values for discrete columns.
    static std::vector<Float> calc_discrete(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );

    /// Calculates the critical values for numerical columns.
    static std::vector<Float> calc_numerical(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );

    /// Calculates the critical values for subfeatures.
    static std::vector<Float> calc_subfeatures(
        const size_t _col,
        const containers::Subfeatures& _subfeatures,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );

    /// Calculate the critical values necessary for the moving time window.
    static std::vector<Float> calc_time_window(
        const Float _delta_t,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm );

    // --------------------------------------------------------------------

   public:
    /// Wrapper around calc_discrete.
    static std::vector<Float> calc_discrete(
        const enums::DataUsed _data_used,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm )
    {
        assert_true( !is_same_units( _data_used ) );
        assert_true( _data_used != enums::DataUsed::same_units_categorical );

        return calc_discrete(
            _data_used,
            _num_column,
            _num_column,
            _input,
            _output,
            _begin,
            _end,
            _comm );
    }

    /// Wrapper around calc_numerical.
    static std::vector<Float> calc_numerical(
        const enums::DataUsed _data_used,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        multithreading::Communicator* _comm )
    {
        assert_true( !is_same_units( _data_used ) );
        assert_true( _data_used != enums::DataUsed::same_units_categorical );

        return calc_numerical(
            _data_used,
            _num_column,
            _num_column,
            _input,
            _output,
            _begin,
            _end,
            _comm );
    }

    // --------------------------------------------------------------------

   private:
    /// Does the actual job of calculating the numerical values.
    static std::vector<Float> calc_numerical(
        const size_t _num_critical_values, const Float _min, const Float _max );

    /// Finds the minimum and the maximum needed for calculating the
    /// critical values (categorical).
    static void find_min_max(
        const enums::DataUsed _data_used,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        Int* _min,
        Int* _max,
        multithreading::Communicator* _comm );

    /// Finds the minimum and the maximum needed for calculating the
    /// critical values (discrete and numerical).
    static void find_min_max(
        const enums::DataUsed _data_used,
        const size_t _num_column,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        Float* _min,
        Float* _max,
        multithreading::Communicator* _comm );

    /// Finds the minimum and the maximum needed for calculating the
    /// critical values (same units discrete and numerical).
    static void find_min_max(
        const enums::DataUsed _data_used,
        const size_t _input_col,
        const size_t _output_col,
        const containers::DataFrame& _input,
        const containers::DataFrameView& _output,
        const std::vector<const containers::Match*>::iterator _begin,
        const std::vector<const containers::Match*>::iterator _end,
        Float* _min,
        Float* _max,
        multithreading::Communicator* _comm );

    // --------------------------------------------------------------------

   private:
    /// Calculates the number of critical values needed for numerical columns
    static size_t calc_num_critical_values( Int _num_matches )
    {
        return std::max(
            static_cast<size_t>(
                std::sqrt( static_cast<Float>( _num_matches ) ) ),
            static_cast<size_t>( 1 ) );
    }

    /// Checks whether the _data_used is same units
    static bool is_same_units( const enums::DataUsed _data_used )
    {
        return (
            _data_used == enums::DataUsed::same_units_categorical ||
            _data_used == enums::DataUsed::same_units_discrete ||
            _data_used == enums::DataUsed::same_units_numerical );
    }

    // --------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_CRITICALVALUES_HPP_

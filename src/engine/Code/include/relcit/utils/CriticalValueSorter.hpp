#ifndef RELBOOSTXX_UTILS_CRITICALVALUESORTER_HPP_
#define RELBOOSTXX_UTILS_CRITICALVALUESORTER_HPP_

// ----------------------------------------------------------------------------

namespace relcit
{
namespace utils
{
// ------------------------------------------------------------------------

class CriticalValueSorter
{
   public:
    /// Sort critical values in DESCENDING order of the associated averages.
    static std::shared_ptr<const std::vector<Int>> sort(
        const Int _min,
        const std::vector<size_t>& _indptr,
        const containers::Rescaled& _output_rescaled,
        const containers::Rescaled& _input_rescaled,
        const std::vector<containers::CandidateSplit>::iterator
            _candidates_begin,
        const std::vector<containers::CandidateSplit>::iterator _candidates_end,
        std::vector<containers::Match>* _bins,
        multithreading::Communicator* _comm );

   private:
    /// Calculates the sums and count of the current candidate split.
    static std::pair<Float, Float> calc_average(
        const containers::CandidateSplit& _split,
        const std::vector<Float>& _total_sums,
        const Float _total_count,
        const containers::Rescaled& _output_rescaled,
        const containers::Rescaled& _input_rescaled,
        const std::vector<containers::Match>::iterator _split_begin,
        const std::vector<containers::Match>::iterator _split_end );

    /// Calculates the sums of all aggregated columns
    static std::pair<std::vector<Float>, Float> calc_sums(
        const containers::Rescaled& _output_rescaled,
        const containers::Rescaled& _input_rescaled,
        const std::vector<containers::Match>::iterator _begin,
        const std::vector<containers::Match>::iterator _end );

    /// Every candidate split has a average value it is associated with for the
    /// sorting. This calculates said averages.
    static std::vector<Float> make_averages(
        const Int _min,
        const std::vector<size_t>& _indptr,
        const containers::Rescaled& _output_rescaled,
        const containers::Rescaled& _input_rescaled,
        const std::vector<containers::CandidateSplit>::iterator
            _candidates_begin,
        const std::vector<containers::CandidateSplit>::iterator _candidates_end,
        std::vector<containers::Match>* _bins,
        multithreading::Communicator* _comm );

    /// Generates the tuples for sorting.
    static std::vector<std::tuple<Float, Int>> make_tuples(
        const std::vector<Float>& _averages,
        const std::vector<containers::CandidateSplit>::iterator _begin,
        const std::vector<containers::CandidateSplit>::iterator _end );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relcit

// ----------------------------------------------------------------------------

#endif  // RELBOOSTXX_UTILS_CRITICALVALUESORTER_HPP_

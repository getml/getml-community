#ifndef RELBOOST_UTILS_CRITICALVALUESORTER_HPP_
#define RELBOOST_UTILS_CRITICALVALUESORTER_HPP_

// ----------------------------------------------------------------------------

namespace relboost
{
namespace utils
{
// ------------------------------------------------------------------------

struct CriticalValueSorter
{
    /// Sort critical values in DESCENDING order of associated weights.
    static std::shared_ptr<const std::vector<Int>> sort(
        const std::vector<containers::CandidateSplit>::iterator _begin,
        const std::vector<containers::CandidateSplit>::iterator _end );

    // ------------------------------------------------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_CRITICALVALUESORTER_HPP_
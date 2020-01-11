#ifndef MULTIREL_UTILS_DISCRETEBINNER_HPP_
#define MULTIREL_UTILS_DISCRETEBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

class DiscreteBinner
{
   public:
    /// Bins the matches into _num_bins equal-width bins.
    /// The bins will be written into _bins_begin and the
    /// method returns and indptr to them.
    /// This assumes that min and max are known.
    static std::pair<std::vector<size_t>, Float> bin(
        const Float _min,
        const Float _max,
        const size_t _num_bins_numerical,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin,
        const std::vector<containers::Match*>::const_iterator _end,
        std::vector<containers::Match*>* _bins );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_DISCRETEBINNER_HPP_

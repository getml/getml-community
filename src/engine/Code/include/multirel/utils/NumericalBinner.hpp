#ifndef MULTIREL_UTILS_NUMERICALBINNER_HPP_
#define MULTIREL_UTILS_NUMERICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

class NumericalBinner
{
   public:
    /// Bins the matches into _num_bins equal-width bins.
    /// The bins will be written into _bins_begin and the
    /// method returns and indptr to them as well as the
    /// calculated step size.
    static std::pair<std::vector<size_t>, Float> bin(
        const Float _min,
        const Float _max,
        const size_t _num_bins,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin,
        const std::vector<containers::Match*>::const_iterator _end,
        std::vector<containers::Match*>* _bins );

    /// Bins under the assumption that the step size is known.
    static std::vector<size_t> bin_given_step_size(
        const Float _min,
        const Float _max,
        const Float _step_size,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin,
        const std::vector<containers::Match*>::const_iterator _end,
        std::vector<containers::Match*>* _bins );

   private:
    /// Generates the indptr, which indicates the beginning and end of
    /// each bin.
    static std::vector<size_t> make_indptr(
        const Float _min,
        const Float _max,
        const Float _step_size,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_NUMERICALBINNER_HPP_

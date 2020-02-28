#ifndef MULTIREL_UTILS_CATEGORICALBINNER_HPP_
#define MULTIREL_UTILS_CATEGORICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

class CategoricalBinner
{
   public:
    static std::
        pair<std::vector<size_t>, std::shared_ptr<const std::vector<Int>>>
        bin( const Int _min,
             const Int _max,
             const std::vector<containers::Match*>::const_iterator _begin,
             const std::vector<containers::Match*>::const_iterator _nan_begin,
             const std::vector<containers::Match*>::const_iterator _end,
             std::vector<containers::Match*>* _bins,
             multithreading::Communicator* _comm );

   private:
    /// Generates the critical values - a list of all category that have a count
    /// of at least one.
    static std::shared_ptr<const std::vector<Int>> make_critical_values(
        const Int _min,
        const Int _max,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin,
        multithreading::Communicator* _comm );

    /// Generates the indptr, which indicates the beginning and end of
    /// each bin.
    static std::vector<size_t> make_indptr(
        const Int _min,
        const Int _max,
        const std::vector<containers::Match*>::const_iterator _begin,
        const std::vector<containers::Match*>::const_iterator _nan_begin );
};

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_CATEGORICALBINNER_HPP_

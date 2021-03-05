#ifndef MULTIREL_UTILS_CATEGORICALBINNER_HPP_
#define MULTIREL_UTILS_CATEGORICALBINNER_HPP_

// ----------------------------------------------------------------------------

namespace multirel
{
namespace utils
{
// ----------------------------------------------------------------------------

template <class GetValueType>
using CategoricalBinner =
    binning::CategoricalBinner<containers::Match*, GetValueType>;

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace multirel

#endif  // MULTIREL_UTILS_CATEGORICALBINNER_HPP_

#ifndef RELBOOST_UTILS_MATCHMAKER_HPP_
#define RELBOOST_UTILS_MATCHMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "relboost/Float.hpp"
#include "relboost/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relboost {
namespace utils {
// ------------------------------------------------------------------------

struct Matchmaker {
  /// Identifies matches between population table and peripheral tables.
  static std::vector<containers::Match> make_matches(
      const containers::DataFrameView& _population,
      const containers::DataFrame& _peripheral,
      const std::shared_ptr<const std::vector<Float>>& _sample_weights);

  /// Makes "matches" for when Relboost is used as a predictor.
  static std::vector<containers::Match> make_matches(
      const containers::DataFrameView& _population);

  /// Identifies matches between a specific sample in the population table
  /// (signified by _ix_output and peripheral tables.
  static void make_matches(const containers::DataFrameView& _population,
                           const containers::DataFrame& _peripheral,
                           const size_t _ix_output,
                           std::vector<containers::Match>* _matches);
};

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

// ----------------------------------------------------------------------------

#endif  // RELBOOST_UTILS_MATCHMAKER_HPP_

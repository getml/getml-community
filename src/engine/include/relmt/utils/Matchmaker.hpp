#ifndef RELMT_UTILS_MATCHMAKER_HPP_
#define RELMT_UTILS_MATCHMAKER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "binning/binning.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
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
}  // namespace relmt

// ----------------------------------------------------------------------------

#endif  // RELMT_UTILS_MATCHMAKER_HPP_

#include "relboost/utils/Matchmaker.hpp"

namespace relboost {
namespace utils {
// ----------------------------------------------------------------------------

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrameView& _population,
    const containers::DataFrame& _peripheral,
    const std::shared_ptr<const std::vector<Float>>& _sample_weights) {
  const auto make_match = [](const size_t ix_input, const size_t ix_output) {
    return containers::Match{ix_input, ix_output};
  };

  return helpers::Matchmaker<
      containers::DataFrameView, containers::Match,
      decltype(make_match)>::make_matches(_population, _peripheral,
                                          _sample_weights, make_match);
}

// ----------------------------------------------------------------------------

std::vector<containers::Match> Matchmaker::make_matches(
    const containers::DataFrameView& _population) {
  std::vector<containers::Match> matches;

  for (size_t ix_output = 0; ix_output < _population.nrows(); ++ix_output) {
    matches.emplace_back(containers::Match{0, ix_output});
  }

  return matches;
}

// ----------------------------------------------------------------------------

void Matchmaker::make_matches(const containers::DataFrameView& _population,
                              const containers::DataFrame& _peripheral,
                              const size_t _ix_output,
                              std::vector<containers::Match>* _matches) {
  const auto make_match = [](const size_t ix_input, const size_t ix_output) {
    return containers::Match{ix_input, ix_output};
  };

  helpers::Matchmaker<containers::DataFrameView, containers::Match,
                      decltype(make_match)>::make_matches(_population,
                                                          _peripheral,
                                                          _ix_output,
                                                          make_match, _matches);
}

// ----------------------------------------------------------------------------
}  // namespace utils
}  // namespace relboost

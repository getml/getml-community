// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/dependency/DataFrameTracker.hpp"

#include "engine/utils/Getter.hpp"
#include "rfl/json.hpp"

namespace engine {
namespace dependency {

void DataFrameTracker::add(const containers::DataFrame& _df,
                           const commands::Fingerprint& _build_history) {
  clean_up();

  const auto b_str = rfl::json::write(_build_history);

  const auto b_hash = std::hash<std::string>()(b_str);

  const auto df_pair = std::make_pair(_df.name(), _df.last_change());

  pairs_.insert_or_assign(b_hash, df_pair);
}

// -------------------------------------------------------------------------

void DataFrameTracker::clean_up() {
  std::vector<size_t> remove_keys;

  for (const auto& [key, _] : pairs_) {
    if (!get_df(key)) {
      remove_keys.push_back(key);
    }
  }

  for (const auto key : remove_keys) {
    pairs_.erase(key);
  }
}

// -------------------------------------------------------------------------

void DataFrameTracker::clear() {
  pairs_ = std::map<size_t, std::pair<std::string, std::string>>();
}

// ----------------------------------------------------------------------

std::optional<containers::DataFrame> DataFrameTracker::get_df(
    const size_t _b_hash) const {
  const auto it = pairs_.find(_b_hash);

  if (it == pairs_.end()) {
    return std::nullopt;
  }

  const auto name = it->second.first;

  const auto last_change = it->second.second;

  const auto [df, exists] =
      utils::Getter::get_if_exists(name, data_frames_.get());

  if (!exists) {
    return std::nullopt;
  }

  if (df->last_change() != last_change) {
    return std::nullopt;
  }

  return *df;
}

// ----------------------------------------------------------------------

commands::Fingerprint DataFrameTracker::make_build_history(
    const std::vector<commands::Fingerprint>& _dependencies,
    const containers::DataFrame& _population_df,
    const std::vector<containers::DataFrame>& _peripheral_dfs) const {
  const auto data_frames = fct::join::vector<containers::DataFrame>(
      {std::vector<containers::DataFrame>({_population_df}), _peripheral_dfs});

  const auto get_fingerprint =
      [](const containers::DataFrame& _df) -> commands::Fingerprint {
    return _df.fingerprint();
  };

  const auto df_fingerprints =
      fct::collect::vector(data_frames | VIEWS::transform(get_fingerprint));

  using PipelineBuildHistory =
      typename commands::Fingerprint::PipelineBuildHistory;

  const auto build_history = PipelineBuildHistory{
      .dependencies = _dependencies, .df_fingerprints = df_fingerprints};

  return commands::Fingerprint(build_history);
}

// -------------------------------------------------------------------------

std::optional<containers::DataFrame> DataFrameTracker::retrieve(
    const commands::Fingerprint& _build_history) const {
  const auto b_str = rfl::json::write(_build_history);
  const auto b_hash = std::hash<std::string>()(b_str);
  return get_df(b_hash);
}

// -------------------------------------------------------------------------
}  // namespace dependency
}  // namespace engine

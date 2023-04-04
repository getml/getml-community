// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/ImportanceMaker.hpp"

namespace helpers {
// ----------------------------------------------------------------------------

void ImportanceMaker::add_to_importances(const ColumnDescription& _desc,
                                         const Float _value) {
  const auto it = importances_.find(_desc);

  if (it != importances_.end()) {
    it->second += _value;
    return;
  }

  importances_[_desc] = _value;
}

// ----------------------------------------------------------------------------

void ImportanceMaker::add_to_importance_factors(const size_t _ix,
                                                const Float _value) {
  if (_ix < importance_factors_avg_.size()) {
    importance_factors_avg_.at(_ix) += _value;
    return;
  }

  assert_true(_ix <
              importance_factors_avg_.size() + importance_factors_sum_.size());

  const auto ix = _ix - importance_factors_avg_.size();

  importance_factors_sum_.at(ix) += _value;
}

// ----------------------------------------------------------------------------

void ImportanceMaker::fill_zeros(const Schema& _pl, const std::string& _tname,
                                 const bool _is_population) {
  const auto marker = _is_population ? population() : peripheral();

  fill_zeros_from_columns(marker, _tname, _pl.val_.get<f_categoricals>());

  fill_zeros_from_columns(marker, _tname, _pl.val_.get<f_discretes>());

  fill_zeros_from_columns(marker, _tname, _pl.val_.get<f_numericals>());

  fill_zeros_from_columns(marker, _tname, _pl.val_.get<f_text>());

  fill_zeros_from_columns(marker, _tname, _pl.val_.get<f_time_stamps>());
}

// ----------------------------------------------------------------------------

void ImportanceMaker::fill_zeros_from_columns(
    const std::string& _marker, const std::string& _pname,
    const std::vector<std::string>& _colnames) {
  for (const auto& colname : _colnames) {
    const auto desc = ColumnDescription(_marker, _pname, colname);

    add_to_importances(desc, 0.0);
  }
}

// ----------------------------------------------------------------------------

std::vector<Float> ImportanceMaker::retrieve_fast_prop(
    const std::vector<ColumnDescription>& _fast_prop_descs) {
  std::vector<Float> importance_factors(_fast_prop_descs.size());

  for (size_t i = 0; i < _fast_prop_descs.size(); ++i) {
    const auto it = importances_.find(_fast_prop_descs.at(i));

    if (it == importances_.end()) {
      continue;
    }

    importance_factors.at(i) = it->second;

    importances_.erase(it);
  }

  return importance_factors;
}

// ----------------------------------------------------------------------------

void ImportanceMaker::merge(
    const std::map<ColumnDescription, Float>& _importances) {
  for (const auto& [key, value] : _importances) {
    add_to_importances(key, value);
  }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::multiply(const Float _importance_factor) {
  for (auto& [_, value] : importances_) {
    value *= _importance_factor;
  }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::normalize() {
  Float sum = 0.0;

  for (const auto& [_, value] : importances_) {
    sum += std::abs(value);
  }

  if (sum > 0.0) {
    for (auto& [_, value] : importances_) {
      value /= sum;
    }
  }
}

// ----------------------------------------------------------------------------

void ImportanceMaker::transfer(const ColumnDescription& _from,
                               const ColumnDescription& _to) {
  auto it = importances_.find(_from);

  if (it == importances_.end()) {
    return;
  }

  add_to_importances(_to, it->second);

  importances_.erase(it);
}

// ----------------------------------------------------------------------------

void ImportanceMaker::transfer_population() {
  auto importance_maker = ImportanceMaker();

  for (const auto& [key, value] : importances()) {
    const auto desc = ColumnDescription(ColumnDescription::PERIPHERAL,
                                        key.table(), key.name());

    importance_maker.add_to_importances(desc, value);
  }

  importances_ = importance_maker.importances();
}

// ----------------------------------------------------------------------------
}  // namespace helpers

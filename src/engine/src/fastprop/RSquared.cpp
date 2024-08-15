// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fastprop/algorithm/RSquared.hpp"

#include "fct/to.hpp"
#include "helpers/Aggregations.hpp"

namespace fastprop {
namespace algorithm {
// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calculate(
    const std::vector<containers::Column<Float>>& _targets,
    const containers::Features& _features,
    const std::vector<size_t>& _rownums) {
  const auto mean_targets = calc_mean_targets(_targets, _rownums);

  const auto var_targets = calc_var_targets(_targets, _rownums);

  const auto calc_r =
      [&mean_targets, &var_targets, &_targets,
       &_rownums](const helpers::Feature<Float, false>& feature) -> Float {
    return RSquared::calc_for_feature(mean_targets, var_targets, _targets,
                                      feature, _rownums);
  };

  return _features | std::views::transform(calc_r) |
         fct::ranges::to<std::vector>();
}

// ----------------------------------------------------------------------------

Float RSquared::calc_for_feature(
    const std::vector<Float>& _mean_targets,
    const std::vector<Float>& _var_targets,
    const std::vector<containers::Column<Float>>& _targets,
    const helpers::Feature<Float, false>& _feature,
    const std::vector<size_t>& _rownums) {
  assert_true(_mean_targets.size() == _targets.size());

  assert_true(_var_targets.size() == _targets.size());

  const auto calc = [_feature, &_rownums](const Float _mean_target,
                                          const Float _var_target,
                                          const auto& _targets) -> Float {
    return RSquared::calc_for_target(_mean_target, _var_target, _targets,
                                     _feature, _rownums);
  };

  const auto apply = [&calc, &_mean_targets, &_var_targets,
                      &_targets](const size_t _i) {
    return std::apply(calc, std::make_tuple(_mean_targets[_i], _var_targets[_i],
                                            _targets[_i]));
  };

  auto range =
      std::views::iota(0uz, _targets.size()) | std::views::transform(apply);

  return helpers::Aggregations::avg(range.begin(), range.end());
}

// ----------------------------------------------------------------------------

Float RSquared::calc_for_target(const Float _mean_target,
                                const Float _var_target,
                                const containers::Column<Float>& _targets,
                                const helpers::Feature<Float, false>& _feature,
                                const std::vector<size_t>& _rownums) {
  if (_var_target == 0.0) {
    return 0.0;
  }

  assert_true(_feature.size() == _targets.nrows_);

  const auto get_feature = [&_feature](size_t i) -> Float {
    assert_true(i < _feature.size());
    return _feature[i];
  };

  const auto get_target = [_targets](size_t i) -> Float {
    assert_true(i < _targets.nrows_);
    return _targets[i];
  };

  const auto feature = _rownums | std::views::transform(get_feature);

  const auto targets = _rownums | std::views::transform(get_target);

  const auto var_feature =
      helpers::Aggregations::var(feature.begin(), feature.end());

  if (var_feature == 0.0) {
    return 0.0;
  }

  const auto mean_feature =
      helpers::Aggregations::avg(feature.begin(), feature.end());

  const auto multiply = [_mean_target, mean_feature](const Float t,
                                                     const Float f) -> Float {
    return (t - _mean_target) * (f - mean_feature);
  };

  const auto nrows_float = static_cast<Float>(_rownums.size());

  const auto cross_corr =
      std::inner_product(targets.begin(), targets.end(), feature.begin(), 0.0,
                         std::plus<Float>(), multiply) /
      nrows_float;

  return (cross_corr / var_feature) * (cross_corr / _var_target);
}

// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calc_mean_targets(
    const std::vector<containers::Column<Float>>& _targets,
    const std::vector<size_t>& _rownums) {
  const auto calc_mean =
      [&_rownums](const containers::Column<Float>& _target) -> Float {
    const auto get_target = [&_target](size_t i) -> Float {
      assert_true(i < _target.nrows_);
      return _target[i];
    };

    const auto target = _rownums | std::views::transform(get_target);

    return helpers::Aggregations::avg(target.begin(), target.end());
  };

  return _targets | std::views::transform(calc_mean) |
         fct::ranges::to<std::vector>();
}

// ----------------------------------------------------------------------------

std::vector<Float> RSquared::calc_var_targets(
    const std::vector<containers::Column<Float>>& _targets,
    const std::vector<size_t>& _rownums) {
  const auto calc_var =
      [&_rownums](const containers::Column<Float>& _target) -> Float {
    const auto get_target = [&_target](size_t i) -> Float {
      assert_true(i < _target.nrows_);
      return _target[i];
    };

    const auto target = _rownums | std::views::transform(get_target);

    return helpers::Aggregations::var(target.begin(), target.end());
  };

  return _targets | std::views::transform(calc_var) |
         fct::ranges::to<std::vector>();
}

// ----------------------------------------------------------------------------
}  // namespace algorithm
}  // namespace fastprop

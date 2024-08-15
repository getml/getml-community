// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "metrics/AUC.hpp"

#include <numeric>

namespace metrics {

Float AUC::calc_auc(const std::vector<Float>& _true_positive_rate,
                    const std::vector<Float>& _false_positive_rate) const {
  Float auc = 0.0;

  for (size_t i = 1; i < _true_positive_rate.size(); ++i) {
    auc += (_false_positive_rate.at(i - 1) - _false_positive_rate.at(i)) *
           (_true_positive_rate.at(i) + _true_positive_rate.at(i - 1)) * 0.5;
  }

  return auc;
}

// ----------------------------------------------------------------------------

std::vector<Float> AUC::calc_false_positives(
    const std::vector<Float>& _true_positives,
    const std::vector<Float>& _predicted_negative) const {
  Float nrow_float = static_cast<Float>(nrows());

  std::vector<Float> false_positives(_true_positives.size());

  std::transform(_true_positives.begin(), _true_positives.end(),
                 _predicted_negative.begin(), false_positives.begin(),
                 [nrow_float](const Float& tp, const Float& pn) {
                   return nrow_float - tp - pn;
                 });

  return false_positives;
}

// ----------------------------------------------------------------------------

std::pair<std::vector<Float>, std::vector<Float>> AUC::calc_lift(
    const std::vector<Float>& _precision,
    const std::vector<Float>& _predicted_negative,
    const Float _all_negatives) const {
  const auto nrows_float = static_cast<Float>(nrows());

  const auto positive_share = (nrows_float - _all_negatives) / nrows_float;

  std::vector<Float> lift;

  std::vector<Float> proportion;

  for (size_t i = 0; i < _precision.size(); ++i) {
    if (positive_share > 0.0) {
      lift.push_back(_precision.at(i) / positive_share);

      proportion.push_back(1.0 - _predicted_negative.at(i) / nrows_float);
    }
  }

  return std::make_pair(lift, proportion);
}

// ----------------------------------------------------------------------------

std::vector<Float> AUC::calc_precision(
    const std::vector<Float>& _true_positives,
    const std::vector<Float>& _predicted_negative) const {
  const auto nrows_float = static_cast<Float>(nrows());

  std::vector<Float> precision(_true_positives.size());

  for (size_t i = 0; i < _true_positives.size(); ++i) {
    const auto predicted_positive = nrows_float - _predicted_negative.at(i);

    if (predicted_positive > 0.0) {
      precision.at(i) = _true_positives.at(i) / predicted_positive;
    } else {
      precision.at(i) = 1.0;
    }
  }

  return precision;
}

// ----------------------------------------------------------------------------

std::vector<Float> AUC::calc_rate(const std::vector<Float>& _raw,
                                  const Float _all) const {
  std::vector<Float> rate(_raw.size());

  std::transform(_raw.begin(), _raw.end(), rate.begin(),
                 [_all](const Float& raw) { return raw / _all; });

  return rate;
}

// ----------------------------------------------------------------------------

std::pair<std::vector<Float>, Float> AUC::calc_true_positives_uncompressed(
    const std::vector<std::pair<Float, Float>>& _pairs) const {
  auto true_positives_uncompressed = std::vector<Float>(nrows());

  for (size_t i = 0; i < nrows(); ++i) {
    true_positives_uncompressed.at(i) = std::get<1>(_pairs.at(i));
  }

  std::partial_sum(true_positives_uncompressed.begin(),
                   true_positives_uncompressed.end(),
                   true_positives_uncompressed.begin());

  const Float all_positives = true_positives_uncompressed.back();

  std::for_each(true_positives_uncompressed.begin(),
                true_positives_uncompressed.end(),
                [all_positives](Float& val) { val = all_positives - val; });

  return std::make_pair(true_positives_uncompressed, all_positives);
}

// ----------------------------------------------------------------------------

std::pair<std::vector<Float>, std::vector<Float>> AUC::compress(
    const std::vector<std::pair<Float, Float>>& _pairs,
    const std::vector<Float>& _true_positives_uncompressed,
    const Float _all_positives) const {
  auto true_positives = std::vector<Float>({_all_positives});

  auto predicted_negative = std::vector<Float>({0.0});

  for (size_t i = 0; i < _pairs.size();) {
    const auto it1 = _pairs.begin() + i;

    const auto prediction_is_greater = [it1](const std::pair<Float, Float>& p) {
      return std::get<0>(p) > std::get<0>(*it1);
    };

    const auto it2 = std::find_if(it1, _pairs.end(), prediction_is_greater);

    const auto dist = static_cast<size_t>(std::distance(it1, it2));

    assert_true(dist > 0);
    assert_true(i + dist - 1 < _true_positives_uncompressed.size());

    true_positives.push_back(_true_positives_uncompressed.at(i + dist - 1));

    predicted_negative.push_back(predicted_negative.back() +
                                 static_cast<Float>(dist));

    i += dist;
  }

  return std::make_pair(true_positives, predicted_negative);
}

// ----------------------------------------------------------------------------

std::vector<Float> AUC::downsample(const std::vector<Float>& _original) const {
  const auto step_size =
      std::max(_original.size() / 100, static_cast<size_t>(1));

  auto downsampled = std::vector<Float>(0);

  if (_original.size() == 0) {
    return downsampled;
  }

  for (size_t i = 0; i < _original.size(); i += step_size) {
    downsampled.push_back(_original.at(i));
  }

  downsampled.push_back(_original.back());

  return downsampled;
}

// ----------------------------------------------------------------------------

std::pair<Float, Float> AUC::find_min_max(const size_t _j) const {
  Float yhat_min = yhat(0, _j);

  Float yhat_max = yhat(0, _j);

  for (size_t i = 0; i < nrows(); ++i) {
    if (yhat(i, _j) < yhat_min)
      yhat_min = yhat(i, _j);

    else if (yhat(i, _j) > yhat_max)
      yhat_max = yhat(i, _j);
  }

  return std::make_pair(yhat_min, yhat_max);
}

// ----------------------------------------------------------------------------

void AUC::make_default_values(
    std::vector<std::vector<Float>>* _true_positive_arr,
    std::vector<std::vector<Float>>* _false_positive_arr,
    std::vector<std::vector<Float>>* _lift_arr,
    std::vector<std::vector<Float>>* _precision_arr,
    std::vector<std::vector<Float>>* _proportion_arr, Float* _auc) const {
  _true_positive_arr->push_back({1.0, 0.0});

  _false_positive_arr->push_back({0.0, 1.0});

  _lift_arr->push_back({1.0, 1.0});

  _precision_arr->push_back({0.0, 0.0});

  _proportion_arr->push_back({0.0, 1.0});

  *_auc = 0.5;
}

// ----------------------------------------------------------------------------

std::vector<std::pair<Float, Float>> AUC::make_pairs(const size_t _j) const {
  std::vector<std::pair<Float, Float>> pairs(nrows());

  for (size_t i = 0; i < pairs.size(); ++i) {
    pairs.at(i) = std::make_pair(yhat(i, _j), y(i, _j));
  }

  const auto sort_by_prediction = [](const std::pair<Float, Float>& p1,
                                     const std::pair<Float, Float>& p2) {
    return (std::get<0>(p1) < std::get<0>(p2));
  };

  std::stable_sort(pairs.begin(), pairs.end(), sort_by_prediction);

  return pairs;
}

// ----------------------------------------------------------------------------

typename AUC::ResultType AUC::score(const Features _yhat, const Features _y) {
  impl_.set_data(_yhat, _y);

  if (nrows() < 1) {
    std::runtime_error(
        "There needs to be at least one row for the AUC score to "
        "work!");
  }

  std::vector<Float> auc(ncols());

  auto true_positive_arr = std::vector<std::vector<Float>>();

  auto false_positive_arr = std::vector<std::vector<Float>>();

  auto lift_arr = std::vector<std::vector<Float>>();

  auto precision_arr = std::vector<std::vector<Float>>();

  auto proportion_arr = std::vector<std::vector<Float>>();

  for (size_t j = 0; j < ncols(); ++j) {
    const auto [yhat_min, yhat_max] = find_min_max(j);

    if (yhat_min == yhat_max) {
      make_default_values(&true_positive_arr, &false_positive_arr, &lift_arr,
                          &precision_arr, &proportion_arr, &auc.at(j));

      continue;
    }

    const auto pairs = make_pairs(j);

    const auto [true_positives_uncompressed, all_positives] =
        calc_true_positives_uncompressed(pairs);

    const auto [true_positives, predicted_negative] =
        compress(pairs, true_positives_uncompressed, all_positives);

    const auto false_positives =
        calc_false_positives(true_positives, predicted_negative);

    const Float all_negatives = static_cast<Float>(nrows()) - all_positives;

    const auto true_positive_rate = calc_rate(true_positives, all_positives);

    const auto false_positive_rate = calc_rate(false_positives, all_negatives);

    const auto precision = calc_precision(true_positives, predicted_negative);

    const auto [lift, proportion] =
        calc_lift(precision, predicted_negative, all_negatives);

    auc.at(j) = calc_auc(true_positive_rate, false_positive_rate);

    const auto tpr_downsampled = downsample(true_positive_rate);

    const auto fpr_downsampled = downsample(false_positive_rate);

    const auto lift_downsampled = downsample(lift);

    const auto precision_downsampled = downsample(precision);

    const auto proportion_downsampled = downsample(proportion);

    false_positive_arr.push_back(fpr_downsampled);

    true_positive_arr.push_back(tpr_downsampled);

    lift_arr.push_back(lift_downsampled);

    precision_arr.push_back(precision_downsampled);

    proportion_arr.push_back(proportion_downsampled);
  }

  return f_auc(auc) * f_fpr(false_positive_arr) * f_tpr(true_positive_arr) *
         f_lift(lift_arr) * f_precision(precision_arr) *
         f_proportion(proportion_arr);
}

// ----------------------------------------------------------------------------
}  // namespace metrics

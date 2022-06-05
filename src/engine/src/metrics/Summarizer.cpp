#include "metrics/Summarizer.hpp"

namespace metrics {
// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calc_categorical_column_plot(
    const size_t _num_bins, const std::vector<strings::String>& _vec) {
  // ------------------------------------------------------------------------

  if (_num_bins > 100000) {
    throw std::runtime_error("Number of bins cannot be greater than 100000!");
  }

  // ------------------------------------------------------------------------

  auto str_map =
      std::unordered_map<strings::String, Int, strings::StringHasher>();

  for (const auto& str : _vec) {
    auto it = str_map.find(str);

    if (it == str_map.end()) {
      str_map[str] = 1;
    } else {
      ++(it->second);
    }
  }

  // ------------------------------------------------------------------------

  auto counts = std::vector<std::pair<strings::String, Int>>(str_map.begin(),
                                                             str_map.end());

  const auto comp = [](const std::pair<strings::String, Int>& p1,
                       const std::pair<strings::String, Int>& p2) {
    return p1.second > p2.second;
  };

  std::sort(counts.begin(), counts.end(), comp);

  // ------------------------------------------------------------------------

  return make_object(_num_bins, counts);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calc_categorical_column_plot(
    const size_t _num_bins, const std::vector<strings::String>& _vec,
    const std::vector<Float>& _target) {
  // ------------------------------------------------------------------------

  if (_num_bins > 100000) {
    throw std::runtime_error("Number of bins cannot be greater than 100000!");
  }

  // ------------------------------------------------------------------------

  assert_true(_vec.size() == _target.size());

  // ------------------------------------------------------------------------

  auto str_map = std::unordered_map<strings::String, std::pair<Float, Float>,
                                    strings::StringHasher>();

  for (size_t i = 0; i < _vec.size(); ++i) {
    const auto& str = _vec[i];

    const auto val = _target[i];

    if (std::isnan(val) || std::isinf(val)) {
      continue;
    }

    auto it = str_map.find(str);

    if (it == str_map.end()) {
      str_map[str] = std::make_pair(1.0, val);
    } else {
      std::get<0>(it->second) += 1.0;
      std::get<1>(it->second) += val;
    }
  }

  // ------------------------------------------------------------------------

  auto pairs = std::vector<std::pair<Float, Float>>();

  for (const auto& p : str_map) {
    const auto avg = std::get<1>(p.second) / std::get<0>(p.second);
    pairs.emplace_back(std::make_pair(std::get<0>(p.second), avg));
  }

  const auto comp = [](const std::pair<Float, Float>& p1,
                       const std::pair<Float, Float>& p2) {
    return std::get<1>(p1) > std::get<1>(p2);
  };

  std::sort(pairs.begin(), pairs.end(), comp);

  // ------------------------------------------------------------------------

  return make_object(_num_bins, pairs);

  // ------------------------------------------------------------------------
}
// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Summarizer::calculate_average_targets(
    const std::vector<Float>& _minima, const std::vector<Float>& _step_sizes,
    const std::vector<size_t>& _actual_num_bins,
    const std::vector<std::vector<Int>>& _feature_densities,
    const Features& _features, const size_t _nrows, const size_t _ncols,
    const std::vector<const Float*>& _targets) {
  // ------------------------------------------------------------------------

  if (_targets.size() == 0) {
    return Poco::JSON::Array::Ptr(new Poco::JSON::Array());
  }

  // ------------------------------------------------------------------------
  // Init average_targets.

  assert_true(_actual_num_bins.size() == _step_sizes.size());

  assert_true(_actual_num_bins.size() == _ncols);

  const auto num_targets = _targets.size();

  auto average_targets = std::vector<std::vector<std::vector<Float>>>(_ncols);

  std::for_each(average_targets.begin(), average_targets.end(),
                [num_targets](std::vector<std::vector<Float>>& vec) {
                  vec.resize(num_targets);
                });

  for (size_t j = 0; j < _feature_densities.size(); ++j) {
    for (size_t k = 0; k < num_targets; ++k) {
      average_targets[j][k].resize(_actual_num_bins[j]);
    }
  }

  auto counts = average_targets;

  // ------------------------------------------------------------------------
  // Calculate sums.

  assert_true(_feature_densities.size() == _ncols);

  for (size_t j = 0; j < _ncols; ++j) {
    if (_actual_num_bins[j] == 0) {
      continue;
    }

    for (size_t i = 0; i < _nrows; ++i) {
      const auto feat = get(i, j, _features);

      if (std::isinf(feat) || std::isnan(feat)) {
        continue;
      }

      const auto bin =
          identify_bin(_actual_num_bins[j], _step_sizes[j], feat, _minima[j]);

      assert_true(bin < _feature_densities[j].size());

      for (size_t k = 0; k < average_targets[j].size(); ++k) {
        assert_true(bin < average_targets[j][k].size());

        const auto tar = _targets[k][i];

        if (std::isinf(tar) || std::isnan(tar)) {
          continue;
        }

        average_targets[j][k][bin] += tar;

        ++counts[j][k][bin];
      }
    }
  }

  // ------------------------------------------------------------------------
  // Divide targets by column densities .

  for (size_t j = 0; j < _ncols; ++j) {
    for (size_t k = 0; k < average_targets[j].size(); ++k) {
      for (size_t bin = 0; bin < average_targets[j][k].size(); ++bin) {
        if (counts[j][k][bin] > 0) {
          average_targets[j][k][bin] /= counts[j][k][bin];
        }
      }
    }
  }

  // ------------------------------------------------------------------------
  // Transform to JSON Array.

  Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());

  for (const auto& vec : average_targets) {
    Poco::JSON::Array::Ptr subarr(new Poco::JSON::Array());

    for (const auto& v : vec) {
      subarr->add(jsonutils::JSON::vector_to_array_ptr(v));
    }

    arr->add(subarr);
  }

  // ------------------------------------------------------------------------

  return arr;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_plots(
    const Features& _features, const size_t _nrows, const size_t _ncols,
    const size_t _num_bins, const std::vector<const Float*>& _targets) {
  if (_num_bins > 100000) {
    throw std::runtime_error("Number of bins cannot be greater than 100000!");
  }

  auto minima = std::vector<Float>(0);

  auto maxima = std::vector<Float>(0);

  find_min_and_max(_features, _nrows, _ncols, &minima, &maxima);

  // ------------------------------------------------------------------------
  // Calculate step_sizes and actual_num_bins. Note that actual_num_bins
  // can be smaller than num_bins.

  auto step_sizes = std::vector<Float>(0);

  auto actual_num_bins = std::vector<size_t>(0);

  calculate_step_sizes_and_num_bins(minima, maxima,
                                    static_cast<Float>(_num_bins), &step_sizes,
                                    &actual_num_bins);

  assert_true(step_sizes.size() == _ncols);
  assert_true(actual_num_bins.size() == _ncols);

  // ------------------------------------------------------------------------
  // Init feature densities.

  auto feature_densities = std::vector<std::vector<Int>>(_ncols);

  for (size_t j = 0; j < _ncols; ++j) {
    feature_densities[j].resize(actual_num_bins[j]);
  }

  // ------------------------------------------------------------------------
  // Calculate feature densities.

  for (size_t i = 0; i < _nrows; ++i) {
    for (size_t j = 0; j < _ncols; ++j) {
      const auto val = get(i, j, _features);

      if (actual_num_bins[j] == 0 || std::isinf(val) || std::isnan(val)) {
        continue;
      }

      auto bin =
          identify_bin(actual_num_bins[j], step_sizes[j], val, minima[j]);

      ++feature_densities[j][bin];
    }
  }

  // ------------------------------------------------------------------------
  // Transform to JSON Array.

  Poco::JSON::Array::Ptr densities_arr(new Poco::JSON::Array());

  for (const auto& c : feature_densities) {
    densities_arr->add(jsonutils::JSON::vector_to_array_ptr(c));
  }

  // ------------------------------------------------------------------------

  auto labels_arr =
      calculate_labels(minima, step_sizes, actual_num_bins, feature_densities,
                       _features, _nrows, _ncols);

  // ------------------------------------------------------------------------

  auto average_targets_arr = calculate_average_targets(
      minima, step_sizes, actual_num_bins, feature_densities, _features, _nrows,
      _ncols, _targets);

  // ------------------------------------------------------------------------
  // Add to JSON object.

  Poco::JSON::Object obj;

  obj.set("average_targets_", average_targets_arr);

  obj.set("feature_densities_", densities_arr);

  obj.set("labels_", labels_arr);

  return obj;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::calculate_feature_correlations(
    const Features& _features, const size_t _nrows, const size_t _ncols,
    const std::vector<const Float*>& _targets) {
  // -----------------------------------------------------------

  assert_true(_ncols == _features.size());

  const auto t_ncols = _targets.size();

  // -----------------------------------------------------------
  // Prepare datasets

  std::vector<Float> sum_yhat(_ncols);

  std::vector<Float> sum_yhat_yhat(_ncols);

  std::vector<Float> sum_y(t_ncols);

  std::vector<Float> sum_y_y(t_ncols);

  std::vector<Float> sum_yhat_y(_ncols * t_ncols);

  // -----------------------------------------------------------
  // Calculate sufficient statistics

  // Calculate sum yhat
  for (size_t i = 0; i < _nrows; ++i)
    for (size_t j = 0; j < _ncols; ++j) sum_yhat[j] += get(i, j, _features);

  // Calculate sum yhat_yhat
  for (size_t i = 0; i < _nrows; ++i)
    for (size_t j = 0; j < _ncols; ++j)
      sum_yhat_yhat[j] += get(i, j, _features) * get(i, j, _features);

  // Calculate sum y
  for (size_t k = 0; k < t_ncols; ++k)
    for (size_t i = 0; i < _nrows; ++i) sum_y[k] += _targets[k][i];

  // Calculate sum y_y
  for (size_t k = 0; k < t_ncols; ++k)
    for (size_t i = 0; i < _nrows; ++i)
      sum_y_y[k] += _targets[k][i] * _targets[k][i];

  // Calculate sum_yhat_y
  for (size_t k = 0; k < t_ncols; ++k)
    for (size_t i = 0; i < _nrows; ++i)
      for (size_t j = 0; j < _ncols; ++j)
        get(j, k, t_ncols, &sum_yhat_y) +=
            get(i, j, _features) * _targets[k][i];

  // -----------------------------------------------------------
  // Calculate correlations from sufficient statistics

  const auto n = static_cast<Float>(_nrows);

  auto feature_correlations =
      std::vector<std::vector<Float>>(_ncols, std::vector<Float>(t_ncols));

  for (size_t j = 0; j < _ncols; ++j)
    for (size_t k = 0; k < t_ncols; ++k) {
      const Float var_yhat =
          sum_yhat_yhat[j] / n - (sum_yhat[j] / n) * (sum_yhat[j] / n);

      const Float var_y = sum_y_y[k] / n - (sum_y[k] / n) * (sum_y[k] / n);

      const Float cov_y_yhat = get(j, k, t_ncols, &sum_yhat_y) / n -
                               (sum_yhat[j] / n) * (sum_y[k] / n);

      feature_correlations[j][k] = cov_y_yhat / sqrt(var_yhat * var_y);

      if (std::isnan(feature_correlations[j][k]) ||
          std::isinf(feature_correlations[j][k])) {
        feature_correlations[j][k] = 0.0;
      }
    }

  // -----------------------------------------------------------
  // Transform to JSON Array.

  Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());

  for (const auto& f : feature_correlations) {
    arr->add(jsonutils::JSON::vector_to_array_ptr(f));
  }

  // -----------------------------------------------------------
  // Add to JSON object.

  Poco::JSON::Object obj;

  obj.set("feature_correlations_", arr);

  return obj;

  // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Summarizer::calculate_labels(
    const std::vector<Float>& _minima, const std::vector<Float>& _step_sizes,
    const std::vector<size_t>& _actual_num_bins,
    const std::vector<std::vector<Int>>& _feature_densities,
    const Features& _features, const size_t _nrows, const size_t _ncols) {
  // ------------------------------------------------------------------------
  // Init labels.

  auto labels = std::vector<std::vector<Float>>(_ncols);

  for (size_t j = 0; j < _ncols; ++j) {
    labels[j].resize(_actual_num_bins[j]);
  }

  // ------------------------------------------------------------------------
  // Calculate feature densities.

  for (size_t i = 0; i < _nrows; ++i) {
    for (size_t j = 0; j < _ncols; ++j) {
      const auto val = get(i, j, _features);

      if (_actual_num_bins[j] == 0 || std::isinf(val) || std::isnan(val)) {
        continue;
      }

      auto bin =
          identify_bin(_actual_num_bins[j], _step_sizes[j], val, _minima[j]);

      labels[j][bin] += val;
    }
  }

  // ------------------------------------------------------------------------
  // Divide labels by column densities or replace with appropriate value.

  for (size_t j = 0; j < _ncols; ++j) {
    assert_true(labels[j].size() == _feature_densities[j].size());

    for (size_t bin = 0; bin < labels[j].size(); ++bin) {
      if (_feature_densities[j][bin] > 0) {
        labels[j][bin] /= static_cast<Float>(_feature_densities[j][bin]);
      } else {
        labels[j][bin] =
            _minima[j] + (static_cast<Float>(bin) + 0.5) * _step_sizes[j];
      }
    }
  }

  // ------------------------------------------------------------------------
  // Transform to JSON Array.

  Poco::JSON::Array::Ptr arr(new Poco::JSON::Array());

  for (const auto& l : labels) {
    arr->add(jsonutils::JSON::vector_to_array_ptr(l));
  }

  // ------------------------------------------------------------------------

  return arr;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Summarizer::calculate_step_sizes_and_num_bins(
    const std::vector<Float>& _minima, const std::vector<Float>& _maxima,
    const Float _num_bins, std::vector<Float>* _step_sizes,
    std::vector<size_t>* _actual_num_bins) {
  assert_true(_minima.size() == _maxima.size());

  _step_sizes->resize(_minima.size());

  _actual_num_bins->resize(_minima.size());

  for (size_t j = 0; j < _minima.size(); ++j) {
    const auto min = _minima[j];

    const auto max = _maxima[j];

    if (min >= max || std::isinf(min) || std::isnan(min) || std::isinf(max) ||
        std::isnan(max)) {
      continue;
    }

    (*_step_sizes)[j] = (max - min) / _num_bins;

    if (min >= max || std::isinf(min) || std::isnan(min) || std::isinf(max) ||
        std::isnan(max)) {
      continue;
    }

    (*_actual_num_bins)[j] =
        static_cast<size_t>((max - min) / (*_step_sizes)[j]);
  }
}

// ----------------------------------------------------------------------------

void Summarizer::find_min_and_max(const Features& _features,
                                  const size_t _nrows, const size_t _ncols,
                                  std::vector<Float>* _minima,
                                  std::vector<Float>* _maxima) {
  *_minima = std::vector<Float>(_ncols, std::numeric_limits<Float>::max());

  *_maxima = std::vector<Float>(_ncols, std::numeric_limits<Float>::lowest());

  for (size_t i = 0; i < _nrows; ++i) {
    for (size_t j = 0; j < _ncols; ++j) {
      if (get(i, j, _features) < (*_minima)[j]) {
        (*_minima)[j] = get(i, j, _features);
      } else if (get(i, j, _features) > (*_maxima)[j]) {
        (*_maxima)[j] = get(i, j, _features);
      }
    }
  }
}

// ----------------------------------------------------------------------------

size_t Summarizer::identify_bin(const size_t _num_bins, const Float _step_size,
                                const Float _val, const Float _min) {
  assert_true(_step_size > 0.0);

  // Note that this always rounds down.
  auto bin = static_cast<size_t>((_val - _min) / _step_size);

  assert_true(bin >= 0);

  // The maximum value will be out of range, if we do not
  // do this!
  if (bin >= _num_bins) {
    bin = _num_bins - 1;
  }

  return bin;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::make_object(
    const size_t _num_bins,
    const std::vector<std::pair<strings::String, Int>>& _counts) {
  // ------------------------------------------------------------------------

  if (_num_bins == 0) {
    throw std::runtime_error("Number of bins cannot be 0!");
  }

  // ------------------------------------------------------------------------

  Poco::JSON::Array::Ptr labels(new Poco::JSON::Array());

  Poco::JSON::Array::Ptr data(new Poco::JSON::Array());

  // ------------------------------------------------------------------------

  if (_counts.size() < _num_bins) {
    for (const auto& p : _counts) {
      labels->add(p.first.str());
      data->add(p.second);
    }
  } else {
    const auto entries_per_bin2 = _counts.size() / _num_bins;

    const auto entries_per_bin1 = entries_per_bin2 + 1;

    const auto entries_per_bin1_f = static_cast<Float>(entries_per_bin1);

    const auto entries_per_bin2_f = static_cast<Float>(entries_per_bin2);

    const auto remaining = _counts.size() % _num_bins;

    const auto add = [](const Int init,
                        const std::pair<strings::String, Int>& p) {
      return init + p.second;
    };

    for (size_t j = 0; j < remaining * entries_per_bin1;
         j += entries_per_bin1) {
      const auto sum =
          std::accumulate(_counts.begin() + j,
                          _counts.begin() + j + entries_per_bin1, 0.0, add);

      labels->add(std::string(""));

      data->add(static_cast<Float>(sum) / entries_per_bin1_f);
    }

    for (size_t j = remaining * entries_per_bin1; j < _counts.size();
         j += entries_per_bin2) {
      const auto sum =
          std::accumulate(_counts.begin() + j,
                          _counts.begin() + j + entries_per_bin2, 0.0, add);

      labels->add(std::string(""));

      data->add(static_cast<Float>(sum) / entries_per_bin2_f);
    }
  }

  // ------------------------

  Poco::JSON::Object obj;

  obj.set("labels_", labels);

  obj.set("data_", data);

  // ------------------------------------------------------------------------

  return obj;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Summarizer::make_object(
    const size_t _num_bins,
    const std::vector<std::pair<Float, Float>>& _pairs) {
  // ------------------------------------------------------------------------

  if (_num_bins == 0) {
    throw std::runtime_error("Number of bins cannot be 0!");
  }

  // ------------------------------------------------------------------------

  Poco::JSON::Array::Ptr labels(new Poco::JSON::Array());

  Poco::JSON::Array::Ptr data(new Poco::JSON::Array());

  // ------------------------------------------------------------------------

  if (_pairs.size() < _num_bins) {
    Float cumul = 0.0;

    for (const auto& p : _pairs) {
      cumul += p.first;
      labels->add(static_cast<Int>(cumul));
      data->add(p.second);
    }
  } else {
    const auto entries_per_bin2 = _pairs.size() / _num_bins;

    const auto entries_per_bin1 = entries_per_bin2 + 1;

    const auto remaining = _pairs.size() % _num_bins;

    const auto add_counts = [](const Float init,
                               const std::pair<Float, Float>& p) {
      return init + p.first;
    };

    const auto add_targets = [](const Float init,
                                const std::pair<Float, Float>& p) {
      return init + p.first * p.second;
    };

    Float cumul = 0.0;

    for (size_t j = 0; j < remaining * entries_per_bin1;
         j += entries_per_bin1) {
      const auto sum_counts = std::accumulate(
          _pairs.begin() + j, _pairs.begin() + j + entries_per_bin1, 0.0,
          add_counts);

      const auto sum_targets = std::accumulate(
          _pairs.begin() + j, _pairs.begin() + j + entries_per_bin1, 0.0,
          add_targets);

      cumul += sum_counts;

      labels->add(cumul);

      data->add(sum_targets / sum_counts);
    }

    for (size_t j = remaining * entries_per_bin1; j < _pairs.size();
         j += entries_per_bin2) {
      const auto sum_counts = std::accumulate(
          _pairs.begin() + j, _pairs.begin() + j + entries_per_bin2, 0.0,
          add_counts);

      const auto sum_targets = std::accumulate(
          _pairs.begin() + j, _pairs.begin() + j + entries_per_bin2, 0.0,
          add_targets);

      cumul += sum_counts;

      labels->add(cumul);

      data->add(sum_targets / sum_counts);
    }
  }

  // ------------------------------------------------------------------------

  Poco::JSON::Object obj;

  obj.set("accumulated_frequencies_", labels);

  obj.set("data_", data);

  // ------------------------------------------------------------------------

  return obj;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

}  // namespace metrics

#include "multirel/optimizationcriteria/RSquaredCriterion.hpp"

namespace multirel {
namespace optimizationcriteria {
// ----------------------------------------------------------------------------

RSquaredCriterion::RSquaredCriterion(
    const std::shared_ptr<const descriptors::Hyperparameters>& _hyperparameters,
    const containers::DataFrameView& _main_table,
    multithreading::Communicator* _comm)
    : OptimizationCriterion(),
      comm_(_comm),
      hyperparameters_(_hyperparameters),
      impl_(OptimizationCriterionImpl(_hyperparameters, _main_table, _comm)) {
  calc_sampling_rate();
  calc_residuals();
};

// ----------------------------------------------------------------------------

std::vector<size_t> RSquaredCriterion::argsort(const size_t _begin,
                                               const size_t _end) const {
  // ---------------------------------------------------------------------

  debug_log("argsort...");

  assert_true(_begin <= _end);

  assert_true(_begin >= 0);
  assert_true(_end <= impl().storage_ix());

  debug_log("Preparing sufficient statistics...");

  const auto sufficient_statistics =
      impl().reduce_sufficient_statistics_stored();

  // ---------------------------------------------------------------------

  debug_log("Calculating values...");

  std::vector<Float> values(_end - _begin);

  for (size_t i = _begin; i < _end; ++i) {
    values[i - _begin] = calculate_r_squared(i, sufficient_statistics);
  }

  // ---------------------------------------------------------------------

  debug_log("Calculating indices...");

  std::vector<size_t> indices(_end - _begin);

  for (size_t i = 0; i < _end - _begin; ++i) {
    indices[i] = i;
  }

  std::sort(indices.begin(), indices.end(),
            [&values](const size_t ix1, const size_t ix2) {
              return values[ix1] > values[ix2];
            });

  // ---------------------------------------------------------------------

  return indices;

  // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float RSquaredCriterion::calculate_r_squared(
    const size_t _i,
    const std::deque<std::vector<Float>>& _sufficient_statistics) const {
  assert_true(sample_weights_.size() == y_centered_[0].size());

  assert_true(_i < _sufficient_statistics.size());

  assert_true(_sufficient_statistics[_i].size() == y_.size() + 4);

  assert_true(sum_y_centered_y_centered_.size() == y_.size());

  const Float sum_yhat = _sufficient_statistics[_i][0];

  assert_true(sum_yhat == sum_yhat);

  const Float sum_yhat_yhat = _sufficient_statistics[_i][1];

  assert_true(sum_yhat_yhat == sum_yhat_yhat);

  Float result = 0.0;

  for (size_t j = 0; j < y_.size(); ++j) {
    const Float sum_y_centered_yhat = _sufficient_statistics[_i][2 + j];

    assert_true(!std::isnan(sum_y_centered_yhat));

    const Float var_yhat =
        sum_sample_weights_ * sum_yhat_yhat - sum_yhat * sum_yhat;

    if (var_yhat == 0.0 || sum_y_centered_y_centered_[j] == 0.0) {
      continue;
    }

    result += sum_sample_weights_ * (sum_y_centered_yhat / var_yhat) *
              (sum_y_centered_yhat / sum_y_centered_y_centered_[j]);

    debug_log("result: " + std::to_string(result) +
              ", sum_y_centered_yhat: " + std::to_string(sum_y_centered_yhat) +
              ", var_yhat: " + std::to_string(var_yhat));
  }

  assert_true(sample_weights_.size() == y_centered_[0].size());

  return result;
}

// ----------------------------------------------------------------------------

Int RSquaredCriterion::find_maximum() {
  assert_true(sample_weights_.size() == y_centered_[0].size());

  Int max_ix = 0;

  debug_log("Preparing sufficient statistics...");

  const auto sufficient_statistics =
      impl().reduce_sufficient_statistics_stored();

  debug_log("Finding maximum...");

  assert_true(sample_weights_.size() == y_centered_[0].size());

  assert_true(impl().storage_ix() == sufficient_statistics.size());

  impl().values_stored().resize(impl().storage_ix());

  for (size_t i = 0; i < sufficient_statistics.size(); ++i) {
    assert_true(i < impl().values_stored().size());

    impl().values_stored()[i] = 0.0;

    // num_samples_smaller and num_samples_greater are always the
    // elements in the last two columns of
    // sufficient_statistics_stored_, which is why
    // sufficient_statistics_stored_ has two extra columns over
    // ..._current and ..._committed.

    assert_true(sufficient_statistics[i].size() >= 2);

    const Float num_samples_smaller =
        sufficient_statistics[i][sufficient_statistics[i].size() - 2];

    const Float num_samples_greater =
        sufficient_statistics[i][sufficient_statistics[i].size() - 1];

    debug_log("num_samples_smaller: " + std::to_string(num_samples_smaller));

    debug_log("num_samples_greater: " + std::to_string(num_samples_greater));

    assert_true(hyperparameters_);

    assert_true(hyperparameters_->tree_hyperparameters_);

    const auto min_num_samples = static_cast<Float>(
        hyperparameters_->tree_hyperparameters_->min_num_samples_);

    // If the split would result in an insufficient number
    // of samples on any node, it will not be considered.
    if (num_samples_smaller < min_num_samples ||
        num_samples_greater < min_num_samples) {
      continue;
    }

    impl().values_stored()[i] = calculate_r_squared(i, sufficient_statistics);

    if (impl().values_stored()[i] > impl().values_stored()[max_ix]) {
      max_ix = static_cast<Int>(i);
    }
  }

  impl().set_max_ix(max_ix);

  debug_log("Done finding maximum...");

  assert_true(sample_weights_.size() == y_centered_[0].size());

  return max_ix;
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init(const std::vector<Float>& _sample_weights) {
  // ---------------------------------------------------------------------

  assert_true(_sample_weights.size() > 0);

  debug_log("Optimization init ");

  debug_log("_sample_weights.size(): " +
            std::to_string(_sample_weights.size()));

  // ---------------------------------------------------------------------

  sample_weights_ = _sample_weights;

  sufficient_statistics_committed_ = std::vector<Float>(y_.size() + 2);

  sufficient_statistics_current_ = std::vector<Float>(y_.size() + 2);

  sum_yhat_committed_ = sufficient_statistics_committed_.data();
  sum_yhat_current_ = sufficient_statistics_current_.data();

  sum_yhat_yhat_committed_ = sufficient_statistics_committed_.data() + 1;
  sum_yhat_yhat_current_ = sufficient_statistics_current_.data() + 1;

  sum_y_centered_yhat_committed_ = sufficient_statistics_committed_.data() + 2;

  sum_y_centered_yhat_current_ = sufficient_statistics_current_.data() + 2;

  sum_y_centered_y_centered_ = std::vector<Float>(y_.size());

  y_centered_ = std::vector<std::vector<Float>>(
      y_.size(), std::vector<Float>(_sample_weights.size()));

  // ---------------------------------------------------------------------
  // Calculate sum_sample_weights_

  sum_sample_weights_ =
      std::accumulate(sample_weights_.begin(), sample_weights_.end(), 0.0);

  utils::Reducer::reduce(std::plus<Float>(), &sum_sample_weights_, comm_);

  // ---------------------------------------------------------------------
  // Calculate y_mean

  auto y_mean = std::vector<Float>(y_.size());

  for (size_t j = 0; j < y_.size(); ++j) {
    assert_true(sample_weights_.size() == y_[j].size());

    for (size_t i = 0; i < y_[j].size(); ++i) {
      assert_true(!std::isnan(y_[j][i]) && !std::isinf(y_[j][i]));
      assert_true(!std::isnan(sample_weights_[i]) &&
                  !std::isinf(sample_weights_[i]));

      y_mean[j] += y_[j][i] * sample_weights_[i];
    }
  }

  utils::Reducer::reduce(std::plus<Float>(), &y_mean, comm_);

  for (auto& ym : y_mean) {
    ym /= sum_sample_weights_;
  }

  // ---------------------------------------------------------------------
  // Calculate y_centered

  for (size_t j = 0; j < y_.size(); ++j) {
    assert_true(y_centered_[j].size() == y_[j].size());

    for (size_t i = 0; i < y_[j].size(); ++i) {
      assert_true(!std::isnan(y_[j][i]));
      assert_true(!std::isnan(y_mean[j]));

      y_centered_[j][i] = y_[j][i] - y_mean[j];
    }
  }

  // ---------------------------------------------------------------------
  // Calculate sum_y_centered_y_centered_

  assert_true(sum_y_centered_y_centered_.size() == y_.size());

  for (size_t j = 0; j < y_.size(); ++j) {
    assert_true(y_centered_[j].size() == y_[j].size());
    assert_true(y_centered_[j].size() == sample_weights_.size());

    for (size_t i = 0; i < y_[j].size(); ++i) {
      assert_true(!std::isnan(y_centered_[j][i]));
      assert_true(!std::isnan(sample_weights_[i]));

      sum_y_centered_y_centered_[j] +=
          y_centered_[j][i] * y_centered_[j][i] * sample_weights_[i];
    }

    assert_true(!std::isnan(sum_y_centered_y_centered_[j]));
  }

  utils::Reducer::reduce(std::plus<Float>(), &sum_y_centered_y_centered_,
                         comm_);

  for (size_t j = 0; j < sum_y_centered_y_centered_.size(); ++j) {
    assert_true(!std::isnan(sum_y_centered_y_centered_[j]));
  }

  debug_log("y_centered_[0].size(): " + std::to_string(y_centered_[0].size()));

  // ---------------------------------------------------------------------

  debug_log("Optimization done.");

  assert_true(sample_weights_.size() == y_centered_[0].size());

  // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::init_yhat(const std::vector<Float>& _yhat,
                                  const containers::IntSet& _indices) {
  // ---------------------------------------------------------------------

  assert_true(sample_weights_.size() == y_centered_[0].size());

  // ---------------------------------------------------------------------

  debug_log("init_yhat...");

  debug_log("y_centered_[0].size(): " + std::to_string(y_centered_[0].size()));

  // ---------------------------------------------------------------------
  // Calculate y_hat_mean_

  y_hat_mean_ = 0.0;

  for (size_t i = 0; i < _yhat.size(); ++i) {
    y_hat_mean_ += _yhat[i] * sample_weights_[i];
  }

  utils::Reducer::reduce(std::plus<Float>(), &y_hat_mean_, comm_);

  y_hat_mean_ /= sum_sample_weights_;

  // ---------------------------------------------------------------------
  // sum_yhat_current_ is 0.0 by definition after calling init(...)
  // (because it is defined as the sum of all y_hat - y_hat_mean_)

  *sum_yhat_current_ = 0.0;

  // ---------------------------------------------------------------------
  // Calculate sum_yhat_yhat_current_

  *sum_yhat_yhat_current_ = 0.0;

  assert_true(_yhat.size() == sample_weights_.size());

  for (size_t i = 0; i < _yhat.size(); ++i) {
    *sum_yhat_yhat_current_ += (_yhat[i] - y_hat_mean_) *
                               (_yhat[i] - y_hat_mean_) * sample_weights_[i];
  }

  // ---------------------------------------------------------------------
  // Calculate sum_y_centered_yhat_current_

  assert_true(y_.size() == y_centered_.size());

  std::fill(sum_y_centered_yhat_current_,
            sum_y_centered_yhat_current_ + y_.size(), 0.0);

  for (size_t j = 0; j < y_.size(); ++j) {
    assert_true(_yhat.size() == y_centered_[j].size());

    for (size_t i = 0; i < _yhat.size(); ++i) {
      sum_y_centered_yhat_current_[j] +=
          (_yhat[i] - y_hat_mean_) * y_centered_[j][i] * sample_weights_[i];
    }
  }

  // ---------------------------------------------------------------------

  debug_log("init_yhat...done");

  // ---------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void RSquaredCriterion::update_samples(const containers::IntSet& _indices,
                                       const std::vector<Float>& _new_values,
                                       const std::vector<Float>& _old_values) {
  for (auto ix : _indices) {
    const Float new_value = _new_values[ix] - y_hat_mean_;
    const Float old_value = _old_values[ix] - y_hat_mean_;

    sum_yhat_current_[0] += (new_value - old_value) * sample_weights_[ix];

    sum_yhat_yhat_current_[0] +=
        (new_value * new_value - old_value * old_value) * sample_weights_[ix];

    for (size_t j = 0; j < y_.size(); ++j) {
      assert_true(ix < y_centered_[j].size());

      sum_y_centered_yhat_current_[j] +=
          y_centered_[j][ix] * (new_value - old_value) * sample_weights_[ix];
    }
  }

  assert_true(sample_weights_.size() == y_centered_[0].size());
}

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace multirel

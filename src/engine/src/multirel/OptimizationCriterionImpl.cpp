#include "multirel/optimizationcriteria/OptimizationCriterionImpl.hpp"

namespace multirel {
namespace optimizationcriteria {
// ----------------------------------------------------------------------------

OptimizationCriterionImpl::OptimizationCriterionImpl(
    const std::shared_ptr<const descriptors::Hyperparameters>& _hyperparameters,
    const containers::DataFrameView& _main_table,
    multithreading::Communicator* _comm)
    : comm_(_comm),
      hyperparameters_(_hyperparameters),
      main_table_(_main_table),
      max_ix_(-1),
      sampler_(utils::Sampler(_hyperparameters->seed_)),
      value_(0.0),
      yhat_old_(std::vector<std::vector<Float>>(
          main_table_.num_targets(), std::vector<Float>(main_table_.nrows()))) {
  assert_true(hyperparameters_);
  loss_function_ = lossfunctions::LossFunctionParser::parse_loss_function(
      hyperparameters_->loss_function_, comm_);
};

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::commit(
    std::vector<Float>* _sufficient_statistics_committed) {
  assert_true(max_ix_ >= 0);

  assert_true(max_ix_ < storage_ix());

  assert_true(max_ix_ < values_stored_.size());

  assert_true(max_ix_ < sufficient_statistics_stored_.size());

  const auto ncols = _sufficient_statistics_committed->size();

  // sufficient_statistics_stored_ has two extra columns for
  // num_samples_smaller and num_samples_greater
  assert_true(ncols == sufficient_statistics_stored_[max_ix_].size() - 2);

  std::copy(sufficient_statistics_stored_[max_ix_].begin(),
            sufficient_statistics_stored_[max_ix_].begin() + ncols,
            _sufficient_statistics_committed->begin());

  value_ = values_stored_[max_ix_];
}

// ----------------------------------------------------------------------------

std::deque<std::vector<Float>>
OptimizationCriterionImpl::reduce_sufficient_statistics_stored() const {
  std::deque<std::vector<Float>> sufficient_statistics_global;

  for (const auto& local : sufficient_statistics_stored()) {
    sufficient_statistics_global.push_back(std::vector<Float>(local.size()));

    multithreading::all_reduce(
        *comm_,                                      // comm
        local.data(),                                // in_values
        local.size(),                                // n
        sufficient_statistics_global.back().data(),  // out_values
        std::plus<Float>()                           // op
    );

    comm_->barrier();
  }

  return sufficient_statistics_global;
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::reset(
    std::vector<Float>* _sufficient_statistics_current,
    std::vector<Float>* _sufficient_statistics_committed) {
  std::fill(_sufficient_statistics_committed->begin(),
            _sufficient_statistics_committed->end(), 0.0);

  std::fill(_sufficient_statistics_current->begin(),
            _sufficient_statistics_current->end(), 0.0);

  reset_storage_size();
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::store_current_stage(
    const Float _num_samples_smaller, const Float _num_samples_greater,
    const std::vector<Float>& _sufficient_statistics_current) {
  // num_samples_smaller and num_samples_greater are always the elements
  // in the last two columns of sufficient_statistics_stored_, which is
  // why sufficient_statistics_stored_ has two extra columns over ..._current
  // and ..._committed.

  sufficient_statistics_stored_.push_back(
      std::vector<Float>(_sufficient_statistics_current.size() + 2));

  std::copy(_sufficient_statistics_current.begin(),
            _sufficient_statistics_current.end(),
            sufficient_statistics_stored_.back().begin());

  sufficient_statistics_stored_.back()[_sufficient_statistics_current.size()] =
      _num_samples_smaller;

  sufficient_statistics_stored_
      .back()[_sufficient_statistics_current.size() + 1] = _num_samples_greater;
}

// ----------------------------------------------------------------------------

void OptimizationCriterionImpl::update_yhat_old(
    const std::vector<std::vector<Float>>& _residuals,
    const std::vector<Float>& _sample_weights,
    const std::vector<Float>& _yhat_new) {
  // ----------------------------------------------------------------

  assert_true(hyperparameters_);

  const auto shrinkage = hyperparameters_->shrinkage_;

  if (shrinkage <= 0.0) {
    return;
  }

  // ----------------------------------------------------------------
  // Train a linear regression from the prediction of the last
  // tree on the residuals and generate predictions f_t on that basis

  auto linear_regression = utils::LinearRegression(comm_);

  linear_regression.fit(_yhat_new, _residuals, _sample_weights);

  const auto predictions = linear_regression.predict(_yhat_new);

  // ----------------------------------------------------------------
  // Find the optimal update_rates and update parameters of linear
  // regression accordingly.

  auto update_rates = loss_function()->calculate_update_rates(
      yhat_old_, predictions, main_table_, _sample_weights);

  // ----------------------------------------------------------------
  // Do the actual updates

  assert_true(update_rates.size() == predictions.size());
  assert_true(update_rates.size() == yhat_old_.size());

  for (size_t j = 0; j < predictions.size(); ++j) {
    assert_true(yhat_old_[j].size() == predictions[j].size());

    for (size_t i = 0; i < predictions[j].size(); ++i) {
      const Float update = predictions[j][i] * update_rates[j] * shrinkage;

      yhat_old_[j][i] +=
          ((std::isnan(update) || std::isinf(update)) ? 0.0 : update);
    }
  }

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace multirel

#ifndef MULTIREL_OPTIMIZATIONCRITERIONIMPL_HPP_
#define MULTIREL_OPTIMIZATIONCRITERIONIMPL_HPP_

// ----------------------------------------------------------------------------

#include <deque>
#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "multirel/Float.hpp"
#include "multirel/Int.hpp"
#include "multirel/containers/containers.hpp"
#include "multirel/descriptors/descriptors.hpp"
#include "multirel/lossfunctions/lossfunctions.hpp"
#include "multirel/utils/utils.hpp"

// ----------------------------------------------------------------------------

namespace multirel {
namespace optimizationcriteria {
// ----------------------------------------------------------------------------

class OptimizationCriterionImpl {
 public:
  OptimizationCriterionImpl(
      const std::shared_ptr<const descriptors::Hyperparameters>&
          _hyperparameters,
      const containers::DataFrameView& _main_table,
      multithreading::Communicator* _comm);

  ~OptimizationCriterionImpl() = default;

  // --------------------------------------

  /// Commits the current stage, accepting it as the new state of the
  /// tree
  void commit(std::vector<Float>* _sufficient_statistics_committed);

  /// Resets sufficient statistics to zero
  void reset(std::vector<Float>* _sufficient_statistics_current,
             std::vector<Float>* _sufficient_statistics_committed);

  /// Returns the sum of all sufficient statistics stored in the individual
  /// processes
  std::deque<std::vector<Float>> reduce_sufficient_statistics_stored() const;

  /// Reverts to the committed version
  void revert_to_commit();

  /// Stores the current stage of the sufficient statistics
  void store_current_stage(
      const Float _num_samples_smaller, const Float _num_samples_greater,
      const std::vector<Float>& _sufficient_statistics_current);

  /// Updates yhat_old based on _yhat_new.
  void update_yhat_old(const std::vector<std::vector<Float>>& _residuals,
                       const std::vector<Float>& _sample_weights,
                       const std::vector<Float>& _yhat_new);

  // --------------------------------------

  /// Calculates the residuals.
  void calc_residuals(std::vector<std::vector<Float>>* _residuals) {
    *_residuals = loss_function()->calculate_residuals(yhat_old_, main_table_);
  }

  /// Calculates the sampling rate.
  void calc_sampling_rate() {
    assert_true(comm_ != nullptr);
    assert_true(hyperparameters_);
    sampler_.calc_sampling_rate(main_table_.nrows(),
                                hyperparameters_->sampling_factor_, comm_);
  }

  /// Generates a new set of sample weights.
  std::shared_ptr<std::vector<Float>> make_sample_weights() {
    return sampler_.make_sample_weights(main_table_.nrows());
  }

  /// Resets the storage size to zero.
  void reset_storage_size() {
    max_ix_ = -1;
    sufficient_statistics_stored_.clear();
    values_stored_.clear();
  }

  /// Resets yhat_old to the initial value.
  void reset_yhat_old() {
    for (auto& y : yhat_old_) {
      std::fill(y.begin(), y.end(), 0.0);
    }
  }

  /// Sets the indicator of the best split
  inline void set_max_ix(const Int _max_ix) { max_ix_ = _max_ix; }

  /// Returns the current storage_ix.
  inline const size_t storage_ix() const {
    return sufficient_statistics_stored_.size();
  }

  /// Trivial getter
  inline Float value() { return value_; }

  /// Trivial getter
  inline Float values_stored(const size_t _i) {
    if (_i < storage_ix()) {
      assert_true(_i < values_stored().size());

      return values_stored()[_i];
    } else {
      return 0.0;
    }
  }

  /// Trivial getter
  inline std::vector<Float>& values_stored() { return values_stored_; }

  // --------------------------------------

 private:
  /// Trivial (private) accessor
  inline lossfunctions::LossFunction* loss_function() {
    assert_true(loss_function_);
    return loss_function_.get();
  }

  /// Trivial (private) accessor
  inline std::deque<std::vector<Float>>& sufficient_statistics_stored() {
    return sufficient_statistics_stored_;
  }

  /// Trivial (private) accessor
  inline const std::deque<std::vector<Float>>& sufficient_statistics_stored()
      const {
    return sufficient_statistics_stored_;
  }

  // --------------------------------------

 private:
  /// Multithreading communicator
  multithreading::Communicator* comm_;

  /// The hyperparameters used to train the model.
  std::shared_ptr<const descriptors::Hyperparameters> hyperparameters_;

  /// The loss function used.
  std::shared_ptr<lossfunctions::LossFunction> loss_function_;

  /// The main table containing the targets..
  containers::DataFrameView main_table_;

  /// Indicates the best split.
  Int max_ix_;

  /// For creating the sample weights
  utils::Sampler sampler_;

  /// Stores the sufficient statistics when store_current_stage(...)
  /// is called
  std::deque<std::vector<Float>> sufficient_statistics_stored_;

  /// Value of the optimization criterion of the currently
  /// committed stage
  Float value_;

  /// Stores the values calculated by find maximum. Can be resized
  /// by set_storage_size
  std::vector<Float> values_stored_;

  /// The current predictions generated by the  previous features.
  std::vector<std::vector<Float>> yhat_old_;
};

// ----------------------------------------------------------------------------
}  // namespace optimizationcriteria
}  // namespace multirel

#endif  // MULTIREL_OPTIMIZATIONCRITERIONIMPL_HPP_

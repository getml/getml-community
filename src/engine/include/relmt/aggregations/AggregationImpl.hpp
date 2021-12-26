#ifndef RELMT_AGGREGATIONS_AGGREGATIONIMPL_HPP_
#define RELMT_AGGREGATIONS_AGGREGATIONIMPL_HPP_

// ----------------------------------------------------------------------------

#include <array>
#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"
#include "relmt/containers/containers.hpp"
#include "relmt/lossfunctions/lossfunctions.hpp"

// ----------------------------------------------------------------------------

namespace relmt {
namespace aggregations {

// -------------------------------------------------------------------------

/// AggregationImpl implements helper functions that are needed
/// by all aggregations.
class AggregationImpl {
  // -----------------------------------------------------------------

 public:
  AggregationImpl(
      lossfunctions::LossFunction* _child, std::vector<Float>* _eta1,
      std::vector<Float>* _eta1_old, std::vector<Float>* _eta2,
      std::vector<Float>* _eta2_old, containers::IntSet* _indices,
      containers::IntSet* _indices_current,
      const std::optional<containers::Rescaled>& _input = std::nullopt,
      const std::optional<containers::Rescaled>& _output = std::nullopt)
      : child_(_child),
        eta1_(*_eta1),
        eta1_old_(*_eta1_old),
        eta2_(*_eta2),
        eta2_old_(*_eta2_old),
        indices_(*_indices),
        indices_current_(*_indices_current),
        ncols_(0) {
    if (_input) {
      input_.reset(new containers::Rescaled(*_input));
    }

    if (_output) {
      output_.reset(new containers::Rescaled(*_output));
    }

    if (input_ && output_) {
      ncols_ = input().ncols() + output().ncols();
    }
  }

  ~AggregationImpl() = default;

  // -----------------------------------------------------------------

 public:
  /// Commits the _weights.
  void commit(const containers::Weights& _weights);

  /// Returns the loss reduction associated with a split.
  Float evaluate_split(const Float _old_intercept, const Float _old_weight,
                       const containers::Weights& _weights);

  /// Helper class that determines whether the min_num_samples requirement is
  /// fulfilled.
  bool is_balanced(const Float _num_samples_1, const Float _num_samples_2,
                   const Float _min_num_samples,
                   multithreading::Communicator* _comm) const;

  /// Resets the critical resources to zero.
  void reset();

  /// Resizes critical resources.
  void resize(size_t _nrows, size_t _ncols);

  /// Reverts the weights to the last time commit has been called.
  void revert_to_commit();

  /// Updates one line in _eta (called by calc_all(...))
  void update_eta(const size_t _ix_input, const size_t _ix_output,
                  const Float _divisor, Float* _eta) const;

  /// Updates one line in _eta1 and _eta2 (called by calc_diff(...))
  void update_etas(const size_t _ix_input, const size_t _ix_output,
                   const Float _divisor, Float* _eta1, Float* _eta2) const;

  // -----------------------------------------------------------------

 public:
  /// Trivial (const) accessor.
  size_t ncols() const { return ncols_; }

  // -----------------------------------------------------------------

 private:
  /// Trivial (const) accessor.
  const containers::Rescaled& input() const {
    assert_true(input_);
    return *input_;
  }

  /// The number of rows in the output table.
  size_t nrows() const { return output().nrows(); }

  /// Trivial (const) accessor.
  const containers::Rescaled& output() const {
    assert_true(output_);
    return *output_;
  }

  // -----------------------------------------------------------------

 private:
  /// Sets the etas to 0.
  void reset_etas();

  // -----------------------------------------------------------------

 private:
  /// Either The next higher level of aggregation or the loss function.
  lossfunctions::LossFunction* const child_;

  /// Parameters for weight 1.
  std::vector<Float>& eta1_;

  /// Parameters for weight 1 as of the last split.
  std::vector<Float>& eta1_old_;

  /// Parameters for weight 2.
  std::vector<Float>& eta2_;

  /// Parameters for weight 2 as of the last split.
  std::vector<Float>& eta2_old_;

  /// Keeps track of the samples that have been altered.
  containers::IntSet& indices_;

  /// Keeps track of the samples that have been altered since the last split.
  containers::IntSet& indices_current_;

  /// The input data used for this aggregation (if this is the lowest level).
  containers::Optional<containers::Rescaled> input_;

  /// The number of columns that are included in the linear equation.
  size_t ncols_;

  /// The output data used for this aggregation (if this is the lowest level).
  containers::Optional<containers::Rescaled> output_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

#endif  // RELMT_AGGREGATIONS_AGGREGATIONIMPL_HPP_

#ifndef RELMT_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_
#define RELMT_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"
#include "multithreading/multithreading.hpp"

// ----------------------------------------------------------------------------

#include "relmt/Float.hpp"
#include "relmt/Int.hpp"
#include "relmt/containers/containers.hpp"
#include "relmt/enums/enums.hpp"
#include "relmt/lossfunctions/lossfunctions.hpp"

// ----------------------------------------------------------------------------

#include "relmt/aggregations/AggregationIndex.hpp"

// ----------------------------------------------------------------------------
namespace relmt {
namespace aggregations {
// -------------------------------------------------------------------------

/// IntermediateAggregationImpl implements helper functions that are needed
/// by all aggregations when they are used as IntermediateAggregations.
class IntermediateAggregationImpl {
  // -----------------------------------------------------------------

 public:
  IntermediateAggregationImpl(
      const std::shared_ptr<AggregationIndex>& _agg_index,
      const std::shared_ptr<lossfunctions::LossFunction>& _child)
      : agg_index_(_agg_index),
        child_(_child),
        indices_(containers::IntSet(0)),
        indices_current_(containers::IntSet(0)),
        ncols_(0) {
    counts_ = std::vector<Float>(agg_index().nrows(), NAN);

    indices_ = containers::IntSet(agg_index().nrows());
    indices_current_ = containers::IntSet(agg_index().nrows());
  }

  ~IntermediateAggregationImpl() = default;

  // -----------------------------------------------------------------

 public:
  /// Calculates the etas needed.
  std::tuple<const std::vector<Float>*, const std::vector<Float>*,
             const std::vector<Float>*, const std::vector<Float>*>
  calc_etas(const bool _divide_by_count, const enums::Aggregation _agg,
            const std::vector<size_t>& _indices_current,
            const std::vector<Float>& _eta1,
            const std::vector<Float>& _eta1_old,
            const std::vector<Float>& _eta2,
            const std::vector<Float>& _eta2_old);

  /// Calculates indices_, eta1_ and eta2_ given the previous
  /// iteration's variables.
  std::pair<Float, containers::Weights> calc_pair(
      const bool _divide_by_count, const enums::Aggregation _agg,
      const enums::Revert _revert, const enums::Update _update,
      const std::vector<Float>& _old_weights,
      const std::vector<size_t>& _indices,
      const std::vector<size_t>& _indices_current,
      const std::vector<Float>& _eta1, const std::vector<Float>& _eta1_old,
      const std::vector<Float>& _eta2, const std::vector<Float>& _eta2_old);

  /// Applies the aggregation on the predictions.
  std::shared_ptr<std::vector<Float>> reduce_predictions(
      const bool _divide_by_count,
      const std::vector<Float>& _input_predictions);

  /// Resets everything to zero.
  void reset(const bool _reset_child);

  /// Update the "old" values.
  void update_etas_old(const enums::Aggregation _agg);

  // -----------------------------------------------------------------

 public:
  /// Trivial (const) accessor.
  const std::vector<size_t>& indices() const {
    return indices_.unique_integers();
  }

  /// Trivial (const) accessor.
  const std::vector<size_t>& indices_current() const {
    return indices_current_.unique_integers();
  }

  // -----------------------------------------------------------------

 private:
  /// Trivial accessor
  size_t ncols() const { return ncols_; }

  /// The expected number of rows for the output etas.
  size_t nrows() const { return counts_.size(); }

  /// Resizes the critical resources.
  void resize(const size_t _ncols);

  /// Update the output.
  void update_etas(const bool _divide_by_count,
                   const std::vector<size_t>& _indices_current,
                   const std::vector<Float>& _eta1_input,
                   const std::vector<Float>& _eta1_input_old,
                   const std::vector<Float>& _eta2_input,
                   const std::vector<Float>& _eta2_input_old,
                   std::vector<Float>* _eta1_output,
                   std::vector<Float>* _eta2_output);

  void update_etas_divide_by_count(const std::vector<size_t>& _indices_current,
                                   const std::vector<Float>& _eta1_input,
                                   const std::vector<Float>& _eta1_input_old,
                                   const std::vector<Float>& _eta2_input,
                                   const std::vector<Float>& _eta2_input_old,
                                   std::vector<Float>* _eta1_output,
                                   std::vector<Float>* _eta2_output);

  void update_etas_dont_divide(const std::vector<size_t>& _indices_current,
                               const std::vector<Float>& _eta1_input,
                               const std::vector<Float>& _eta1_input_old,
                               const std::vector<Float>& _eta2_input,
                               const std::vector<Float>& _eta2_input_old,
                               std::vector<Float>* _eta1_output,
                               std::vector<Float>* _eta2_output);

  // -----------------------------------------------------------------

 private:
  /// Trivial (private) accessor
  AggregationIndex& agg_index() {
    assert_true(agg_index_);
    return *agg_index_;
  }

  /// If the counts were already calculated, we can retrieve them.
  Float get_count(const Int _ix_output) {
    assert_true(_ix_output >= 0);
    assert_true(static_cast<size_t>(_ix_output) < counts_.size());

    if (std::isnan(counts_[_ix_output]))
      counts_[_ix_output] = agg_index().get_count(_ix_output);

    return counts_[_ix_output];
  }

  // -----------------------------------------------------------------

 private:
  /// The aggregation index, which is needed for
  /// intermediate aggregations.
  std::shared_ptr<AggregationIndex> agg_index_;

  /// The child of this loss function.
  std::shared_ptr<lossfunctions::LossFunction> child_;

  /// Counts the number of instances for each ix_output.
  std::vector<Float> counts_;

  /// Parameters for weight 1.
  std::vector<Float> eta1_;

  /// Parameters for weight 1 as of the last split.
  std::vector<Float> eta1_old_;

  /// Parameters for weight 2.
  std::vector<Float> eta2_;

  /// Parameters for weight 2 as of the last split.
  std::vector<Float> eta2_old_;

  /// Keeps track of the samples that have been altered.
  containers::IntSet indices_;

  /// Keeps track of the samples that have been altered since the last split.
  containers::IntSet indices_current_;

  /// The number of columns for the etas and w_fixed.
  size_t ncols_;
};

// -------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

#endif  // RELMT_AGGREGATIONS_INTERMEDIATEAGGREGATIONIMPL_HPP_

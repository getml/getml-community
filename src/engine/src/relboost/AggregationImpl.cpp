#include "relboost/aggregations/AggregationImpl.hpp"

#include "relboost/utils/utils.hpp"

namespace relboost {
namespace aggregations {
// ----------------------------------------------------------------------------

void AggregationImpl::commit(const std::array<Float, 3>& _weights) {
  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == eta1_old_.size());
  assert_true(eta1_.size() == eta2_old_.size());

  child_->commit(indices_.unique_integers(), _weights);

  for (auto ix : indices_) {
    assert_true(ix < eta1_.size());
    eta1_[ix] = 0.0;
    eta1_old_[ix] = 0.0;
    eta2_[ix] = 0.0;
    eta2_old_[ix] = 0.0;
  }

  indices_.clear();
  indices_current_.clear();
};

// ----------------------------------------------------------------------------

bool AggregationImpl::is_balanced(const Float _num_samples_1,
                                  const Float _num_samples_2,
                                  const Float _min_num_samples,
                                  multithreading::Communicator* _comm) const {
  auto global_num_samples_1 = _num_samples_1;

  auto global_num_samples_2 = _num_samples_2;

  assert_true(_comm != nullptr);

  utils::Reducer::reduce<Float>(std::plus<Float>(), &global_num_samples_1,
                                _comm);

  utils::Reducer::reduce<Float>(std::plus<Float>(), &global_num_samples_2,
                                _comm);

  return (global_num_samples_1 > _min_num_samples &&
          global_num_samples_2 > _min_num_samples);
}

// ----------------------------------------------------------------------------

void AggregationImpl::reset() {
  indices_.clear();
  indices_current_.clear();

  std::fill(eta1_.begin(), eta1_.end(), 0.0);
  std::fill(eta2_.begin(), eta2_.end(), 0.0);
  std::fill(eta1_old_.begin(), eta1_old_.end(), 0.0);
  std::fill(eta2_old_.begin(), eta2_old_.end(), 0.0);

  child_->reset();
}

// ----------------------------------------------------------------------------

void AggregationImpl::resize(size_t _size) {
  eta1_ = std::vector<Float>(_size);
  eta1_old_ = std::vector<Float>(_size);

  eta2_ = std::vector<Float>(_size);
  eta2_old_ = std::vector<Float>(_size);

  indices_ = containers::IntSet(_size);

  indices_current_ = containers::IntSet(_size);
}

// ----------------------------------------------------------------------------

void AggregationImpl::revert_to_commit() {
  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == eta1_old_.size());
  assert_true(eta1_.size() == eta2_old_.size());

  for (auto ix : indices_) {
    assert_true(ix < eta1_.size());
    eta1_[ix] = 0.0;
    eta1_old_[ix] = 0.0;
    eta2_[ix] = 0.0;
    eta2_old_[ix] = 0.0;
  }

  child_->revert_to_commit(indices_.unique_integers());

  indices_.clear();
  indices_current_.clear();
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relboost

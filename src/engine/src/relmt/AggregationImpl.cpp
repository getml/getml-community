#include "relmt/aggregations/AggregationImpl.hpp"

#include "relmt/utils/utils.hpp"

namespace relmt {
namespace aggregations {
// ----------------------------------------------------------------------------

void AggregationImpl::commit(const containers::Weights& _weights) {
  reset_etas();

  child_->commit(indices_.unique_integers(), _weights);

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

void AggregationImpl::reset_etas() {
  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == nrows() * ncolsplus1);
  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == eta1_old_.size());
  assert_true(eta1_.size() == eta2_old_.size());

  for (auto ix : indices_) {
    assert_true(ix < nrows());

    for (size_t j = 0; j < ncolsplus1; ++j) {
      const auto ix2 = ix * ncolsplus1 + j;

      eta1_[ix2] = 0.0;
      eta1_old_[ix2] = 0.0;
      eta2_[ix2] = 0.0;
      eta2_old_[ix2] = 0.0;
    }
  }
}

// ----------------------------------------------------------------------------

void AggregationImpl::resize(size_t _nrows, size_t _ncols) {
  const auto ncolsplus1 = _ncols + 1;

  eta1_ = std::vector<Float>(_nrows * ncolsplus1);
  eta1_old_ = std::vector<Float>(_nrows * ncolsplus1);

  eta2_ = std::vector<Float>(_nrows * ncolsplus1);
  eta2_old_ = std::vector<Float>(_nrows * ncolsplus1);

  indices_ = containers::IntSet(_nrows);

  indices_current_ = containers::IntSet(_nrows);
}

// ----------------------------------------------------------------------------

void AggregationImpl::revert_to_commit() {
  reset_etas();

  child_->revert_to_commit(indices_.unique_integers());

  indices_.clear();
  indices_current_.clear();
}

// ----------------------------------------------------------------------------

void AggregationImpl::update_eta(const size_t _ix_input,
                                 const size_t _ix_output, const Float _divisor,
                                 Float* _eta) const {
  ++_eta[0];

  size_t i = 1;

  const auto output_row = output().row(_ix_output);

  for (size_t j = 0; j < output().ncols(); ++i, ++j) {
    _eta[i] += output_row[j] / _divisor;
  }

  const auto input_row = input().row(_ix_input);

  for (size_t j = 0; j < input().ncols(); ++i, ++j) {
    _eta[i] += input_row[j] / _divisor;
  }
}

// ----------------------------------------------------------------------------

void AggregationImpl::update_etas(const size_t _ix_input,
                                  const size_t _ix_output, const Float _divisor,
                                  Float* _eta1, Float* _eta2) const {
  assert_true(_ix_input < input().nrows());
  assert_true(_ix_output < output().nrows());

  ++_eta1[0];
  --_eta2[0];

  size_t i = 1;

  const auto output_row = output().row(_ix_output);

  for (size_t j = 0; j < output().ncols(); ++i, ++j) {
    const auto val = output_row[j] / _divisor;
    _eta1[i] += val;
    _eta2[i] -= val;
  }

  const auto input_row = input().row(_ix_input);

  for (size_t j = 0; j < input().ncols(); ++i, ++j) {
    const auto val = input_row[j] / _divisor;
    _eta1[i] += val;
    _eta2[i] -= val;
  }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

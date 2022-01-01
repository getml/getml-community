#include "relmt/aggregations/aggregations.hpp"

namespace relmt {
namespace aggregations {
// ----------------------------------------------------------------------------

void Sum::calc_all(const enums::Revert _revert,
                   const std::vector<containers::Match>::iterator _begin,
                   const std::vector<containers::Match>::iterator _split_begin,
                   const std::vector<containers::Match>::iterator _split_end,
                   const std::vector<containers::Match>::iterator _end) {
  // ------------------------------------------------------------------------

  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == nrows() * ncolsplus1);

  assert_true(indices_.size() == 0);
  assert_true(indices_current_.size() == 0);

  update_ = enums::Update::calc_all;

  // ------------------------------------------------------------------------
  // All matches between _split_begin and _split_end are allocated to
  // _eta1. All others are allocated to eta2_.

  num_samples_1_ = 0.0;

  num_samples_2_ = 0.0;

  for (auto it = _begin; it != _split_begin; ++it) {
    assert_true(it->ix_output < nrows());

    impl_.update_eta(it->ix_input, it->ix_output, 1.0,
                     eta2_.data() + it->ix_output * ncolsplus1);

    ++num_samples_2_;

    indices_.insert(it->ix_output);
    indices_current_.insert(it->ix_output);
  }

  for (auto it = _split_begin; it != _split_end; ++it) {
    assert_true(it->ix_output < nrows());

    impl_.update_eta(it->ix_input, it->ix_output, 1.0,
                     eta1_.data() + it->ix_output * ncolsplus1);

    ++num_samples_1_;

    indices_.insert(it->ix_output);
    indices_current_.insert(it->ix_output);
  }

  for (auto it = _split_end; it != _end; ++it) {
    assert_true(it->ix_output < nrows());

    impl_.update_eta(it->ix_input, it->ix_output, 1.0,
                     eta2_.data() + it->ix_output * ncolsplus1);

    ++num_samples_2_;

    indices_.insert(it->ix_output);
    indices_current_.insert(it->ix_output);
  }

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_diff(const enums::Revert _revert,
                    const std::vector<containers::Match>::iterator _split_begin,
                    const std::vector<containers::Match>::iterator _split_end) {
  // ------------------------------------------------------------------------

  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == nrows() * ncolsplus1);

  assert_true(_split_end >= _split_begin);

  // ------------------------------------------------------------------------
  // Incremental updates imply that we move samples from eta2_ to eta1_.

  for (auto it = _split_begin; it != _split_end; ++it) {
    assert_true(it->ix_output < nrows());

    impl_.update_etas(it->ix_input, it->ix_output, 1.0,
                      eta1_.data() + it->ix_output * ncolsplus1,
                      eta2_.data() + it->ix_output * ncolsplus1);

    indices_current_.insert(it->ix_output);

    assert_true(eta2_[it->ix_output * ncolsplus1] >= 0.0);
  }

  // ------------------------------------------------------------------------

  const auto dist = static_cast<Float>(std::distance(_split_begin, _split_end));

  num_samples_1_ += dist;
  num_samples_2_ -= dist;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_etas(const enums::Aggregation _agg, const enums::Update _update,
                    const std::vector<Float>& _old_weights,
                    const std::vector<size_t>& _indices_current,
                    const std::vector<Float>& _eta1,
                    const std::vector<Float>& _eta1_old,
                    const std::vector<Float>& _eta2,
                    const std::vector<Float>& _eta2_old) {
  const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
      false, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old);

  child_->calc_etas(_agg, _update, _old_weights,
                    intermediate_agg().indices_current(), *eta1, *eta1_old,
                    *eta2, *eta2_old);

  intermediate_agg().update_etas_old(_agg);
}

// ----------------------------------------------------------------------------

std::pair<Float, containers::Weights> Sum::calc_pair(
    const enums::Revert _revert, const enums::Update _update,
    const Float _min_num_samples, const Float _old_intercept,
    const std::vector<Float>& _old_weights,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end) {
  // -------------------------------------------------------------

  assert_true(eta1_.size() == eta2_.size());

  // -------------------------------------------------------------

  switch (_update) {
    case enums::Update::calc_one:
    case enums::Update::calc_all:
      calc_all(_revert, _begin, _split_begin, _split_end, _end);
      break;

    case enums::Update::calc_diff:
      calc_diff(_revert, _split_begin, _split_end);
      break;

    default:
      assert_true(false && "Unknown Update!");
  }

  // -------------------------------------------------------------

  // TODO
  /*if ( !impl_.is_balanced(
           num_samples_1_, num_samples_2_, _min_num_samples, comm_ ) )
      {
          return results;
      }*/

  // -------------------------------------------------------------

  const auto result = child_->calc_pair(
      enums::Aggregation::sum, _revert, update_, _old_weights,
      indices_.unique_integers(), indices_current_.unique_integers(), eta1_,
      eta1_old_, eta2_, eta2_old_);

  update_etas_old();

  update_ = enums::Update::calc_diff;

  // -------------------------------------------------------------

  if (_revert == enums::Revert::False) {
    indices_current_.clear();
  }

  // -------------------------------------------------------------

  return result;

  // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Sum::calc_yhat(const enums::Aggregation _agg,
                    const std::vector<Float>& _old_weights,
                    const containers::Weights& _new_weights,
                    const std::vector<size_t>& _indices,
                    const std::vector<Float>& _eta1,
                    const std::vector<Float>& _eta1_old,
                    const std::vector<Float>& _eta2,
                    const std::vector<Float>& _eta2_old) {
  assert_true(!std::isnan(std::get<0>(_new_weights)));

  const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
      false, _agg, _indices, _eta1, _eta1_old, _eta2, _eta2_old);

  child_->calc_yhat(_agg, _old_weights, _new_weights,
                    intermediate_agg().indices(), *eta1, *eta1_old, *eta2,
                    *eta2_old);

  intermediate_agg().update_etas_old(_agg);
}

// ----------------------------------------------------------------------------

void Sum::commit(const Float _old_intercept,
                 const std::vector<Float>& _old_weights,
                 const containers::Weights& _weights) {
  assert_true(eta1_.size() == eta2_.size());

  impl_.commit(_weights);
};

// ----------------------------------------------------------------------------

Float Sum::evaluate_split(const Float _old_intercept,
                          const std::vector<Float>& _old_weights,
                          const containers::Weights& _weights,
                          const std::vector<containers::Match>::iterator _begin,
                          const std::vector<containers::Match>::iterator _split,
                          const std::vector<containers::Match>::iterator _end) {
  calc_all(enums::Revert::False, _begin, _begin, _split, _end);

  calc_yhat(_old_weights, _weights);

  return child_->evaluate_split(_old_intercept, _old_weights, _weights);
}

// ----------------------------------------------------------------------------

void Sum::revert(const std::vector<Float>& _old_weights) {
  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == eta2_.size());

  for (auto ix : indices_current_) {
    for (size_t j = 0; j < ncolsplus1; ++j) {
      const auto ix2 = ix * ncolsplus1 + j;

      assert_true(ix2 < eta1_.size());

      eta2_[ix2] += eta1_[ix2];
      eta1_[ix2] = 0.0;
    }
  }

  child_->calc_etas(enums::Aggregation::sum, update_, _old_weights,
                    indices_current_.unique_integers(), eta1_, eta1_old_, eta2_,
                    eta2_old_);

  update_etas_old();

  update_ = enums::Update::calc_diff;

  num_samples_2_ += num_samples_1_;

  num_samples_1_ = 0.0;

  indices_current_.clear();
}

// ----------------------------------------------------------------------------

Float Sum::transform(const std::vector<Float>& _weights) const {
  return std::accumulate(_weights.begin(), _weights.end(), 0.0);
}

// ----------------------------------------------------------------------------

void Sum::update_etas_old() {
  const auto ncolsplus1 = ncols() + 1;

  for (auto ix : indices_current_) {
    for (size_t j = 0; j < ncolsplus1; ++j) {
      const auto ix2 = ix * ncolsplus1 + j;

      eta1_old_[ix2] = eta1_[ix2];
      eta2_old_[ix2] = eta2_[ix2];
    }
  }
}

// ----------------------------------------------------------------------------
}  // namespace aggregations
}  // namespace relmt

#include "relmt/aggregations/Avg.hpp"

namespace relmt {
namespace aggregations {
// ----------------------------------------------------------------------------

void Avg::calc_all(const enums::Revert _revert,
                   const std::vector<Float>& _old_weights,
                   const std::vector<containers::Match>::iterator _begin,
                   const std::vector<containers::Match>::iterator _split_begin,
                   const std::vector<containers::Match>::iterator _split_end,
                   const std::vector<containers::Match>::iterator _end) {
  // ------------------------------------------------------------------------

  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == nrows() * ncolsplus1);
  assert_true(nrows() == count_committed_.size());

  assert_true(indices_.size() == 0);
  assert_true(indices_current_.size() == 0);

  update_ = enums::Update::calc_all;

  // ------------------------------------------------------------------------

#ifndef NDEBUG

  for (auto val : count1_) {
    assert_true(val == 0.0);
  }

  for (auto val : count2_) {
    assert_true(val == 0.0);
  }

#endif  // NDEBUG

  // ------------------------------------------------------------------------

  num_samples_1_ = 0.0;

  num_samples_2_ = 0.0;

  // ------------------------------------------------------------------------
  // Calculate eta1_, eta2_, eta_old_, count1_ and count2_.

  for (auto it = _begin; it != _split_begin; ++it) {
    const auto ix = it->ix_output;

    assert_true(ix < count_committed_.size());
    assert_true(count_committed_[ix] > 0.0);

    impl_.update_eta(it->ix_input, ix, count_committed_[ix],
                     eta2_.data() + ix * ncolsplus1);

    ++count2_[ix];

    ++num_samples_2_;

    indices_.insert(ix);
    indices_current_.insert(ix);
  }

  for (auto it = _split_begin; it != _split_end; ++it) {
    const auto ix = it->ix_output;

    assert_true(ix < count_committed_.size());
    assert_true(count_committed_[ix] > 0.0);

    impl_.update_eta(it->ix_input, ix, count_committed_[ix],
                     eta1_.data() + ix * ncolsplus1);

    ++count1_[ix];

    ++num_samples_1_;

    indices_.insert(ix);
    indices_current_.insert(ix);
  }

  for (auto it = _split_end; it != _end; ++it) {
    const auto ix = it->ix_output;

    assert_true(ix < count_committed_.size());
    assert_true(count_committed_[ix] > 0.0);

    impl_.update_eta(it->ix_input, ix, count_committed_[ix],
                     eta2_.data() + ix * ncolsplus1);

    ++count2_[ix];

    ++num_samples_2_;

    indices_.insert(ix);
    indices_current_.insert(ix);
  }

  /*for ( auto ix : indices_ )
      {
          eta_old_[ix] = count1_[ix] + count2_[ix];
      }*/

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_diff(const std::vector<Float>& _old_weights,
                    const std::vector<containers::Match>::iterator _split_begin,
                    const std::vector<containers::Match>::iterator _split_end) {
  // ------------------------------------------------------------------------

  const auto ncolsplus1 = ncols() + 1;

  assert_true(eta1_.size() == eta2_.size());
  assert_true(eta1_.size() == nrows() * ncolsplus1);
  assert_true(nrows() == count_committed_.size());

  assert_true(_split_end >= _split_begin);

  // ------------------------------------------------------------------------

  for (auto it = _split_begin; it != _split_end; ++it) {
    const auto ix = it->ix_output;

    assert_true(ix < eta1_.size());
    assert_true(count_committed_[ix] > 0.0);

    impl_.update_etas(it->ix_input, it->ix_output, count_committed_[ix],
                      eta1_.data() + it->ix_output * ncolsplus1,
                      eta2_.data() + it->ix_output * ncolsplus1);

    ++count1_[ix];
    --count2_[ix];

    assert_true(count2_[ix] >= 0.0);

    indices_current_.insert(ix);
  }

  // ------------------------------------------------------------------------

  const auto dist = static_cast<Float>(std::distance(_split_begin, _split_end));

  num_samples_1_ += dist;
  num_samples_2_ -= dist;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::calc_etas(const enums::Aggregation _agg, const enums::Update _update,
                    const std::vector<Float>& _old_weights,
                    const std::vector<size_t>& _indices_current,
                    const std::vector<Float>& _eta1,
                    const std::vector<Float>& _eta1_old,
                    const std::vector<Float>& _eta2,
                    const std::vector<Float>& _eta2_old) {
  const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
      true, _agg, _indices_current, _eta1, _eta1_old, _eta2, _eta2_old);

  child_->calc_etas(_agg, _update, _old_weights,
                    intermediate_agg().indices_current(), *eta1, *eta1_old,
                    *eta2, *eta2_old);

  intermediate_agg().update_etas_old(_agg);
}

// ----------------------------------------------------------------------------

std::pair<Float, containers::Weights> Avg::calc_pair(
    const enums::Revert _revert, const enums::Update _update,
    const Float _min_num_samples, const Float _old_intercept,
    const std::vector<Float>& _old_weights,
    const std::vector<containers::Match>::iterator _begin,
    const std::vector<containers::Match>::iterator _split_begin,
    const std::vector<containers::Match>::iterator _split_end,
    const std::vector<containers::Match>::iterator _end) {
  // -------------------------------------------------------------

  debug_log("std::distance(_begin, _split_begin): " +
            std::to_string(std::distance(_begin, _split_begin)));

  debug_log("std::distance(_split_begin, _split_end): " +
            std::to_string(std::distance(_split_begin, _split_end)));

  debug_log("std::distance(_split_begin, _split_end): " +
            std::to_string(std::distance(_split_end, _end)));

  // -------------------------------------------------------------

  switch (_update) {
    case enums::Update::calc_one:
    case enums::Update::calc_all:
      update_ = enums::Update::calc_all;
      calc_all(_revert, _old_weights, _begin, _split_begin, _split_end, _end);
      break;

    case enums::Update::calc_diff:
      calc_diff(_old_weights, _split_begin, _split_end);
      break;

    default:
      assert_true(false && "Unknown Update!");
  }

  // -------------------------------------------------------------

  // TODO
  /*
      if ( !impl_.is_balanced(
               num_samples_1_, num_samples_2_, _min_num_samples, comm_ ) )
          {
              return results;
          }*/

  // -------------------------------------------------------------

  const auto result = child_->calc_pair(
      enums::Aggregation::avg, _revert, update_, _old_weights,
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

void Avg::calc_yhat(const std::vector<Float>& _old_weights,
                    const containers::Weights& _new_weights) {
  assert_true(!std::isnan(std::get<0>(_new_weights)));

  child_->calc_yhat(enums::Aggregation::avg, _old_weights, _new_weights,
                    indices_.unique_integers(), eta1_, eta1_old_, eta2_,
                    eta2_old_);
}

// ----------------------------------------------------------------------------

void Avg::calc_yhat(const enums::Aggregation _agg,
                    const std::vector<Float>& _old_weights,
                    const containers::Weights& _new_weights,
                    const std::vector<size_t>& _indices,
                    const std::vector<Float>& _eta1,
                    const std::vector<Float>& _eta1_old,
                    const std::vector<Float>& _eta2,
                    const std::vector<Float>& _eta2_old) {
  assert_true(!std::isnan(std::get<0>(_new_weights)));

  const auto [eta1, eta1_old, eta2, eta2_old] = intermediate_agg().calc_etas(
      true, _agg, _indices, _eta1, _eta1_old, _eta2, _eta2_old);

  child_->calc_yhat(_agg, _old_weights, _new_weights,
                    intermediate_agg().indices(), *eta1, *eta1_old, *eta2,
                    *eta2_old);

  intermediate_agg().update_etas_old(enums::Aggregation::avg);
}

// ----------------------------------------------------------------------------

void Avg::commit(const Float _old_intercept,
                 const std::vector<Float>& _old_weights,
                 const containers::Weights& _weights) {
  // ------------------------------------------------------------------------

  assert_true(eta1_.size() == eta2_.size());

  // TODO
  /*for ( auto ix : indices_ )
      {
          w_fixed_committed_[ix] += eta1_[ix] * std::get<1>( _weights ) +
                                    eta2_[ix] * std::get<2>( _weights ) -
                                    ( eta1_[ix] + eta2_[ix] ) * _old_weight;
      }*/

  // ------------------------------------------------------------------------

  for (auto ix : indices_) {
    assert_true(ix < count1_.size());

    count1_[ix] = 0.0;
    count2_[ix] = 0.0;
  }

  // ------------------------------------------------------------------------

#ifndef NDEBUG

  for (auto val : count1_) {
    assert_true(val == 0.0);
  }

  for (auto val : count2_) {
    assert_true(val == 0.0);
  }

#endif  // NDEBUG

  // ------------------------------------------------------------------------

  impl_.commit(_weights);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float Avg::evaluate_split(const Float _old_intercept,
                          const std::vector<Float>& _old_weights,
                          const containers::Weights& _weights,
                          const std::vector<containers::Match>::iterator _begin,
                          const std::vector<containers::Match>::iterator _split,
                          const std::vector<containers::Match>::iterator _end) {
  calc_all(enums::Revert::False, _old_weights, _begin, _begin, _split, _end);

  calc_yhat(_old_weights, _weights);

  return child_->evaluate_split(_old_intercept, _old_weights, _weights);
}

// ----------------------------------------------------------------------------

void Avg::init_count_committed(const std::vector<containers::Match>& _matches) {
  assert_true(count_committed_.size() == nrows());

  for (auto m : _matches) {
    assert_true(m.ix_output < count_committed_.size());

    ++count_committed_[m.ix_output];
  }
}

// ----------------------------------------------------------------------------

void Avg::resize(const size_t _nrows, const size_t _ncols) {
  count_committed_ = std::vector<Float>(_nrows);

  count1_ = std::vector<Float>(_nrows);
  count2_ = std::vector<Float>(_nrows);

  eta_old_ = std::vector<Float>(_nrows * (_ncols + 1));

  indices_current_ = containers::IntSet(_nrows);

  impl_.resize(_nrows, _ncols);
}

// ----------------------------------------------------------------------------

void Avg::revert(const std::vector<Float>& _old_weights) {
  // -------------------------------------------------------------

  const auto ncolsplus1 = ncols() + 1;

  // -------------------------------------------------------------

  assert_true(eta1_.size() == eta2_.size());
  assert_true(count1_.size() == count2_.size());

  // -------------------------------------------------------------

  for (auto ix : indices_current_) {
    for (size_t j = 0; j < ncolsplus1; ++j) {
      const auto ix2 = ix * ncolsplus1 + j;

      assert_true(ix2 < eta1_.size());

      eta2_[ix2] += eta1_[ix2];
      eta1_[ix2] = 0.0;
    }
  }

  // -------------------------------------------------------------

  for (auto ix : indices_current_) {
    assert_true(ix < count1_.size());

    count2_[ix] += count1_[ix];
    count1_[ix] = 0.0;
  }

  // -------------------------------------------------------------

  child_->calc_etas(enums::Aggregation::avg, update_, _old_weights,
                    indices_current_.unique_integers(), eta1_, eta1_old_, eta2_,
                    eta2_old_);

  update_etas_old();

  update_ = enums::Update::calc_diff;

  // -------------------------------------------------------------

  num_samples_2_ += num_samples_1_;

  num_samples_1_ = 0.0;

  // -------------------------------------------------------------

  indices_current_.clear();

  // -------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void Avg::revert_to_commit() {
  // ------------------------------------------------------------------------

  assert_true(count1_.size() == count2_.size());

  // ------------------------------------------------------------------------

  for (auto ix : indices_) {
    assert_true(ix < count1_.size());
    count1_[ix] = 0.0;
    count2_[ix] = 0.0;
  }

  // ------------------------------------------------------------------------

  impl_.revert_to_commit();

  assert_true(indices_.size() == 0);

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Float Avg::transform(const std::vector<Float>& _weights) const {
  Float count = 0.0;

  for (auto weight : _weights) {
    if (!std::isnan(weight)) {
      count += 1.0;
    }
  }

  Float result = 0.0;

  for (auto weight : _weights) {
    if (!std::isnan(weight)) {
      result += weight / count;
    }
  }

  return result;
}
// ----------------------------------------------------------------------------

void Avg::update_etas_old() {
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

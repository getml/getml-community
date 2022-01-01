#include "multirel/decisiontrees/DecisionTreeNode.hpp"

namespace multirel {
namespace decisiontrees {
// ----------------------------------------------------------------------------

DecisionTreeNode::DecisionTreeNode(bool _is_activated, Int _depth,
                                   const DecisionTreeImpl *_tree)
    : depth_(_depth),
      improvement_(NAN),
      initial_value_(NAN),
      is_activated_(_is_activated),
      tree_(_tree){};

// ----------------------------------------------------------------------------

void DecisionTreeNode::add_subfeatures(
    std::set<size_t> *_subfeatures_used) const {
  if (!split_) {
    return;
  }

  if (split_->data_used == enums::DataUsed::x_subfeature) {
    _subfeatures_used->insert(split_->column_used);
  }

  if (child_node_greater_) {
    assert_true(child_node_smaller_);
    child_node_greater_->add_subfeatures(_subfeatures_used);
    child_node_smaller_->add_subfeatures(_subfeatures_used);
  }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Int>> DecisionTreeNode::calculate_categories(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  // ------------------------------------------------------------------------

  Int categories_begin = 0;
  Int categories_end = 0;

  // ------------------------------------------------------------------------
  // In distributed versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they will not be the
  // chosen minimum or maximum.

  if (std::distance(_match_container_begin, _match_container_end) > 0) {
    categories_begin =
        std::max((*_match_container_begin)->categorical_value, 0);
    categories_end =
        std::max((*(_match_container_end - 1))->categorical_value + 1, 0);
  } else {
    categories_begin = std::numeric_limits<Int>::max();
    categories_end = 0;
  }

  utils::Reducer::reduce(multithreading::minimum<Int>(), &categories_begin,
                         comm());

  utils::Reducer::reduce(multithreading::maximum<Int>(), &categories_end,
                         comm());

  // ------------------------------------------------------------------------
  // There is a possibility that all critical values are NULL (signified by
  // -1) in all processes. This accounts for this edge case.

  if (categories_begin >= categories_end) {
    return std::make_shared<std::vector<Int>>(0);
  }

  // ------------------------------------------------------------------------
  // Find unique categories (signified by a boolean vector). We cannot use the
  // actual boolean type, because bool is smaller than char and therefore the
  // all_reduce operator won't work. So we use std::int8_t instead.

  auto included =
      std::vector<std::int8_t>(categories_end - categories_begin, 0);

  for (auto it = _match_container_begin; it != _match_container_end; ++it) {
    if ((*it)->categorical_value < 0) {
      continue;
    }

    assert_true((*it)->categorical_value >= categories_begin);
    assert_true((*it)->categorical_value < categories_end);

    included[(*it)->categorical_value - categories_begin] = 1;
  }

  utils::Reducer::reduce(multithreading::maximum<std::int8_t>(), &included,
                         comm());

  // ------------------------------------------------------------------------
  // Build vector.

  auto categories = std::make_shared<std::vector<Int>>(0);

  for (Int i = 0; i < categories_end - categories_begin; ++i) {
    if (included[i] == 1) {
      categories->push_back(categories_begin + i);
    }
  }

  // ------------------------------------------------------------------------

  return categories;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_discrete(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  // ---------------------------------------------------------------------------

  Float min = 0.0, max = 0.0;

  // ---------------------------------------------------------------------------
  // In distributed versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  if (std::distance(_match_container_begin, _match_container_end) > 0) {
    min = std::floor((*_match_container_begin)->numerical_value);
    max = std::ceil((*(_match_container_end - 1))->numerical_value);
  } else {
    min = std::numeric_limits<Float>::max();
    max = std::numeric_limits<Float>::lowest();
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), &min, comm());

  utils::Reducer::reduce(multithreading::maximum<Float>(), &max, comm());

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0.0, 1);
  }

  // ---------------------------------------------------------------------------
  // If the number of critical values is too large, then use the numerical
  // algorithm instead.

  const auto num_critical_values = static_cast<Int>(max - min + 1);

  const auto num_critical_values_numerical =
      calculate_num_critical_values(_sample_size);

  if (num_critical_values_numerical < num_critical_values) {
    auto critical_values = calculate_critical_values_numerical(
        num_critical_values_numerical, min, max);

    for (auto &c : critical_values) {
      c = std::floor(c);
    }

    return critical_values;
  }

  // ---------------------------------------------------------------------------

  std::vector<Float> critical_values(num_critical_values, 1);

  for (Int i = 0; i < num_critical_values; ++i) {
    critical_values[i] = min + static_cast<Float>(i);
  }

  // ---------------------------------------------------------------------------

  return critical_values;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_numerical(
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  // ---------------------------------------------------------------------------

  Float min = 0.0, max = 0.0;

  // ---------------------------------------------------------------------------
  // In distributed versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  if (std::distance(_match_container_begin, _match_container_end) > 0) {
    min = (*_match_container_begin)->numerical_value;
    max = (*(_match_container_end - 1))->numerical_value;
  } else {
    min = std::numeric_limits<Float>::max();
    max = std::numeric_limits<Float>::lowest();
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), &min, comm());

  utils::Reducer::reduce(multithreading::maximum<Float>(), &max, comm());

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0);
  }

  // ---------------------------------------------------------------------------

  Int num_critical_values = calculate_num_critical_values(_sample_size);

  return calculate_critical_values_numerical(num_critical_values, min, max);

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_numerical(
    const Int _num_critical_values, const Float _min, const Float _max) {
  Float step_size =
      (_max - _min) / static_cast<Float>(_num_critical_values + 1);

  std::vector<Float> critical_values(_num_critical_values);

  for (Int i = 0; i < _num_critical_values; ++i) {
    critical_values[i] = _min + static_cast<Float>(i + 1) * step_size;
  }

  return critical_values;
}

// ----------------------------------------------------------------------------

std::vector<Float> DecisionTreeNode::calculate_critical_values_window(
    const Float _delta_t,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  // ---------------------------------------------------------------------------

  Float min = 0.0, max = 0.0;

  // ---------------------------------------------------------------------------
  // In distributed versions, it is possible that there are no sample sizes
  // left in this process rank. In that case we effectively pass plus infinity
  // to min and minus infinity to max, ensuring that they not will be the
  // chosen minimum or maximum.

  if (std::distance(_match_container_begin, _match_container_end) > 0) {
    min = (*_match_container_begin)->numerical_value;
    max = (*(_match_container_end - 1))->numerical_value;
  } else {
    min = std::numeric_limits<Float>::max();
    max = std::numeric_limits<Float>::lowest();
  }

  utils::Reducer::reduce(multithreading::minimum<Float>(), &min, comm());

  utils::Reducer::reduce(multithreading::maximum<Float>(), &max, comm());

  // ---------------------------------------------------------------------------
  // There is a possibility that all critical values are NAN in all processes.
  // This accounts for this edge case.

  if (min > max) {
    return std::vector<Float>(0, 1);
  }

  // ---------------------------------------------------------------------------
  // The input value for delta_t could be stupid...we want to avoid memory
  // overflow.

  assert_true(_delta_t > 0.0);

  const auto num_critical_values =
      static_cast<size_t>((max - min) / _delta_t) + 1;

  if (num_critical_values > 100000) {
    return std::vector<Float>(0, 1);
  }

  // ---------------------------------------------------------------------------

  std::vector<Float> critical_values(num_critical_values);

  for (size_t i = 0; i < num_critical_values; ++i) {
    critical_values[i] = min + static_cast<Float>(i + 1) * _delta_t;
  }

  return critical_values;

  // ---------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::column_importances(
    utils::ImportanceMaker *_importance_maker) const {
  if (!std::isnan(initial_value_)) {
    _importance_maker->add(tree_->input(), tree_->output(),
                           tree_->column_to_be_aggregated_.data_used,
                           tree_->column_to_be_aggregated_.ix_column_used,
                           tree_->same_units_, initial_value_);
  }

  if (!std::isnan(improvement_)) {
    assert_true(split_);

    _importance_maker->add(tree_->input(), tree_->output(), split_->data_used,
                           split_->column_used, tree_->same_units_,
                           improvement_);
  }

  if (child_node_greater_) {
    assert_true(child_node_smaller_);

    child_node_greater_->column_importances(_importance_maker);

    child_node_smaller_->column_importances(_importance_maker);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::commit(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _separator,
    containers::MatchPtrs::iterator _match_container_end) {
  const auto old_value = optimization_criterion()->value();

  update(true, _match_container_begin, _separator, _match_container_end,
         aggregation());

  aggregation()->commit();

  optimization_criterion()->commit();

  improvement_ = optimization_criterion()->value() - old_value;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  const size_t sample_size = reduce_sample_size(
      std::distance(_match_container_begin, _match_container_end));

  if (sample_size == 0 || sample_size < tree_->min_num_samples() * 2) {
    return;
  }

  optimization_criterion()->reset_storage_size();

  // ------------------------------------------------------------------------
  // Try imposing different conditions and measure the performance.

  std::vector<descriptors::Split> candidate_splits = {};

  try_conditions(_population, _peripheral, _subfeatures, sample_size,
                 _match_container_begin, _match_container_end,
                 &candidate_splits);

  // ------------------------------------------------------------------------

  Int ix_max = optimization_criterion()->find_maximum();

  const Float max_value = optimization_criterion()->values_stored(ix_max);

  // ------------------------------------------------------------------------
  // DEBUG and parallel mode only: Make sure that the values_stored are
  // aligned!

#ifndef NDEBUG

  auto global_storage_ix = optimization_criterion()->storage_ix();

  utils::Reducer::reduce<size_t>(multithreading::maximum<size_t>(),
                                 &global_storage_ix, comm());

  assert_true(global_storage_ix == optimization_criterion()->storage_ix());

  auto global_value = optimization_criterion()->value();

  utils::Reducer::reduce<Float>(multithreading::maximum<Float>(), &global_value,
                                comm());

  assert_true(global_value == optimization_criterion()->value());

  auto global_max_value = max_value;

  utils::Reducer::reduce<Float>(multithreading::maximum<Float>(),
                                &global_max_value, comm());

  assert_true(global_max_value == max_value);

#endif  // NDEBUG

  // ------------------------------------------------------------------------
  // Imposing a condition is only necessary, if it actually improves the
  // optimization criterion

  if (max_value >
      optimization_criterion()->value() + tree_->regularization() + 1e-07) {
    assert_true(ix_max < candidate_splits.size());

    split_.reset(new descriptors::Split(candidate_splits[ix_max].deep_copy()));

    set_samples(_population, _peripheral, _subfeatures, _match_container_begin,
                _match_container_end);

    const auto separator = partition(
        _population, _peripheral, _match_container_begin, _match_container_end);

    commit(_match_container_begin, separator, _match_container_end);

    if (depth_ < tree_->max_length()) {
      spawn_child_nodes(_population, _peripheral, _subfeatures,
                        _match_container_begin, separator,
                        _match_container_end);
    }
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::fit_as_root(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  aggregation()->activate_all(true, _match_container_begin,
                              _match_container_end);

  aggregation()->commit();

  optimization_criterion()->commit();

  initial_value_ = optimization_criterion()->value();

  if (tree_->max_length() > 0) {
    fit(_population, _peripheral, _subfeatures, _match_container_begin,
        _match_container_end);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::from_json_obj(const Poco::JSON::Object &_json_obj) {
  is_activated_ = JSON::get_value<bool>(_json_obj, "act_");

  const bool imposes_condition = JSON::get_value<bool>(_json_obj, "imp_");

  if (_json_obj.has("improvement_")) {
    improvement_ = JSON::get_value<Float>(_json_obj, "improvement_");
  }

  if (_json_obj.has("initial_value_")) {
    initial_value_ = JSON::get_value<Float>(_json_obj, "initial_value_");
  }

  if (imposes_condition) {
    split_.reset(new descriptors::Split(_json_obj));

    if (_json_obj.has("sub1_")) {
      if (!_json_obj.has("sub2_")) {
        std::invalid_argument(
            "Error in JSON: Has 'sub1_', but not "
            "'sub2_'!");
      }

      child_node_greater_.reset(new DecisionTreeNode(false, depth_ + 1, tree_));

      child_node_greater_->from_json_obj(*JSON::get_object(_json_obj, "sub1_"));

      child_node_smaller_.reset(new DecisionTreeNode(false, depth_ + 1, tree_));

      child_node_smaller_->from_json_obj(*JSON::get_object(_json_obj, "sub2_"));
    }
  }
}
// ----------------------------------------------------------------------------

bool DecisionTreeNode::is_ts_numerical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral, const size_t _col) const {
  assert_true(_col < tree_->same_units_numerical().size());

  const auto ix =
      std::get<0>(tree_->same_units_numerical().at(_col)).ix_column_used;

  std::string unit;

  switch (std::get<0>(tree_->same_units_numerical().at(_col)).data_used) {
    case enums::DataUsed::x_perip_numerical:
      unit = _peripheral.numerical_unit(ix);
      break;

    case enums::DataUsed::x_popul_numerical:
      unit = _population.numerical_unit(ix);
      break;

    default:
      assert_true(!"is_ts_numerical: enums::DataUsed not known!");
      break;
  }

  const auto is_ts = unit.find("time stamp") != std::string::npos &&
                     unit.find("$GETML_ROWID") == std::string::npos;

  return is_ts;
}

// ----------------------------------------------------------------------------

std::pair<containers::CategoryIndex, std::shared_ptr<const std::vector<Int>>>
DecisionTreeNode::make_index_and_categories(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) {
  const auto get_value = [](const containers::Match *const m) {
    return m->categorical_value;
  };

  const auto is_not_null = [](const containers::Match *m) {
    return m->categorical_value >= 0;
  };

  const auto nan_begin =
      std::partition(_match_container_begin, _match_container_end, is_not_null);

  const auto [min, max] =
      utils::MinMaxFinder<decltype(get_value), Int>::find_min_max(
          get_value, _match_container_begin, nan_begin, comm());

  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  // Note that this bins in ASCENDING order.
  auto [indptr, categories] =
      utils::CategoricalBinner<decltype(get_value)>::bin(
          min, max, get_value, _match_container_begin, nan_begin,
          _match_container_end, &bins, comm());

  auto index =
      containers::CategoryIndex(std::move(bins), std::move(indptr), min);

  return std::make_pair(index, categories);
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  if (categorical_data_used()) {
    return partition_by_categories_used(_match_container_begin,
                                        _match_container_end);
  }

  if (lag_used()) {
    return partition_by_lag(_match_container_begin, _match_container_end);
  }

  if (text_used()) {
    return partition_by_text(_population, _peripheral, _match_container_begin,
                             _match_container_end);
  }

  return partition_by_critical_value(_match_container_begin,
                                     _match_container_end);
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_categories_used(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  const auto is_contained = [this](const containers::Match *_sample) {
    return std::any_of(
        categories_used_begin(), categories_used_end(),
        [_sample](Int cat) { return cat == _sample->categorical_value; });
  };

  return std::partition(_match_container_begin, _match_container_end,
                        is_contained);
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_critical_value(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  // ---------------------------------------------------------

  const bool null_values_to_beginning = (apply_from_above() != is_activated_);

  const auto null_values_separator = separate_null_values(
      _match_container_begin, _match_container_end, null_values_to_beginning);

  const auto smaller_equal = [this](const containers::Match *_sample) {
    return _sample->numerical_value <= critical_value();
  };

  // ---------------------------------------------------------

  if (null_values_to_beginning) {
    return std::partition(null_values_separator, _match_container_end,
                          smaller_equal);
  } else {
    return std::partition(_match_container_begin, null_values_separator,
                          smaller_equal);
  }

  // ---------------------------------------------------------
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_lag(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  return std::partition(
      _match_container_begin, _match_container_end,
      [this](const containers::Match *_sample) {
        return (_sample->numerical_value <= critical_value() &&
                _sample->numerical_value > critical_value() - tree_->delta_t());
      });
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::partition_by_text(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  assert_true(split_);

  const auto &split = *split_;

  const auto &df = (split.data_used == enums::DataUsed::x_popul_text)
                       ? _population.df()
                       : _peripheral;

  const auto col = split.column_used;

  assert_true(col < df.word_indices_.size());
  assert_true(df.word_indices_.at(col));

  const auto &word_index = *df.word_indices_.at(col);

  const auto word_is_contained = [&split,
                                  &word_index](const containers::Match *m) {
    assert_true(m->categorical_value >= 0);

    const auto range =
        word_index.range(static_cast<size_t>(m->categorical_value));

    const auto in_range = [&range](const Int word_ix) -> bool {
      for (const auto &val : range) {
        if (val == word_ix) {
          return true;
        }
        if (val > word_ix) {
          return false;
        }
      }
      return false;
    };

    const auto it = std::find_if(split.categories_used_begin,
                                 split.categories_used_end, in_range);

    return it != split.categories_used_end;
  };

  return std::partition(_match_container_begin, _match_container_end,
                        word_is_contained);
}

// ----------------------------------------------------------------------------

size_t DecisionTreeNode::reduce_sample_size(const size_t _sample_size) {
  size_t global_sample_size = _sample_size;

  utils::Reducer::reduce(std::plus<size_t>(), &global_sample_size, comm());

  return global_sample_size;
}

// ----------------------------------------------------------------------------

containers::MatchPtrs::iterator DecisionTreeNode::separate_null_values(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    bool _null_values_to_beginning) const {
  auto is_null = [](containers::Match *sample) {
    return (std::isnan(sample->numerical_value) ||
            std::isinf(sample->numerical_value));
  };

  auto is_not_null = [](containers::Match *sample) {
    return (!std::isnan(sample->numerical_value) &&
            !std::isinf(sample->numerical_value));
  };

  if (_null_values_to_beginning) {
    return std::partition(_match_container_begin, _match_container_end,
                          is_null);
  } else {
    return std::partition(_match_container_begin, _match_container_end,
                          is_not_null);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::set_samples(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end) const {
  switch (data_used()) {
    case enums::DataUsed::same_unit_categorical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->categorical_value = get_same_unit_categorical(
            _population, _peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::same_unit_discrete_ts:
    case enums::DataUsed::same_unit_discrete:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value = get_same_unit_discrete(
            _population, _peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::same_unit_numerical_ts:
    case enums::DataUsed::same_unit_numerical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value = get_same_unit_numerical(
            _population, _peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::x_perip_categorical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->categorical_value =
            get_x_perip_categorical(_peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::x_perip_numerical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_x_perip_numerical(_peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::x_perip_discrete:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_x_perip_discrete(_peripheral, *it, column_used());
      }

      break;

    case enums::DataUsed::x_perip_text:
      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->categorical_value = static_cast<Int>((*it)->ix_x_perip);
      }

      break;

    case enums::DataUsed::x_popul_categorical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->categorical_value =
            get_x_popul_categorical(_population, *it, column_used());
      }

      break;

    case enums::DataUsed::x_popul_numerical:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_x_popul_numerical(_population, *it, column_used());
      }

      break;

    case enums::DataUsed::x_popul_discrete:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_x_popul_discrete(_population, *it, column_used());
      }

      break;

    case enums::DataUsed::x_popul_text:
      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        const auto ix = (*it)->ix_x_popul;

        assert_true(ix < _population.rows().size());

        (*it)->categorical_value = static_cast<Int>(_population.rows()[ix]);
      }

      break;

    case enums::DataUsed::x_subfeature:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_x_subfeature(_subfeatures, *it, column_used());
      }

      break;

    case enums::DataUsed::time_stamps_diff:
    case enums::DataUsed::time_stamps_window:

      for (auto it = _match_container_begin; it != _match_container_end; ++it) {
        (*it)->numerical_value =
            get_time_stamps_diff(_population, _peripheral, *it);
      }

      break;

    default:

      assert_true(false && "Unknown enums::DataUsed!");
  }
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<Int>> DecisionTreeNode::sort_categories(
    const std::shared_ptr<const std::vector<Int>> &_categories,
    const size_t _begin, const size_t _end) const {
  assert_true(_end >= _begin);

  const auto sorted = std::make_shared<std::vector<Int>>(_end - _begin);

  const auto indices = optimization_criterion()->argsort(_begin, _end);

  assert_true(indices.size() == _categories->size());

  for (size_t i = 0; i < indices.size(); ++i) {
    assert_true(indices[i] >= 0);
    assert_true(indices[i] < _end - _begin);

    (*sorted)[i] = (*_categories)[indices[i]];
  }

  return sorted;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::spawn_child_nodes(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _separator,
    containers::MatchPtrs::iterator _match_container_end) {
  // -------------------------------------------------------------------------

  const bool child_node_greater_is_activated =
      (apply_from_above() != is_activated_);

  // -------------------------------------------------------------------------

  child_node_smaller_.reset(new DecisionTreeNode(
      !(child_node_greater_is_activated),  // _is_activated
      depth_ + 1,                          // _depth
      tree_                                // _tree
      ));

  child_node_smaller_->fit(_population, _peripheral, _subfeatures,
                           _match_container_begin, _separator);

  // -------------------------------------------------------------------------

  child_node_greater_.reset(new DecisionTreeNode(
      child_node_greater_is_activated,  // _is_activated
      depth_ + 1,                       // _depth
      tree_                             // _tree
      ));

  child_node_greater_->fit(_population, _peripheral, _subfeatures, _separator,
                           _match_container_end);

  // -------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

Poco::JSON::Object DecisionTreeNode::to_json_obj() const {
  Poco::JSON::Object obj;

  obj.set("act_", is_activated_);

  obj.set("imp_", (split_ && true));

  if (!std::isnan(improvement_)) {
    obj.set("improvement_", improvement_);
  }

  if (!std::isnan(initial_value_)) {
    obj.set("initial_value_", initial_value_);
  }

  if (split_) {
    obj.set("app_", apply_from_above());

    obj.set("categories_used_", JSON::vector_to_array_ptr(categories_used()));

    obj.set("critical_value_", critical_value());

    obj.set("column_used_", column_used());

    obj.set("data_used_", JSON::data_used_to_int(data_used()));

    if (child_node_greater_) {
      obj.set("sub1_", child_node_greater_->to_json_obj());

      obj.set("sub2_", child_node_smaller_->to_json_obj());
    }
  }

  return obj;
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::to_sql(
    const helpers::StringIterator &_categories, const VocabForDf &_vocab_popul,
    const VocabForDf &_vocab_perip,
    const std::shared_ptr<const helpers::SQLDialectGenerator>
        &_sql_dialect_generator,
    const std::string &_feature_prefix, const std::string &_feature_num,
    std::vector<std::string> &_conditions, std::string _sql) const {
  if (split_) {
    const bool activate_greater = (apply_from_above() != is_activated_);

    const auto sql_maker =
        utils::SQLMaker(tree_->delta_t(), tree_->ix_perip_used(),
                        tree_->same_units_, _sql_dialect_generator);

    const auto prefix = (_sql == "") ? "" : " AND ";

    const auto sql_greater =
        _sql + prefix +
        sql_maker.condition_greater(
            _categories, _vocab_popul, _vocab_perip, _feature_prefix,
            tree_->input(), tree_->output(), *split_, !activate_greater);

    const auto sql_smaller =
        _sql + prefix +
        sql_maker.condition_smaller(_categories, _vocab_popul, _vocab_perip,
                                    _feature_prefix, tree_->input(),
                                    tree_->output(), *split_, activate_greater);

    if (child_node_greater_) {
      assert_true(child_node_smaller_);

      child_node_greater_->to_sql(_categories, _vocab_popul, _vocab_perip,
                                  _sql_dialect_generator, _feature_prefix,
                                  _feature_num, _conditions, sql_greater);

      child_node_smaller_->to_sql(_categories, _vocab_popul, _vocab_perip,
                                  _sql_dialect_generator, _feature_prefix,
                                  _feature_num, _conditions, sql_smaller);

      return;
    }

    if (activate_greater) {
      _conditions.push_back(sql_greater);
    } else {
      _conditions.push_back(sql_smaller);
    }

    return;
  }

  if (is_activated_ && _sql != "") {
    _conditions.push_back(_sql);
  }
}

// ----------------------------------------------------------------------------

bool DecisionTreeNode::transform(const containers::DataFrameView &_population,
                                 const containers::DataFrame &_peripheral,
                                 const containers::Subfeatures &_subfeatures,
                                 containers::Match *_match) const {
  // -----------------------------------------------------------

  if (!split_) {
    return is_activated_;
  }

  // -----------------------------------------------------------

  auto match_container = containers::MatchPtrs({_match});

  set_samples(_population, _peripheral, _subfeatures, match_container.begin(),
              match_container.end());

  // -----------------------------------------------------------

  const auto separator = partition(
      _population, _peripheral, match_container.begin(), match_container.end());

  const bool is_greater = (separator == match_container.begin());

  // -----------------------------------------------------------

  if (!child_node_greater_) {
    const bool greater_is_activated = (apply_from_above() != is_activated_);

    return (is_greater == greater_is_activated);
  }

  // -----------------------------------------------------------

  if (is_greater) {
    return child_node_greater_->transform(_population, _peripheral,
                                          _subfeatures, _match);
  }

  // -----------------------------------------------------------

  return child_node_smaller_->transform(_population, _peripheral, _subfeatures,
                                        _match);

  // -----------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_peripheral(
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _peripheral.num_categoricals(); ++col) {
    if (_peripheral.categorical_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->categorical_value = get_x_perip_categorical(_peripheral, *it, col);
    }

    try_categorical_values(col, enums::DataUsed::x_perip_categorical,
                           _sample_size, _match_container_begin,
                           _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_population(
    const containers::DataFrameView &_population, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _population.num_categoricals(); ++col) {
    if (_population.categorical_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->categorical_value = get_x_popul_categorical(_population, *it, col);
    }

    try_categorical_values(col, enums::DataUsed::x_popul_categorical,
                           _sample_size, _match_container_begin,
                           _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_peripheral(
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _peripheral.num_discretes(); ++col) {
    if (_peripheral.discrete_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value = get_x_perip_discrete(_peripheral, *it, col);
    }

    try_discrete_values(col, enums::DataUsed::x_perip_discrete, _sample_size,
                        _match_container_begin, _match_container_end,
                        _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_population(
    const containers::DataFrameView &_population, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _population.num_discretes(); ++col) {
    if (_population.discrete_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value = get_x_popul_discrete(_population, *it, col);
    }

    try_discrete_values(col, enums::DataUsed::x_popul_discrete, _sample_size,
                        _match_container_begin, _match_container_end,
                        _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_peripheral(
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _peripheral.num_numericals(); ++col) {
    if (_peripheral.numerical_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value = get_x_perip_numerical(_peripheral, *it, col);
    }

    try_numerical_values(col, enums::DataUsed::x_perip_numerical, _sample_size,
                         _match_container_begin, _match_container_end,
                         _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_population(
    const containers::DataFrameView &_population, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _population.num_numericals(); ++col) {
    if (_population.numerical_unit(col).find("comparison only") !=
        std::string::npos) {
      continue;
    }

    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value = get_x_popul_numerical(_population, *it, col);
    }

    try_numerical_values(col, enums::DataUsed::x_popul_numerical, _sample_size,
                         _match_container_begin, _match_container_end,
                         _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_values(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  const auto [index, categories] =
      make_index_and_categories(_match_container_begin, _match_container_end);

  if (index.size() == 0) {
    return;
  }

  assert_true(categories);

  // -----------------------------------------------------------------------

  try_categorical_values_individual(_column_used, _data_used, _sample_size,
                                    index, categories, _match_container_begin,
                                    _match_container_end, _candidate_splits);

  // -----------------------------------------------------------------------
  // If there are only two categories, trying combined categories does not
  // make any sense.

  if (categories->size() < 3 || !tree_->allow_sets()) {
    return;
  }

  // -----------------------------------------------------------------------

  try_categorical_values_combined(_column_used, _data_used, _sample_size, index,
                                  categories, _match_container_begin,
                                  _match_container_end, _candidate_splits);

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_values_combined(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size, const containers::CategoryIndex &_index,
    const std::shared_ptr<const std::vector<Int>> &_categories,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  const auto num_categories = _categories->size();

  const auto storage_ix = optimization_criterion()->storage_ix();

  const auto sorted_by_containing =
      sort_categories(_categories, storage_ix - num_categories * 2,
                      storage_ix - num_categories);

  const auto sorted_by_not_containing =
      sort_categories(_categories, storage_ix - num_categories, storage_ix);

  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits.

  for (auto cat = sorted_by_containing->cbegin();
       cat < sorted_by_containing->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        false,                           // _apply_from_above
        sorted_by_containing,            // _categories_used
        sorted_by_containing->cbegin(),  // _categories_used_begin
        cat + 1,                         // _categories_used_end
        _column_used,                    // _column_used
        _data_used                       // _data_used
        ));
  }

  for (auto cat = sorted_by_not_containing->cbegin();
       cat < sorted_by_not_containing->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        true,                                // _apply_from_above
        sorted_by_not_containing,            // _categories_used
        sorted_by_not_containing->cbegin(),  // _categories_used_begin
        cat + 1,                             // _categories_used_end
        _column_used,                        // _column_used
        _data_used                           // _data_used
        ));
  }

  // -----------------------------------------------------------------------

  if (std::distance(_match_container_begin, _match_container_end) == 0) {
    for (size_t i = 0; i < _categories->size() * 2; ++i) {
      optimization_criterion()->store_current_stage(0.0, 0.0);
    }

    return;
  }

  //-----------------------------------------------------------------
  // Try applying all aggregation to all samples that
  // contain a certain category.

  if (is_activated_) {
    aggregation()->deactivate_matches_containing_categories(
        sorted_by_containing->cbegin(), sorted_by_containing->cend(),
        aggregations::Revert::after_all_categories, _index);
  } else {
    aggregation()->activate_matches_containing_categories(
        sorted_by_containing->cbegin(), sorted_by_containing->cend(),
        aggregations::Revert::after_all_categories, _index);
  }

  // -------------------------------------------------------------------
  // Try applying all aggregation to all samples that DO NOT
  // contain a certain category

  if (is_activated_) {
    aggregation()->deactivate_matches_not_containing_categories(
        sorted_by_not_containing->cbegin(), sorted_by_not_containing->cend(),
        aggregations::Revert::after_all_categories, _index);
  } else {
    aggregation()->activate_matches_not_containing_categories(
        sorted_by_not_containing->cbegin(), sorted_by_not_containing->cend(),
        aggregations::Revert::after_all_categories, _index);
  }

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_categorical_values_individual(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size, const containers::CategoryIndex &_index,
    const std::shared_ptr<const std::vector<Int>> &_categories,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits
  // The samples where the category equals category_used_
  // are copied into samples_smaller. This makes sense, because
  // for the numerical values, samples_smaller contains all values
  // <= critical_value().
  // Because we first try the samples containing a category, that means
  // the change must be applied from below, so apply_from_above is
  // first false and then true.

  for (auto cat = _categories->cbegin(); cat < _categories->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        false,         // _apply_from_above
        _categories,   // _categories_used
        cat,           // _categories_used_begin
        cat + 1,       // _categories_used_end
        _column_used,  // _column_used
        _data_used     // _data_used
        ));
  }

  for (auto cat = _categories->cbegin(); cat < _categories->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        true,          // _apply_from_above
        _categories,   // _categories_used
        cat,           // _categories_used_begin
        cat + 1,       // _categories_used_end
        _column_used,  // _column_used
        _data_used     // _data_used
        ));
  }

  // -----------------------------------------------------------------------
  // It is possible that std::distance( _match_container_begin,
  // _match_container_end ) is zero, when we are using the distributed
  // version. In that case we want this process until this point, because
  // calculate_critical_values_numerical contains a barrier and we want to
  // avoid a deadlock.

  if (std::distance(_match_container_begin, _match_container_end) == 0) {
    for (size_t i = 0; i < _categories->size() * 2; ++i) {
      optimization_criterion()->store_current_stage(0.0, 0.0);
    }

    return;
  }

  // -----------------------------------------------------------------------
  // Try applying all aggregation to all samples that
  // contain a certain category.

  if (is_activated_) {
    aggregation()->deactivate_matches_containing_categories(
        _categories->cbegin(), _categories->cend(),
        aggregations::Revert::after_each_category, _index);
  } else {
    aggregation()->activate_matches_containing_categories(
        _categories->cbegin(), _categories->cend(),
        aggregations::Revert::after_each_category, _index);
  }

  // -----------------------------------------------------------------------
  // Try applying all aggregation to all samples that DO NOT
  // contain a certain category

  if (is_activated_) {
    aggregation()->deactivate_matches_not_containing_categories(
        _categories->cbegin(), _categories->cend(),
        aggregations::Revert::after_each_category, _index);
  } else {
    aggregation()->activate_matches_not_containing_categories(
        _categories->cbegin(), _categories->cend(),
        aggregations::Revert::after_each_category, _index);
  }

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_conditions(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  try_same_units_categorical(_population, _peripheral, _sample_size,
                             _match_container_begin, _match_container_end,
                             _candidate_splits);

  try_same_units_discrete(_population, _peripheral, _sample_size,
                          _match_container_begin, _match_container_end,
                          _candidate_splits);

  try_same_units_numerical(_population, _peripheral, _sample_size,
                           _match_container_begin, _match_container_end,
                           _candidate_splits);

  try_categorical_peripheral(_peripheral, _sample_size, _match_container_begin,
                             _match_container_end, _candidate_splits);

  try_discrete_peripheral(_peripheral, _sample_size, _match_container_begin,
                          _match_container_end, _candidate_splits);

  try_numerical_peripheral(_peripheral, _sample_size, _match_container_begin,
                           _match_container_end, _candidate_splits);

  try_text_peripheral(_peripheral, _sample_size, _match_container_begin,
                      _match_container_end, _candidate_splits);

  try_categorical_population(_population, _sample_size, _match_container_begin,
                             _match_container_end, _candidate_splits);

  try_discrete_population(_population, _sample_size, _match_container_begin,
                          _match_container_end, _candidate_splits);

  try_numerical_population(_population, _sample_size, _match_container_begin,
                           _match_container_end, _candidate_splits);

  try_text_population(_population, _sample_size, _match_container_begin,
                      _match_container_end, _candidate_splits);

  try_subfeatures(_subfeatures, _sample_size, _match_container_begin,
                  _match_container_end, _candidate_splits);

  try_time_stamps_window(_population, _peripheral, _sample_size,
                         _match_container_begin, _match_container_end,
                         _candidate_splits);
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_discrete_values(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  const auto nan_begin =
      separate_null_values(_match_container_begin, _match_container_end, false);

  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  const auto get_value = [](const containers::Match *const m) {
    return m->numerical_value;
  };

  const auto [min, max] =
      utils::MinMaxFinder<decltype(get_value), Float>::find_min_max(
          get_value, _match_container_begin, nan_begin, comm());

  const auto num_bins_numerical =
      calc_num_bins(_match_container_begin, nan_begin);

  // Note that this bins in DESCENDING order.
  const auto [indptr, step_size] =
      utils::DiscreteBinner<decltype(get_value)>::bin(
          min, max, get_value, num_bins_numerical, _match_container_begin,
          nan_begin, _match_container_end, &bins);

  // -----------------------------------------------------------------------

  try_non_categorical_values(_column_used, _data_used, _sample_size, max,
                             step_size, indptr, &bins, _candidate_splits);

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_non_categorical_values(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size, const Float _max, const Float _step_size,
    const std::vector<size_t> &_indptr, containers::MatchPtrs *_bins,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------

  if (_indptr.size() < 2) {
    return;
  }

  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits

  for (size_t i = 1; i < _indptr.size(); ++i) {
    const auto cv = _max - static_cast<Float>(i) * _step_size;

    _candidate_splits->push_back(
        descriptors::Split(true, cv, _column_used, _data_used));
  }

  for (size_t i = _indptr.size() - 1; i > 0; --i) {
    const auto cv = _max - static_cast<Float>(i - 1) * _step_size;

    _candidate_splits->push_back(
        descriptors::Split(false, cv, _column_used, _data_used));
  }

  // -----------------------------------------------------------------------
  // If this is an activated node, we need to deactivate all samples for
  // which the numerical value is NULL

  if (is_activated_) {
    aggregation()->deactivate_matches_with_null_values(
        _bins->begin() + _indptr.back(), _bins->end());
  }

  // -----------------------------------------------------------------------
  // It is possible that std::distance( _match_container_begin,
  // _match_container_end ) is zero, when we are using the distributed
  // version. In that case we want this process to continue until this point,
  // because calculate_critical_values_numerical and
  // calculate_critical_values_discrete contains barriers and we want to
  // avoid a livelock.

  if (_indptr.back() == 0) {
    for (size_t i = 0; i < (_indptr.size() - 1) * 2; ++i) {
      aggregation()->update_optimization_criterion_and_clear_updates_current(
          0.0,  // _num_samples_smaller
          0.0   // _num_samples_greater
      );
    }

    aggregation()->revert_to_commit();

    optimization_criterion()->revert_to_commit();

    return;
  }

  // -----------------------------------------------------------------------
  // Try applying from above.

  // Apply changes and store resulting value of optimization criterion
  if (is_activated_) {
    aggregation()->deactivate_matches_from_above(_indptr, _bins->begin(),
                                                 _bins->end());
  } else {
    aggregation()->activate_matches_from_above(_indptr, _bins->begin(),
                                               _bins->end());
  }

  // -----------------------------------------------------------------------
  // Revert to original situation

  aggregation()->revert_to_commit();

  optimization_criterion()->revert_to_commit();

  // -----------------------------------------------------------------------
  // If this is an activated node, we need to deactivate all samples for
  // which the numerical value is NULL. We need to do this again, because
  // all of the changes would have been reverted by revert_to_commit().

  if (is_activated_) {
    aggregation()->deactivate_matches_with_null_values(
        _bins->begin() + _indptr.back(), _bins->end());
  }

  // -----------------------------------------------------------------------
  // Try applying from below.

  // Apply changes and store resulting value of optimization criterion
  if (is_activated_) {
    aggregation()->deactivate_matches_from_below(_indptr, _bins->begin(),
                                                 _bins->end());
  } else {
    aggregation()->activate_matches_from_below(_indptr, _bins->begin(),
                                               _bins->end());
  }

  // -----------------------------------------------------------------------
  // Revert to original situation

  aggregation()->revert_to_commit();

  optimization_criterion()->revert_to_commit();

  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_numerical_values(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  const auto nan_begin =
      separate_null_values(_match_container_begin, _match_container_end, false);

  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  const auto get_value = [](const containers::Match *const m) {
    return m->numerical_value;
  };

  const auto [min, max] =
      utils::MinMaxFinder<decltype(get_value), Float>::find_min_max(
          get_value, _match_container_begin, nan_begin, comm());

  const auto num_bins = calc_num_bins(_match_container_begin, nan_begin);

  // Note that this bins in DESCENDING order.
  const auto [indptr, step_size] =
      utils::NumericalBinner<decltype(get_value)>::bin(
          min, max, get_value, num_bins, _match_container_begin, nan_begin,
          _match_container_end, &bins);

  // -----------------------------------------------------------------------

  try_non_categorical_values(_column_used, _data_used, _sample_size, max,
                             step_size, indptr, &bins, _candidate_splits);

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_categorical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < same_units_categorical().size(); ++col) {
    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->categorical_value =
          get_same_unit_categorical(_population, _peripheral, *it, col);
    }

    try_categorical_values(col, enums::DataUsed::same_unit_categorical,
                           _sample_size, _match_container_begin,
                           _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_discrete(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < same_units_discrete().size(); ++col) {
    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value =
          get_same_unit_discrete(_population, _peripheral, *it, col);
    }

    const auto is_ts = tree_->same_units_.is_ts(_population, _peripheral,
                                                same_units_discrete(), col);

    const auto data_used = is_ts ? enums::DataUsed::same_unit_discrete_ts
                                 : enums::DataUsed::same_unit_discrete;
    try_discrete_values(col, data_used, _sample_size, _match_container_begin,
                        _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_same_units_numerical(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < same_units_numerical().size(); ++col) {
    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value =
          get_same_unit_numerical(_population, _peripheral, *it, col);
    }

    const auto is_ts = tree_->same_units_.is_ts(_population, _peripheral,
                                                same_units_numerical(), col);

    const auto data_used = is_ts ? enums::DataUsed::same_unit_numerical_ts
                                 : enums::DataUsed::same_unit_numerical;

    try_numerical_values(col, data_used, _sample_size, _match_container_begin,
                         _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_subfeatures(
    const containers::Subfeatures &_subfeatures, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  for (size_t col = 0; col < _subfeatures.size(); ++col) {
    if (skip_condition()) {
      continue;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value = get_x_subfeature(_subfeatures, *it, col);
    }

    try_numerical_values(col, enums::DataUsed::x_subfeature, _sample_size,
                         _match_container_begin, _match_container_end,
                         _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_text_population(
    const containers::DataFrameView &_population, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  assert_true(_population.df().row_indices_.size() == _population.num_text());

  assert_true(_population.df().word_indices_.size() == _population.num_text());

  // TODO: Stop reallocating bins all the time.
  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  if (_population.num_text() == 0) {
    return;
  }

  for (auto it = _match_container_begin; it != _match_container_end; ++it) {
    const auto ix = (*it)->ix_x_popul;

    assert_true(ix < _population.rows().size());

    (*it)->categorical_value = static_cast<Int>(_population.rows()[ix]);
  }

  for (size_t col = 0; col < _population.num_text(); ++col) {
    if (skip_condition()) {
      continue;
    }

    const auto row_index = _population.df().row_indices_.at(col);

    const auto word_index = _population.df().word_indices_.at(col);

    assert_true(row_index);

    assert_true(word_index);

    try_text_values(*row_index, *word_index, col, enums::DataUsed::x_popul_text,
                    _sample_size, _match_container_begin, _match_container_end,
                    &bins, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_text_peripheral(
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  assert_true(_peripheral.row_indices_.size() == _peripheral.num_text());

  assert_true(_peripheral.word_indices_.size() == _peripheral.num_text());

  // TODO: Stop reallocating bins all the time.
  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  if (_peripheral.num_text() == 0) {
    return;
  }

  for (auto it = _match_container_begin; it != _match_container_end; ++it) {
    (*it)->categorical_value = static_cast<Int>((*it)->ix_x_perip);
  }

  for (size_t col = 0; col < _peripheral.num_text(); ++col) {
    if (skip_condition()) {
      continue;
    }

    const auto &row_index = _peripheral.row_indices_.at(col);

    const auto &word_index = _peripheral.word_indices_.at(col);

    assert_true(row_index);

    assert_true(word_index);

    try_text_values(*row_index, *word_index, col, enums::DataUsed::x_perip_text,
                    _sample_size, _match_container_begin, _match_container_end,
                    &bins, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_text_values(
    const textmining::RowIndex &_row_index,
    const textmining::WordIndex &_word_index, const size_t _column_used,
    const enums::DataUsed _data_used, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<containers::Match *> *_bins,
    std::vector<descriptors::Split> *_candidate_splits) {
  // ----------------------------------------------------------------

  const auto get_range = [&_word_index](const containers::Match *const m) {
    assert_true(m->categorical_value >= 0);
    return _word_index.range(static_cast<size_t>(m->categorical_value));
  };

  const auto words = utils::WordMaker<decltype(get_range)>::make_words(
      _word_index.vocabulary(), get_range, _match_container_begin,
      _match_container_end, comm());

  assert_true(words);

  if (words->size() == 0) {
    return;
  }

  // ----------------------------------------------------------------

  const auto get_rownum = [](const containers::Match *const m) {
    assert_true(m->categorical_value >= 0);
    return static_cast<size_t>(m->categorical_value);
  };

  const auto rownum_indptr = utils::RownumBinner<decltype(get_rownum)>::bin(
      _word_index.nrows(), get_rownum, _match_container_begin,
      _match_container_end, _bins);

  const auto individual_word_index = containers::WordIndex(
      _bins->begin(), _bins->end(), _row_index, rownum_indptr);

  // ----------------------------------------------------------------

  try_text_values_individual(
      _column_used, _data_used, _sample_size, individual_word_index, words,
      _match_container_begin, _match_container_end, _candidate_splits);

  // -----------------------------------------------------------------------
  // If there are only two words, trying combined words does not
  // make any sense.

  if (words->size() < 3 || !tree_->allow_sets()) {
    return;
  }

  // -----------------------------------------------------------------------

  try_text_values_combined(_column_used, _data_used, _sample_size, _word_index,
                           words, _match_container_begin, _match_container_end,
                           _candidate_splits);

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_text_values_combined(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size, const textmining::WordIndex &_word_index,
    const std::shared_ptr<const std::vector<Int>> &_words,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  assert_true(_words);

  const auto num_words = _words->size();

  const auto storage_ix = optimization_criterion()->storage_ix();

  const auto sorted_by_containing = sort_categories(
      _words, storage_ix - num_words * 2, storage_ix - num_words);

  const auto sorted_by_not_containing =
      sort_categories(_words, storage_ix - num_words, storage_ix);

  // -----------------------------------------------------------------------

  // TODO: Stop reallocating bins all the time
  auto bins_by_containing =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  auto word_indptr_by_containing = make_word_indptr(
      _word_index, sorted_by_containing, _match_container_begin,
      _match_container_end, &bins_by_containing);

  const auto index_by_containing = containers::CategoryIndex(
      std::move(bins_by_containing), std::move(word_indptr_by_containing), 0);

  // -----------------------------------------------------------------------

  // TODO: Stop reallocating bins all the time
  auto bins_by_not_containing =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  auto word_indptr_by_not_containing = make_word_indptr(
      _word_index, sorted_by_not_containing, _match_container_begin,
      _match_container_end, &bins_by_not_containing);

  const auto index_by_not_containing =
      containers::CategoryIndex(std::move(bins_by_not_containing),
                                std::move(word_indptr_by_not_containing), 0);

  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits.

  for (auto cat = sorted_by_containing->cbegin();
       cat < sorted_by_containing->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        false,                           // _apply_from_above
        sorted_by_containing,            // _categories_used
        sorted_by_containing->cbegin(),  // _categories_used_begin
        cat + 1,                         // _categories_used_end
        _column_used,                    // _column_used
        _data_used                       // _data_used
        ));
  }

  for (auto cat = sorted_by_not_containing->cbegin();
       cat < sorted_by_not_containing->cend(); ++cat) {
    _candidate_splits->push_back(descriptors::Split(
        true,                                // _apply_from_above
        sorted_by_not_containing,            // _categories_used
        sorted_by_not_containing->cbegin(),  // _categories_used_begin
        cat + 1,                             // _categories_used_end
        _column_used,                        // _column_used
        _data_used                           // _data_used
        ));
  }

  // -----------------------------------------------------------------------

  if (std::distance(_match_container_begin, _match_container_end) == 0) {
    for (size_t i = 0; i < _words->size() * 2; ++i) {
      optimization_criterion()->store_current_stage(0.0, 0.0);
    }

    return;
  }

  //-----------------------------------------------------------------
  // Try applying all aggregation to all samples that
  // contain a certain category.

  if (is_activated_) {
    aggregation()->deactivate_matches_containing_categories(
        sorted_by_containing->cbegin(), sorted_by_containing->cend(),
        aggregations::Revert::after_all_categories, index_by_containing);
  } else {
    aggregation()->activate_matches_containing_categories(
        sorted_by_containing->cbegin(), sorted_by_containing->cend(),
        aggregations::Revert::after_all_categories, index_by_containing);
  }

  // -------------------------------------------------------------------
  // Try applying all aggregation to all samples that DO NOT
  // contain a certain category

  if (is_activated_) {
    aggregation()->deactivate_matches_not_containing_categories(
        sorted_by_not_containing->cbegin(), sorted_by_not_containing->cend(),
        aggregations::Revert::after_all_categories, index_by_not_containing);
  } else {
    aggregation()->activate_matches_not_containing_categories(
        sorted_by_not_containing->cbegin(), sorted_by_not_containing->cend(),
        aggregations::Revert::after_all_categories, index_by_not_containing);
  }

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

std::vector<size_t> DecisionTreeNode::make_word_indptr(
    const textmining::WordIndex &_word_index,
    const std::shared_ptr<const std::vector<Int>> &_sorted_words,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    containers::MatchPtrs *_bins) const {
  // ----------------------------------------------------------------

  assert_true(_sorted_words);

  // ----------------------------------------------------------------

  const auto extract_word = [&_word_index,
                             _sorted_words](const containers::Match *m) -> Int {
    const auto rownum = m->categorical_value;

    assert_true(rownum >= 0);

    const auto range = _word_index.range(static_cast<size_t>(rownum));

    if (range.begin() == range.end()) {
      return -1;
    }

    for (const auto word_ix : *_sorted_words) {
      for (const auto r : range) {
        if (r == word_ix) {
          return word_ix;
        }

        if (r > word_ix) {
          break;
        }
      }
    }

    return -1;
  };

  // ----------------------------------------------------------------
  // Extracting the words is expensive, so we precalculate it.

  constexpr Int WORD_NOT_SET = -2;

  auto words = std::vector<Int>(_word_index.nrows(), WORD_NOT_SET);

  for (auto it = _match_container_begin; it != _match_container_end; ++it) {
    assert_true((*it)->categorical_value >= 0);

    const auto ix = static_cast<size_t>((*it)->categorical_value);

    assert_true(ix < words.size());

    if (words[ix] != WORD_NOT_SET) {
      continue;
    }

    words[ix] = extract_word(*it);
  }

  // ----------------------------------------------------------------

  const auto get_word = [&words](const containers::Match *m) {
    assert_true(m->categorical_value >= 0);
    const auto ix = static_cast<size_t>(m->categorical_value);
    assert_true(ix < words.size());
    return words[ix];
  };

  // ----------------------------------------------------------------

  const auto is_not_nan = [get_word](const containers::Match *m) -> bool {
    return (get_word(m) >= 0);
  };

  /// Moves text fields without words from the vocabulary to the end.
  const auto nan_begin =
      std::partition(_match_container_begin, _match_container_end, is_not_nan);

  // ----------------------------------------------------------------

  return utils::WordBinner<decltype(get_word)>::bin(
      _word_index.vocabulary(), get_word, _match_container_begin, nan_begin,
      _match_container_end, _bins);

  // ----------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_text_values_individual(
    const size_t _column_used, const enums::DataUsed _data_used,
    const size_t _sample_size, const containers::WordIndex &_word_index,
    const std::shared_ptr<const std::vector<Int>> &_words,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  assert_true(_words);

  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits
  // The samples where the category equals category_used_
  // are copied into samples_smaller. This makes sense, because
  // for the numerical values, samples_smaller contains all values
  // <= critical_value().
  // Because we first try the samples containing a category, that means
  // the change must be applied from below, so apply_from_above is
  // first false and then true.

  for (auto word = _words->cbegin(); word < _words->cend(); ++word) {
    _candidate_splits->push_back(descriptors::Split(
        false,         // _apply_from_above
        _words,        // _categories_used
        word,          // _categories_used_begin
        word + 1,      // _categories_used_end
        _column_used,  // _column_used
        _data_used     // _data_used
        ));
  }

  for (auto word = _words->cbegin(); word < _words->cend(); ++word) {
    _candidate_splits->push_back(descriptors::Split(
        true,          // _apply_from_above
        _words,        // _categories_used
        word,          // _categories_used_begin
        word + 1,      // _categories_used_end
        _column_used,  // _column_used
        _data_used     // _data_used
        ));
  }

  // -----------------------------------------------------------------------
  // It is possible that std::distance( _match_container_begin,
  // _match_container_end ) is zero, when we are using the distributed
  // version. In that case we want this process until this point, because
  // calculate_critical_values_numerical contains a barrier and we want to
  // avoid a deadlock.

  if (std::distance(_match_container_begin, _match_container_end) == 0) {
    for (size_t i = 0; i < _words->size() * 2; ++i) {
      optimization_criterion()->store_current_stage(0.0, 0.0);
    }

    return;
  }

  // -----------------------------------------------------------------------
  // Try applying all aggregation to all samples that
  // contain a certain category.

  if (is_activated_) {
    aggregation()->deactivate_matches_containing_words(
        _words->cbegin(), _words->cend(),
        aggregations::Revert::after_each_category, _word_index);
  } else {
    aggregation()->activate_matches_containing_words(
        _words->cbegin(), _words->cend(),
        aggregations::Revert::after_each_category, _word_index);
  }

  // -----------------------------------------------------------------------
  // Try applying all aggregation to all samples that DO NOT
  // contain a certain category

  if (is_activated_) {
    aggregation()->deactivate_matches_not_containing_words(
        _words->cbegin(), _words->cend(),
        aggregations::Revert::after_each_category, _word_index);
  } else {
    aggregation()->activate_matches_not_containing_words(
        _words->cbegin(), _words->cend(),
        aggregations::Revert::after_each_category, _word_index);
  }

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_time_stamps_window(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral, const size_t _sample_size,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  if (tree_->delta_t() > 0.0) {
    if (skip_condition()) {
      return;
    }

    for (auto it = _match_container_begin; it != _match_container_end; ++it) {
      (*it)->numerical_value =
          get_time_stamps_diff(_population, _peripheral, *it);
    }

    try_window(_match_container_begin, _match_container_end, _candidate_splits);
  }
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::try_window(
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _match_container_end,
    std::vector<descriptors::Split> *_candidate_splits) {
  // -----------------------------------------------------------------------

  assert_true(tree_->delta_t() > 0.0);

  const auto step_size = tree_->delta_t();

  // -----------------------------------------------------------------------

  const auto get_value = [](const containers::Match *const m) {
    return m->numerical_value;
  };

  const auto [min, max] =
      utils::MinMaxFinder<decltype(get_value), Float>::find_min_max(
          get_value, _match_container_begin, _match_container_end, comm());

  const auto num_bins = static_cast<size_t>((max - min) / step_size) + 1;

  // Be reasonable - avoid memory overflow.
  if (num_bins > 1000000) {
    return;
  }

  // ------------------------------------------------------------------------

  auto bins =
      containers::MatchPtrs(_match_container_begin, _match_container_end);

  // Note that this bins in DESCENDING order.
  const auto indptr =
      utils::NumericalBinner<decltype(get_value)>::bin_given_step_size(
          min, max, get_value, step_size, _match_container_begin,
          _match_container_end, _match_container_end, &bins);

  if (indptr.size() == 0) {
    return;
  }

  // -----------------------------------------------------------------------
  // Add new splits to the candidate splits

  for (size_t i = 1; i < indptr.size(); ++i) {
    const auto cv = max - static_cast<Float>(i - 1) * step_size;

    _candidate_splits->push_back(
        descriptors::Split(true, cv, 0, enums::DataUsed::time_stamps_window));
  }

  for (size_t i = 1; i < indptr.size(); ++i) {
    const auto cv = max - static_cast<Float>(i - 1) * step_size;

    _candidate_splits->push_back(
        descriptors::Split(false, cv, 0, enums::DataUsed::time_stamps_window));
  }

  // -----------------------------------------------------------------------
  // It is possible that std::distance( _match_container_begin,
  // _match_container_end ) is zero, when we are using the distributed
  // version. In that case we want this process to continue until this point,
  // because calculate_critical_values_numerical and
  // calculate_critical_values_discrete contains barriers and we want to
  // avoid a livelock.

  if (std::distance(_match_container_begin, _match_container_end) == 0) {
    for (size_t i = 0; i < (indptr.size() - 1) * 2; ++i) {
      aggregation()->update_optimization_criterion_and_clear_updates_current(
          0.0,  // _num_samples_smaller
          0.0   // _num_samples_greater
      );
    }

    return;
  }

  // -----------------------------------------------------------------------
  // Try applying outside the window

  // Apply changes and store resulting value of optimization criterion
  if (is_activated_) {
    aggregation()->deactivate_matches_outside_window(indptr, bins.begin(),
                                                     bins.end());
  } else {
    aggregation()->activate_matches_outside_window(indptr, bins.begin(),
                                                   bins.end());
  }

  // -----------------------------------------------------------------------
  // Try applying inside the window

  // Apply changes and store resulting value of optimization criterion
  if (is_activated_) {
    aggregation()->deactivate_matches_in_window(indptr, bins.begin(),
                                                bins.end());
  } else {
    aggregation()->activate_matches_in_window(indptr, bins.begin(), bins.end());
  }

  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void DecisionTreeNode::update(
    const auto _allow_deactivation,
    containers::MatchPtrs::iterator _match_container_begin,
    containers::MatchPtrs::iterator _separator,
    containers::MatchPtrs::iterator _match_container_end,
    aggregations::AbstractFitAggregation *_aggregation) const {
  if (apply_from_above()) {
    if (!is_activated_) {
      _aggregation->activate_partition_from_above(
          _match_container_begin, _separator, _match_container_end);
    } else {
      if (!_allow_deactivation) {
        return;
      }

      _aggregation->deactivate_partition_from_above(
          _match_container_begin, _separator, _match_container_end);
    }
  } else {
    if (!is_activated_) {
      _aggregation->activate_partition_from_below(
          _match_container_begin, _separator, _match_container_end);
    } else {
      if (!_allow_deactivation) {
        return;
      }

      _aggregation->deactivate_partition_from_below(
          _match_container_begin, _separator, _match_container_end);
    }
  }
}

// ----------------------------------------------------------------------------
}  // namespace decisiontrees
}  // namespace multirel

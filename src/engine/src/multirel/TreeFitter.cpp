#include "multirel/ensemble/TreeFitter.hpp"

namespace multirel {
namespace ensemble {
// ------------------------------------------------------------------------

void TreeFitter::find_best_trees(
    const size_t _num_trees, const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion> &_opt,
    const std::vector<Float> &_values,
    std::vector<containers::Matches> *_samples,
    std::vector<containers::MatchPtrs> *_match_containers,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<decisiontrees::DecisionTree> *_trees) {
  assert_true(_candidate_trees->size() == _values.size());

  std::vector<std::tuple<size_t, Float>> tuples;

  for (size_t ix = 0; ix < _candidate_trees->size(); ++ix) {
    tuples.push_back(std::make_tuple(ix, _values[ix]));
  }

  std::sort(tuples.begin(), tuples.end(),
            [](const std::tuple<size_t, Float> &t1,
               const std::tuple<size_t, Float> &t2) {
              return std::get<1>(t1) > std::get<1>(t2);
            });

  for (size_t i = 0; i < std::min(_num_trees, tuples.size()); ++i) {
    if (i > 0 &&
        std::get<1>(tuples[i]) < tree_hyperparameters().regularization_) {
      break;
    }

    auto it = _candidate_trees->begin();

    std::advance(it, std::get<0>(tuples[i]));

    _trees->emplace_back(std::move(*it));
  }

  *_candidate_trees = std::list<decisiontrees::DecisionTree>();
}

// ------------------------------------------------------------------------

void TreeFitter::fit(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion> &_opt,
    std::vector<containers::Matches> *_samples,
    std::vector<containers::MatchPtrs> *_match_containers,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<decisiontrees::DecisionTree> *_trees) {
  std::vector<Float> values;

  probe(_table_holder, _subfeatures, _aggregation_impl, _opt, _samples,
        _match_containers, _candidate_trees, &values);

  find_best_trees(1, _table_holder, _subfeatures, _opt, values, _samples,
                  _match_containers, _candidate_trees, _trees);
}

// ------------------------------------------------------------------------

void TreeFitter::fit_tree(
    const containers::DataFrameView &_population,
    const containers::DataFrame &_peripheral,
    const containers::Subfeatures &_subfeatures,
    const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion> &_opt,
    std::vector<containers::Matches> *_samples,
    std::vector<containers::MatchPtrs> *_match_containers,
    decisiontrees::DecisionTree *_tree) {
  assert_true(_match_containers->size() == _samples->size());

  const auto ix_perip_used = _tree->column_to_be_aggregated().ix_perip_used;

  assert_true(ix_perip_used < static_cast<Int>(_match_containers->size()));

  auto &matches = _samples->at(ix_perip_used);

  auto &match_ptrs = _match_containers->at(ix_perip_used);

  assert_true(matches.size() == match_ptrs.size());

  const auto aggregation =
      _tree->make_aggregation(_population, _peripheral, _subfeatures,
                              _aggregation_impl, _opt, &matches);

  assert_true(aggregation);

  auto begin = match_ptrs.begin();

  if (_tree->aggregation_type() !=
      aggregations::AggregationType::Count::type()) {
    begin = aggregation->separate_null_values(&match_ptrs);
  }

  _tree->fit(_population, _peripheral, _subfeatures, aggregation, begin,
             match_ptrs.end(), _opt.get());

  aggregation->clear();
}

// ------------------------------------------------------------------------

void TreeFitter::probe(
    const decisiontrees::TableHolder &_table_holder,
    const std::vector<containers::Subfeatures> &_subfeatures,
    const std::shared_ptr<aggregations::AggregationImpl> &_aggregation_impl,
    const std::shared_ptr<optimizationcriteria::OptimizationCriterion> &_opt,
    std::vector<containers::Matches> *_samples,
    std::vector<containers::MatchPtrs> *_match_containers,
    std::list<decisiontrees::DecisionTree> *_candidate_trees,
    std::vector<Float> *_values) {
  for (auto &tree : *_candidate_trees) {
    const auto ix = tree.ix_perip_used();

    assert_true(ix < _table_holder.main_tables().size());

    assert_true(_subfeatures.size() == _table_holder.main_tables().size());

    assert_true(_subfeatures.size() ==
                _table_holder.peripheral_tables().size());

    fit_tree(_table_holder.main_tables().at(ix),
             _table_holder.peripheral_tables().at(ix), _subfeatures.at(ix),
             _aggregation_impl, _opt, _samples, _match_containers, &tree);

    assert_true(_opt);

    _values->push_back(_opt->value());

    _opt->reset();
  }
}

// ------------------------------------------------------------------------
}  // namespace ensemble
}  // namespace multirel

// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/pipelines/Score.hpp"

// ----------------------------------------------------------------------------

#include "metrics/Scores.hpp"

// ----------------------------------------------------------------------------

#include "engine/pipelines/FittedPipeline.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {

std::shared_ptr<const metrics::Scores> Score::calculate_feature_stats(
    const Pipeline& _pipeline, const FittedPipeline& _fitted,
    const containers::NumericalFeatures _features,
    const Poco::JSON::Object& _cmd,
    const containers::DataFrame& _population_df) {
  if (_features.size() == 0) {
    return nullptr;
  }

  const auto nrows = _features.at(0).size();

  const auto ncols = _features.size();

  std::vector<const Float*> targets;

  for (size_t j = 0; j < _population_df.num_targets(); ++j) {
    targets.push_back(_population_df.target(j).data());
  }

  size_t num_bins = 30;

  num_bins = std::min(num_bins, nrows / 30);

  num_bins = std::max(num_bins, static_cast<size_t>(10));

  auto scores = std::make_shared<metrics::Scores>(_pipeline.scores());

  scores->from_json_obj(metrics::Summarizer::calculate_feature_correlations(
      _features, nrows, ncols, targets));

  scores->from_json_obj(metrics::Summarizer::calculate_feature_plots(
      _features, nrows, ncols, num_bins, targets));

  scores->from_json_obj(feature_names_as_obj(_fitted));

  return scores;
}

// ----------------------------------------------------------------------------

std::pair<std::vector<helpers::ColumnDescription>,
          std::vector<std::vector<Float>>>
Score::column_importances(const Pipeline& _pipeline,
                          const FittedPipeline& _fitted) {
  auto c_desc = std::vector<helpers::ColumnDescription>();

  auto c_importances = std::vector<std::vector<Float>>();

  if (_fitted.predictors_.predictors_.size() == 0) {
    return std::make_pair(c_desc, c_importances);
  }

  const auto f_importances = feature_importances(_fitted.predictors_);

  auto importance_makers =
      std::vector<helpers::ImportanceMaker>(f_importances.size());

  column_importances_auto(_fitted, f_importances, &importance_makers);

  column_importances_manual(_pipeline, _fitted, f_importances,
                            &importance_makers);

  const auto make_staging_table_column =
      [](const std::string& _colname,
         const std::string& _alias) -> std::string {
    return transpilation::HumanReadableSQLGenerator().make_staging_table_column(
        _colname, _alias);
  };

  for (auto& i_maker : importance_makers) {
    i_maker = helpers::Macros::modify_column_importances(
        i_maker, make_staging_table_column);
  }

  fill_zeros(&importance_makers);

  for (const auto& i_maker : importance_makers) {
    extract_coldesc(i_maker.importances(), &c_desc);
    extract_importance_values(i_maker.importances(), &c_importances);
  }

  return std::make_pair(c_desc, c_importances);
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Score::column_importances_as_obj(
    const Pipeline& _pipeline, const FittedPipeline& _fitted) {
  const auto [c_desc, c_importances] = column_importances(_pipeline, _fitted);

  if (c_importances.size() == 0) {
    return Poco::JSON::Object();
  }

  auto column_descriptions = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  for (const auto& desc : c_desc) {
    column_descriptions->add(desc.to_json_obj());
  }

  const auto column_importances = transpose(c_importances);

  Poco::JSON::Object obj;

  obj.set("column_descriptions_", column_descriptions);

  obj.set("column_importances_", column_importances);

  return obj;
}

// ----------------------------------------------------------------------------

void Score::column_importances_auto(
    const FittedPipeline& _fitted,
    const std::vector<std::vector<Float>>& _f_importances,
    std::vector<helpers::ImportanceMaker>* _importance_makers) {
  assert_true(_f_importances.size() == _importance_makers->size());

  const auto autofeatures = _fitted.predictors_.impl_->autofeatures();

  assert_true(autofeatures.size() == _fitted.feature_learners_.size());

  for (size_t i = 0; i < _f_importances.size(); ++i) {
    const auto& f_imp_for_target = _f_importances.at(i);

    size_t ix_begin = 0;

    for (size_t j = 0; j < _fitted.feature_learners_.size(); ++j) {
      const auto& fl = _fitted.feature_learners_.at(j);

      const auto ix_end = ix_begin + autofeatures.at(j).size();

      const auto importance_factors =
          make_importance_factors(fl->num_features(), autofeatures.at(j),
                                  f_imp_for_target.begin() + ix_begin,
                                  f_imp_for_target.begin() + ix_end);

      ix_begin = ix_end;

      const auto c_imp_for_target = fl->column_importances(importance_factors);

      _importance_makers->at(i).merge(c_imp_for_target);
    }
  }
}

// ----------------------------------------------------------------------------

void Score::column_importances_manual(
    const Pipeline& _pipeline, const FittedPipeline& _fitted,
    const std::vector<std::vector<Float>>& _f_importances,
    std::vector<helpers::ImportanceMaker>* _importance_makers) {
  assert_true(_f_importances.size() == _importance_makers->size());

  for (size_t i = 0; i < _f_importances.size(); ++i) {
    const auto& f_imp_for_target = _f_importances.at(i);

    auto j = _fitted.predictors_.impl_->num_autofeatures();

    assert_true(j + _fitted.predictors_.impl_->num_manual_features() ==
                f_imp_for_target.size());

    const auto population_name = _pipeline.parse_population();

    assert_true(population_name);

    for (const auto& colname :
         _fitted.predictors_.impl_->numerical_colnames()) {
      const auto desc = helpers::ColumnDescription(
          _importance_makers->at(i).population(), *population_name, colname);

      const auto value = f_imp_for_target.at(j++);

      _importance_makers->at(i).add_to_importances(desc, value);
    }

    for (const auto& colname :
         _fitted.predictors_.impl_->categorical_colnames()) {
      const auto desc = helpers::ColumnDescription(
          _importance_makers->at(i).population(), *population_name, colname);

      const auto value = f_imp_for_target.at(j++);

      _importance_makers->at(i).add_to_importances(desc, value);
    }
  }
}

// ----------------------------------------------------------------------------

void Score::extract_coldesc(
    const std::map<helpers::ColumnDescription, Float>& _column_importances,
    std::vector<helpers::ColumnDescription>* _coldesc) {
  if (_coldesc->size() == 0) {
    for (const auto& [key, _] : _column_importances) {
      _coldesc->push_back(key);
    }
  }
}

// ----------------------------------------------------------------------------

void Score::extract_importance_values(
    const std::map<helpers::ColumnDescription, Float>& _column_importances,
    std::vector<std::vector<Float>>* _all_column_importances) {
  auto importance_values = std::vector<Float>();

  for (const auto& [_, value] : _column_importances) {
    importance_values.push_back(value);
  }

  _all_column_importances->push_back(importance_values);
}

// ----------------------------------------------------------------------------

std::vector<std::vector<Float>> Score::feature_importances(
    const Predictors& _predictors) {
  const auto n_features = _predictors.num_features();

  std::vector<std::vector<Float>> fi;

  for (size_t t = 0; t < _predictors.size(); ++t) {
    auto current_fi = std::vector<Float>(n_features);

    if (_predictors[t].size() == 0) {
      fi.push_back(current_fi);
      continue;
    }

    for (auto& p : _predictors[t]) {
      const auto fi_for_this_target = p->feature_importances(n_features);

      assert_true(current_fi.size() == fi_for_this_target.size());

      std::transform(current_fi.begin(), current_fi.end(),
                     fi_for_this_target.begin(), current_fi.begin(),
                     std::plus<Float>());
    }

    const auto n = static_cast<Float>(_predictors[t].size());

    for (auto& val : current_fi) {
      val /= n;
    }

    fi.push_back(current_fi);
  }

  return fi;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Score::feature_importances_as_obj(
    const FittedPipeline& _fitted) {
  const auto feature_importances_transposed =
      feature_importances(_fitted.predictors_);

  assert_msg(feature_importances_transposed.size() == _fitted.targets().size(),
             "feature_importances_transposed.size(): " +
                 std::to_string(feature_importances_transposed.size()) +
                 ", _fitted.targets().size(): " +
                 std::to_string(_fitted.targets().size()));

  if (feature_importances_transposed.size() == 0) {
    return Poco::JSON::Object();
  }

  const auto feature_importances = transpose(feature_importances_transposed);

  Poco::JSON::Object obj;

  obj.set("feature_importances_", feature_importances);

  return obj;
}

// ----------------------------------------------------------------------------

Poco::JSON::Object Score::feature_names_as_obj(const FittedPipeline& _fitted) {
  const auto fn = _fitted.feature_names();

  Poco::JSON::Array::Ptr all_names(new Poco::JSON::Array());

  for (const auto& name : std::get<0>(fn)) {
    all_names->add(name);
  }

  for (const auto& name : std::get<1>(fn)) {
    all_names->add(name);
  }

  for (const auto& name : std::get<2>(fn)) {
    all_names->add(name);
  }

  Poco::JSON::Object obj;

  obj.set("feature_names_", all_names);

  return obj;
}

// ----------------------------------------------------------------------------

void Score::fill_zeros(std::vector<helpers::ImportanceMaker>* _f_importances) {
  if (_f_importances->size() == 0) {
    return;
  }

  const auto fill_all = [](const helpers::ImportanceMaker& _f1,
                           helpers::ImportanceMaker* _f2) {
    for (const auto& [desc, _] : _f1.importances()) {
      _f2->add_to_importances(desc, 0.0);
    }
  };

  for (size_t i = 1; i < _f_importances->size(); ++i) {
    fill_all(_f_importances->at(i), &_f_importances->at(0));
  }

  for (size_t i = 1; i < _f_importances->size(); ++i) {
    fill_all(_f_importances->at(0), &_f_importances->at(i));
  }
}

// ----------------------------------------------------------------------------

std::vector<Float> Score::make_importance_factors(
    const size_t _num_features, const std::vector<size_t>& _autofeatures,
    const std::vector<Float>::const_iterator _begin,
    const std::vector<Float>::const_iterator _end) {
  auto importance_factors = std::vector<Float>(_num_features);

  assert_true(_end >= _begin);

  assert_true(_autofeatures.size() ==
              static_cast<size_t>(std::distance(_begin, _end)));

  for (size_t i = 0; i < _autofeatures.size(); ++i) {
    const auto ix = _autofeatures.at(i);

    assert_true(ix < importance_factors.size());

    importance_factors.at(ix) = *(_begin + i);
  }

  return importance_factors;
}

// ----------------------------------------------------------------------------

std::pair<fct::Ref<const metrics::Scores>, Poco::JSON::Object> Score::score(
    const Pipeline& _pipeline, const FittedPipeline& _fitted,
    const containers::DataFrame& _population_df,
    const std::string& _population_name,
    const containers::NumericalFeatures& _yhat) {
  const auto get_feature = [](const auto& _col) -> helpers::Feature<Float> {
    return helpers::Feature<Float>(_col.data_ptr());
  };

  const auto y = fct::collect::vector<helpers::Feature<Float>>(
      _population_df.targets() | VIEWS::transform(get_feature));

  if (_yhat.size() != y.size()) {
    throw std::runtime_error(
        "Number of columns in predictions and targets do not "
        "match! "
        "Number of columns in predictions: " +
        std::to_string(_yhat.size()) +
        ". Number of columns in targets: " + std::to_string(y.size()) + ".");
  }

  for (size_t i = 0; i < y.size(); ++i) {
    if (_yhat[i].size() != y[i].size()) {
      throw std::runtime_error(
          "Number of rows in predictions and targets do not "
          "match! "
          "Number of rows in predictions: " +
          std::to_string(_yhat[i].size()) +
          ". Number of rows in targets: " + std::to_string(y[i].size()) + ".");
    }
  }

  auto obj = metrics::Scorer::score(_fitted.is_classification(), _yhat, y);

  auto scores = fct::Ref<metrics::Scores>::make(_pipeline.scores());

  obj.set("set_used_", _population_name);

  scores->from_json_obj(obj);

  scores->to_history();

  return std::make_pair(scores, metrics::Scorer::get_metrics(obj));
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr Score::transpose(
    const std::vector<std::vector<Float>>& _original) {
  assert_true(_original.size() > 0);

  const auto n = _original.at(0).size();

  Poco::JSON::Array::Ptr transposed(new Poco::JSON::Array());

  for (std::size_t i = 0; i < n; ++i) {
    Poco::JSON::Array::Ptr temp(new Poco::JSON::Array());

    for (const auto& vec : _original) {
      assert_msg(vec.size() == n, "vec.size(): " + std::to_string(vec.size()) +
                                      ", n: " + std::to_string(n));

      temp->add(vec.at(i));
    }

    transposed->add(temp);
  }

  return transposed;
}

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

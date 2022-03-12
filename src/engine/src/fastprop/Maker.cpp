#include "fastprop/subfeatures/Maker.hpp"

namespace fastprop {
namespace subfeatures {
// ----------------------------------------------------------------------------

std::pair<std::shared_ptr<const FastPropContainer>, helpers::FeatureContainer>
Maker::fit(const MakerParams& _params) {
  const auto dummy_rownums = std::make_shared<std::vector<size_t>>(0);

  const auto population_view =
      helpers::DataFrameView(_params.population_, dummy_rownums);

  assert_true(_params.peripheral_names_);

  const auto make_staging_table_colname =
      [](const std::string& _colname) -> std::string {
    return transpilation::SQLite3Generator().make_staging_table_colname(
        _colname);
  };

  const auto params = helpers::TableHolderParams{
      .feature_container_ = std::nullopt,
      .make_staging_table_colname_ = make_staging_table_colname,
      .peripheral_ = _params.peripheral_,
      .peripheral_names_ = *_params.peripheral_names_,
      .placeholder_ = _params.placeholder_,
      .population_ = population_view,
      .row_index_container_ = _params.row_index_container_,
      .word_index_container_ = _params.word_index_container_};

  const auto table_holder = helpers::TableHolder(params);

  const auto fast_prop_container =
      fit_fast_prop_container(table_holder, _params);

  const auto feature_container = transform(
      MakerParams{.fast_prop_container_ = fast_prop_container,
                  .hyperparameters_ = _params.hyperparameters_,
                  .logger_ = _params.logger_,
                  .peripheral_ = _params.peripheral_,
                  .peripheral_names_ = _params.peripheral_names_,
                  .placeholder_ = _params.placeholder_,
                  .population_ = _params.population_,
                  .prefix_ = _params.prefix_,
                  .row_index_container_ = _params.row_index_container_,
                  .temp_dir_ = _params.temp_dir_,
                  .word_index_container_ = _params.word_index_container_});

  return std::make_pair(fast_prop_container, feature_container);
}

// ----------------------------------------------------------------------------

std::shared_ptr<const FastPropContainer> Maker::fit_fast_prop_container(
    const helpers::TableHolder& _table_holder, const MakerParams& _params) {
  const auto subcontainers = make_subcontainers(_table_holder, _params);

  const auto new_placeholder = make_placeholder(_params);

  if (!new_placeholder) {
    return std::make_shared<const FastPropContainer>(
        std::shared_ptr<const algorithm::FastProp>(), subcontainers);
  }

  const auto fast_prop = std::make_shared<algorithm::FastProp>(
      _params.hyperparameters_, _params.peripheral_names_, new_placeholder);

  assert_true(_params.row_index_container_);

  const auto params =
      algorithm::FitParams{.feature_container_ = std::nullopt,
                           .logger_ = _params.logger_,
                           .peripheral_ = _params.peripheral_,
                           .population_ = _params.population_,
                           .row_indices_ = _params.row_index_container_.value(),
                           .temp_dir_ = _params.temp_dir_,
                           .word_indices_ = _params.word_index_container_};

  fast_prop->fit(params, true);

  return std::make_shared<const FastPropContainer>(fast_prop, subcontainers);
}

// ----------------------------------------------------------------------------

size_t Maker::find_peripheral_ix(const MakerParams& _params, const size_t _i) {
  assert_true(_params.peripheral_names_);

  const auto name = _params.placeholder_.joined_tables_.at(_i).name_;

  for (size_t ix = 0; ix < _params.peripheral_names_->size(); ++ix) {
    if (_params.peripheral_names_->at(ix) == name) {
      return ix;
    }
  }

  assert_msg(false, name);

  return 0;
}

// ----------------------------------------------------------------------------

MakerParams Maker::make_params(const MakerParams& _params, const size_t _i) {
  assert_true(!_params.fast_prop_container_ ||
              _params.fast_prop_container_->subcontainers(_i));

  assert_true(_i < _params.placeholder_.joined_tables_.size());

  assert_true(_params.peripheral_names_);

  assert_true(_params.peripheral_names_->size() <= _params.peripheral_.size());

  assert_true(!_params.row_index_container_ ||
              _params.peripheral_names_->size() <=
                  _params.row_index_container_->peripheral().size());

  assert_true(_params.peripheral_names_->size() <=
              _params.word_index_container_.peripheral().size());

  const auto ix = find_peripheral_ix(_params, _i);

  const auto row_index_container =
      _params.row_index_container_
          ? helpers::RowIndexContainer(
                _params.row_index_container_->peripheral().at(ix),
                _params.row_index_container_->peripheral())
          : std::optional<helpers::RowIndexContainer>();

  const auto word_index_container = helpers::WordIndexContainer(
      _params.word_index_container_.peripheral().at(ix),
      _params.word_index_container_.peripheral());

  return MakerParams{.fast_prop_container_ =
                         _params.fast_prop_container_
                             ? _params.fast_prop_container_->subcontainers(_i)
                             : std::shared_ptr<const FastPropContainer>(),
                     .hyperparameters_ = _params.hyperparameters_,
                     .logger_ = _params.logger_,
                     .peripheral_ = _params.peripheral_,
                     .peripheral_names_ = _params.peripheral_names_,
                     .placeholder_ = _params.placeholder_.joined_tables_.at(_i),
                     .population_ = _params.peripheral_.at(ix),
                     .prefix_ = _params.prefix_ + std::to_string(_i + 1) + "_",
                     .row_index_container_ = row_index_container,
                     .temp_dir_ = _params.temp_dir_,
                     .word_index_container_ = word_index_container};
}

// ----------------------------------------------------------------------------

std::shared_ptr<const helpers::Placeholder> Maker::make_placeholder(
    const MakerParams& _params) {
  const auto& placeholder = _params.placeholder_;

  std::vector<bool> allow_lagged_targets;

  std::vector<helpers::Placeholder> joined_tables;

  std::vector<std::string> join_keys_used;

  std::vector<std::string> other_join_keys_used;

  std::vector<std::string> other_time_stamps_used;

  std::vector<bool> propositionalization;

  std::vector<std::string> time_stamps_used;

  std::vector<std::string> upper_time_stamps_used;

  for (size_t i = 0; i < placeholder.propositionalization_.size(); ++i) {
    if (!placeholder.propositionalization_.at(i)) {
      continue;
    }

    allow_lagged_targets.push_back(placeholder.allow_lagged_targets_.at(i));

    joined_tables.push_back(placeholder.joined_tables_.at(i));

    join_keys_used.push_back(placeholder.join_keys_used_.at(i));

    other_join_keys_used.push_back(placeholder.other_join_keys_used_.at(i));

    other_time_stamps_used.push_back(placeholder.other_time_stamps_used_.at(i));

    propositionalization.push_back(true);

    time_stamps_used.push_back(placeholder.time_stamps_used_.at(i));

    upper_time_stamps_used.push_back(placeholder.upper_time_stamps_used_.at(i));
  }

  if (joined_tables.size() == 0) {
    return std::shared_ptr<const helpers::Placeholder>();
  }

  return std::make_shared<const helpers::Placeholder>(
      allow_lagged_targets, joined_tables, join_keys_used, placeholder.name_,
      other_join_keys_used, other_time_stamps_used, propositionalization,
      time_stamps_used, upper_time_stamps_used);
}

// ----------------------------------------------------------------------------

std::shared_ptr<typename FastPropContainer::Subcontainers>
Maker::make_subcontainers(const helpers::TableHolder& _table_holder,
                          const MakerParams& _params) {
  const auto subcontainers =
      std::make_shared<typename FastPropContainer::Subcontainers>();

  const auto& placeholder = _params.placeholder_;

  const auto make_subcontainer = [&_table_holder, &placeholder,
                                  &_params](size_t _i) {
    if (placeholder.propositionalization_.at(_i)) {
      return std::shared_ptr<const FastPropContainer>();
    }

    return _table_holder.subtables().at(_i)
               ? fit_fast_prop_container(*_table_holder.subtables().at(_i),
                                         make_params(_params, _i))
               : std::shared_ptr<const FastPropContainer>();
  };

  assert_true(_table_holder.subtables().size() >=
              placeholder.propositionalization_.size());

  const auto iota =
      fct::iota<size_t>(0, placeholder.propositionalization_.size());

  const auto range = iota | VIEWS::transform(make_subcontainer);

  return std::make_shared<typename FastPropContainer::Subcontainers>(
      fct::collect::vector<std::shared_ptr<const FastPropContainer>>(range));
}

// ----------------------------------------------------------------------------

helpers::FeatureContainer Maker::transform(const MakerParams& _params) {
  assert_true(_params.fast_prop_container_);

  assert_true(_params.fast_prop_container_->size() ==
              _params.placeholder_.joined_tables_.size());

  assert_true(_params.prefix_ != "");

  const auto features = transform_make_features(_params);

  const auto subcontainers = transform_make_subcontainers(_params);

  return helpers::FeatureContainer(features, subcontainers);
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<helpers::Column<Float>>>
Maker::transform_make_features(const MakerParams& _params) {
  if (!_params.fast_prop_container_->has_fast_prop()) {
    return std::make_shared<std::vector<helpers::Column<Float>>>();
  }

  const auto& fast_prop = _params.fast_prop_container_->fast_prop();

  const auto index = fct::collect::vector<size_t>(
      fct::iota<size_t>(0, fast_prop.num_features()));

  const auto params = algorithm::TransformParams{
      .feature_container_ = std::nullopt,
      .index_ = index,
      .logger_ = _params.logger_,
      .peripheral_ = _params.peripheral_,
      .population_ = _params.population_,
      .temp_dir_ = _params.temp_dir_,
      .word_indices_ = _params.word_index_container_};

  const auto features = fast_prop.transform(params, nullptr, true);

  const auto to_column = [&_params,
                          &features](size_t _i) -> helpers::Column<Float> {
    return helpers::Column<Float>(features.at(_i).const_ptr(),
                                  helpers::Macros::fast_prop_feature() +
                                      _params.prefix_ + std::to_string(_i + 1),
                                  {}, "");
  };

  const auto iota = fct::iota<size_t>(0, features.size());

  return std::make_shared<std::vector<helpers::Column<Float>>>(
      fct::collect::vector<helpers::Column<Float>>(
          iota | VIEWS::transform(to_column)));
}

// ----------------------------------------------------------------------------

std::shared_ptr<std::vector<std::optional<helpers::FeatureContainer>>>
Maker::transform_make_subcontainers(const MakerParams& _params) {
  const auto& placeholder = _params.placeholder_;

  const auto make_subcontainer =
      [&_params](const size_t _i) -> std::optional<helpers::FeatureContainer> {
    if (!_params.fast_prop_container_->subcontainers(_i)) {
      return std::nullopt;
    }
    const auto params = make_params(_params, _i);
    return transform(params);
  };

  const auto iota = fct::iota<size_t>(0, placeholder.joined_tables_.size());

  return std::make_shared<
      std::vector<std::optional<helpers::FeatureContainer>>>(
      fct::collect::vector<std::optional<helpers::FeatureContainer>>(
          iota | VIEWS::transform(make_subcontainer)));
}

// ----------------------------------------------------------------------------

}  // namespace subfeatures
}  // namespace fastprop

#include "helpers/TableHolder.hpp"

#include <stdexcept>

// ----------------------------------------------------------------------------

#include "stl/stl.hpp"

// ----------------------------------------------------------------------------

#include "helpers/Macros.hpp"

// ----------------------------------------------------------------------------

namespace helpers {

TableHolder::TableHolder(const TableHolderParams& _params)
    : main_tables_(parse_main_tables(_params)),
      peripheral_tables_(parse_peripheral_tables(_params)),
      propositionalization_(parse_propositionalization(_params.placeholder_,
                                                       main_tables_.size())),
      subtables_(parse_subtables(_params)) {
  assert_true(main_tables_.size() == peripheral_tables_.size());
  assert_true(main_tables_.size() == propositionalization_.size());
  assert_true(main_tables_.size() == subtables_.size());
}

// ----------------------------------------------------------------------------

TableHolder::~TableHolder() = default;

// ----------------------------------------------------------------------------

std::vector<DataFrame> TableHolder::add_text_fields_to_peripheral_tables(
    const std::vector<DataFrame>& _original, const TableHolderParams& _params) {
  auto result = _original;

  const auto is_relevant_text_field = [&_params](const auto _df) -> bool {
    return _df.name().find(_params.population_.name() + Macros::text_field()) !=
           std::string::npos;
  };

  for (size_t i = 0; i < _params.peripheral_.size(); ++i) {
    const auto& df = _params.peripheral_.at(i);

    if (!is_relevant_text_field(df)) {
      continue;
    }

    const auto row_indices =
        _params.row_index_container_
            ? _params.row_index_container_->peripheral().at(i)
            : RowIndices();

    const auto word_indices =
        _params.word_index_container_
            ? _params.word_index_container_->peripheral().at(i)
            : WordIndices();

    const auto params = DataFrameParams{.categoricals_ = df.categoricals_,
                                        .discretes_ = df.discretes_,
                                        .indices_ = df.indices_,
                                        .join_keys_ = df.join_keys_,
                                        .name_ = df.name_,
                                        .numericals_ = df.numericals_,
                                        .row_indices_ = row_indices,
                                        .targets_ = df.targets_,
                                        .text_ = df.text_,
                                        .time_stamps_ = df.time_stamps_,
                                        .ts_index_ = df.ts_index_,
                                        .word_indices_ = word_indices};

    const auto text_field = DataFrame(params);

    result.push_back(text_field);
  }

  return result;
}

// ----------------------------------------------------------------------------

size_t TableHolder::count_text(const std::vector<DataFrame>& _peripheral) {
  const auto is_text = [](const DataFrame& _df) -> bool {
    return _df.name_.find(helpers::Macros::text_field()) != std::string::npos;
  };

  return RANGES::count_if(_peripheral, is_text);
}

// ----------------------------------------------------------------------------

size_t TableHolder::find_peripheral_ix(
    const std::vector<std::string>& _peripheral_names,
    const std::string& _name) {
  const auto it =
      std::find(_peripheral_names.begin(), _peripheral_names.end(), _name);

  if (it == _peripheral_names.end()) {
    throw std::runtime_error("Peripheral table named '" + _name +
                             "' not found!");
  }

  return static_cast<size_t>(std::distance(_peripheral_names.begin(), it));
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<size_t>> TableHolder::make_subrows(
    const DataFrameView& _population_subview,
    const DataFrame& _peripheral_subview) {
  std::set<size_t> rows;

  for (size_t i = 0; i < _population_subview.nrows(); ++i) {
    const auto jk = _population_subview.join_key(i);

    if (_peripheral_subview.has(jk)) {
      const auto [begin, end] = _peripheral_subview.find(jk);

      for (auto it = begin; it != end; ++it) {
        rows.insert(*it);
      }
    }
  }

  return std::make_shared<const std::vector<size_t>>(rows.begin(), rows.end());
}

// ----------------------------------------------------------------------------

std::vector<DataFrameView> TableHolder::parse_main_tables(
    const TableHolderParams& _params) {
  assert_true(_params.placeholder_.joined_tables_.size() ==
              _params.placeholder_.join_keys_used_.size());

  assert_true(_params.placeholder_.joined_tables_.size() ==
              _params.placeholder_.time_stamps_used_.size());

  const auto row_indices = _params.row_index_container_
                               ? _params.row_index_container_->population()
                               : RowIndices();

  const auto word_indices = _params.word_index_container_
                                ? _params.word_index_container_->population()
                                : WordIndices();

  const auto features = _params.feature_container_
                            ? _params.feature_container_->features()
                            : AdditionalColumns();

  std::vector<DataFrameView> result;

  for (size_t i = 0; i < _params.placeholder_.joined_tables_.size(); ++i) {
    const auto params = CreateSubviewParams{
        .additional_ = features,
        .join_key_ = _params.placeholder_.join_keys_used_.at(i),
        .make_staging_table_colname_ = _params.make_staging_table_colname_,
        .row_indices_ = row_indices,
        .time_stamp_ = _params.placeholder_.time_stamps_used_.at(i),
        .word_indices_ = word_indices};

    result.push_back(_params.population_.create_subview(params));
  }

  const auto is_relevant_text_field = [&_params](const DataFrame& df) -> bool {
    return df.name_.find(_params.population_.name() + Macros::text_field()) !=
           std::string::npos;
  };

  auto relevant_text_fields =
      _params.peripheral_ | VIEWS::filter(is_relevant_text_field);

  const auto num_fields =
      std::distance(relevant_text_fields.begin(), relevant_text_fields.end());

  for (Int i = 0; i < num_fields; ++i) {
    const auto params = CreateSubviewParams{
        .additional_ = features,
        .join_key_ = Macros::rowid(),
        .make_staging_table_colname_ = _params.make_staging_table_colname_,
        .row_indices_ = row_indices,
        .word_indices_ = word_indices};

    result.push_back(_params.population_.create_subview(params));
  }

  return result;
}

// ----------------------------------------------------------------------------

std::vector<DataFrame> TableHolder::parse_peripheral_tables(
    const TableHolderParams& _params) {
#ifndef NDEBUG
  const size_t num_text = count_text(_params.peripheral_);
#endif  // NDEBUG

  assert_true(_params.placeholder_.joined_tables_.size() ==
              _params.placeholder_.other_join_keys_used_.size());

  assert_true(_params.placeholder_.joined_tables_.size() ==
              _params.placeholder_.other_time_stamps_used_.size());

  assert_true(_params.peripheral_.size() > 0);

  assert_true(_params.peripheral_names_.size() + num_text ==
              _params.peripheral_.size());

  assert_true(!_params.row_index_container_ ||
              _params.peripheral_.size() ==
                  _params.row_index_container_->peripheral().size());

  assert_true(!_params.word_index_container_ ||
              _params.peripheral_.size() ==
                  _params.word_index_container_->peripheral().size());

  const auto make_additional_columns = [&_params](const size_t _i) {
    return TableHolder::make_additional_columns(_params.feature_container_, _i);
  };

  std::vector<DataFrame> result;

  for (size_t i = 0; i < _params.placeholder_.joined_tables_.size(); ++i) {
    const auto j =
        find_peripheral_ix(_params.peripheral_names_,
                           _params.placeholder_.joined_tables_.at(i).name_);

    const auto row_indices =
        _params.row_index_container_
            ? _params.row_index_container_->peripheral().at(j)
            : RowIndices();

    const auto word_indices =
        _params.word_index_container_
            ? _params.word_index_container_->peripheral().at(j)
            : WordIndices();

    const auto additional = make_additional_columns(i);

    const auto jk_population = _params.placeholder_.join_keys_used_.at(i);

    const auto population_join_keys =
        _params.population_.join_key_col(jk_population);

    const auto params = CreateSubviewParams{
        .additional_ = additional,
        .allow_lagged_targets_ =
            _params.placeholder_.allow_lagged_targets_.at(i),
        .join_key_ = _params.placeholder_.other_join_keys_used_.at(i),
        .make_staging_table_colname_ = _params.make_staging_table_colname_,
        .population_join_keys_ = population_join_keys,
        .row_indices_ = row_indices,
        .time_stamp_ = _params.placeholder_.other_time_stamps_used_.at(i),
        .upper_time_stamp_ = _params.placeholder_.upper_time_stamps_used_.at(i),
        .word_indices_ = word_indices};

    result.push_back(_params.peripheral_.at(j).create_subview(params));
  }

  return add_text_fields_to_peripheral_tables(result, _params);
}

// ----------------------------------------------------------------------------

std::vector<bool> TableHolder::parse_propositionalization(
    const Placeholder& _placeholder, const size_t _expected_size) {
  auto propositionalization = _placeholder.propositionalization();

  assert_true(propositionalization.size() <= _expected_size);

  for (size_t i = propositionalization.size(); i < _expected_size; ++i) {
    propositionalization.push_back(false);
  }

  return propositionalization;
}

// ----------------------------------------------------------------------------

std::vector<Column<Float>> TableHolder::make_additional_columns(
    const std::optional<const FeatureContainer>& _feature_container,
    const size_t _i) {
  std::vector<Column<Float>> additional;

  if (_feature_container && _feature_container->subcontainers(_i)) {
    for (const auto& col : _feature_container->subcontainers(_i)->features()) {
      additional.push_back(col);
    }
  }

  return additional;
}

// ----------------------------------------------------------------------------

DataFrameView TableHolder::make_output(const TableHolderParams& _params,
                                       const size_t _i, const size_t _j) {
  const auto population_params = CreateSubviewParams{
      .join_key_ = _params.placeholder_.join_keys_used_.at(_i),
      .make_staging_table_colname_ = _params.make_staging_table_colname_,
      .time_stamp_ = _params.placeholder_.time_stamps_used_.at(_i)};

  const auto population_subview =
      _params.population_.create_subview(population_params);

  const auto peripheral_params = CreateSubviewParams{
      .allow_lagged_targets_ =
          _params.placeholder_.allow_lagged_targets_.at(_i),
      .join_key_ = _params.placeholder_.other_join_keys_used_.at(_i),
      .make_staging_table_colname_ = _params.make_staging_table_colname_,
      .population_join_keys_ =
          population_subview.join_key_col(population_params.join_key_),
      .time_stamp_ = _params.placeholder_.other_time_stamps_used_.at(_i),
      .upper_time_stamp_ = _params.placeholder_.upper_time_stamps_used_.at(_i)};

  const auto peripheral_subview =
      _params.peripheral_.at(_j).create_subview(peripheral_params);

  return DataFrameView(_params.peripheral_.at(_j),
                       make_subrows(population_subview, peripheral_subview));
}

// ----------------------------------------------------------------------------

std::vector<std::optional<TableHolder>> TableHolder::parse_subtables(
    const TableHolderParams& _params) {
#ifndef NDEBUG
  const size_t num_text = count_text(_params.peripheral_);
#endif  // NDEBUG

  assert_true(_params.peripheral_.size() > 0);

  assert_true(_params.peripheral_names_.size() + num_text ==
              _params.peripheral_.size());

  assert_true(!_params.row_index_container_ ||
              _params.row_index_container_->peripheral().size() ==
                  _params.peripheral_.size());

  assert_true(!_params.word_index_container_ ||
              _params.word_index_container_->peripheral().size() ==
                  _params.peripheral_.size());

  const auto make_output = [&_params](const size_t i,
                                      const size_t j) -> DataFrameView {
    return TableHolder::make_output(_params, i, j);
  };

  const auto make_row_index_container =
      [&_params](const size_t j) -> std::optional<RowIndexContainer> {
    if (_params.row_index_container_) {
      assert_true(j < _params.row_index_container_->peripheral().size());
      return RowIndexContainer(_params.row_index_container_->peripheral().at(j),
                               _params.row_index_container_->peripheral());
    }

    return std::nullopt;
  };

  const auto make_word_index_container =
      [&_params](const size_t j) -> std::optional<WordIndexContainer> {
    if (_params.word_index_container_) {
      assert_true(j < _params.word_index_container_->peripheral().size());
      return WordIndexContainer(
          _params.word_index_container_->peripheral().at(j),
          _params.word_index_container_->peripheral());
    }

    return std::nullopt;
  };

  std::vector<std::optional<TableHolder>> result;

  for (size_t i = 0; i < _params.placeholder_.joined_tables_.size(); ++i) {
    const auto& joined = _params.placeholder_.joined_tables_.at(i);

    if (joined.joined_tables_.size() == 0) {
      result.push_back(std::nullopt);
      continue;
    }

    const auto j = find_peripheral_ix(_params.peripheral_names_, joined.name_);

    const auto output = make_output(i, j);

    const auto row_index_container = make_row_index_container(j);

    const auto word_index_container = make_word_index_container(j);

    const auto params = TableHolderParams{
        .make_staging_table_colname_ = _params.make_staging_table_colname_,
        .peripheral_ = _params.peripheral_,
        .peripheral_names_ = _params.peripheral_names_,
        .placeholder_ = joined,
        .population_ = output,
        .row_index_container_ = row_index_container,
        .word_index_container_ = word_index_container};

    result.push_back(std::make_optional<TableHolder>(params));
  }

  const auto is_relevant_text_field = [&_params](const DataFrame& df) -> bool {
    return df.name_.find(_params.population_.name() + Macros::text_field()) !=
           std::string::npos;
  };

  auto relevant_text_fields =
      _params.peripheral_ | VIEWS::filter(is_relevant_text_field);

  const auto num_fields =
      std::distance(relevant_text_fields.begin(), relevant_text_fields.end());

  for (Int i = 0; i < num_fields; ++i) {
    result.push_back(std::nullopt);
  }

  return result;
}

// ----------------------------------------------------------------------------

WordIndexContainer TableHolder::word_indices() const {
  if (main_tables_.size() == 0) {
    return WordIndexContainer({}, {});
  }

  const auto extract_word_indices = [](const DataFrame& df) {
    return df.word_indices_;
  };

  const auto population = extract_word_indices(main_tables_.at(0).df());

  auto range = peripheral_tables_ | VIEWS::transform(extract_word_indices);

  const auto peripheral = stl::collect::vector<WordIndices>(range);

  return WordIndexContainer(population, peripheral);
}

// ----------------------------------------------------------------------------
}  // namespace helpers

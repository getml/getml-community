// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ViewParser.hpp"

#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"

namespace engine {
namespace handlers {

void ViewParser::add_column(const ViewOp& _cmd,
                            containers::DataFrame* _df) const {
  if (!_cmd.get<"added_">()) {
    return;
  }

  const auto handle = [this, &_cmd, _df](const auto& _json_col) {
    using Type = std::decay_t<decltype(_json_col)>;

    const auto& added = *_cmd.get<"added_">();

    const auto& name = added.get<"name_">();

    const auto& role = added.get<"role_">();

    const auto& subroles = added.get<"subroles_">();

    const auto& unit = added.get<"unit_">();

    if constexpr (std::is_same<Type,
                               commands::FloatColumnOrFloatColumnView>()) {
      const auto column_view =
          FloatOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(_json_col);

      auto col = column_view.to_column(0, _df->nrows(), true);

      col.set_name(name);

      col.set_subroles(subroles);

      col.set_unit(unit);

      _df->add_float_column(col, role);
    } else {
      const auto column_view =
          StringOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(_json_col);

      const auto vec = column_view.to_vector(0, _df->nrows(), true);

      assert_true(vec);

      if (role == containers::DataFrame::ROLE_CATEGORICAL ||
          role == containers::DataFrame::ROLE_JOIN_KEY) {
        add_int_column_to_df(name, role, subroles, unit, *vec, _df);
      }

      if (role == containers::DataFrame::ROLE_UNUSED ||
          role == containers::DataFrame::ROLE_TEXT ||
          role == containers::DataFrame::ROLE_UNUSED_STRING) {
        add_string_column_to_df(name, role, subroles, unit, *vec, _df);
      }
    }
  };

  std::visit(handle, _cmd.get<"added_">()->get<"col_">());
}

// ------------------------------------------------------------------------

void ViewParser::add_int_column_to_df(const std::string& _name,
                                      const std::string& _role,
                                      const std::vector<std::string>& _subroles,
                                      const std::string& _unit,
                                      const std::vector<strings::String>& _vec,
                                      containers::DataFrame* _df) const {
  const auto encoding = _role == containers::DataFrame::ROLE_CATEGORICAL
                            ? categories_
                            : join_keys_encoding_;

  auto col = containers::Column<Int>(_df->pool(), _vec.size());

  for (size_t i = 0; i < _vec.size(); ++i) {
    col[i] = (*encoding)[_vec[i]];
  }

  col.set_name(_name);

  col.set_subroles(_subroles);

  col.set_unit(_unit);

  _df->add_int_column(col, _role);

  if (_role == containers::DataFrame::ROLE_JOIN_KEY) {
    _df->create_indices();
  }
}

// ----------------------------------------------------------------------------

void ViewParser::add_string_column_to_df(
    const std::string& _name, const std::string& _role,
    const std::vector<std::string>& _subroles, const std::string& _unit,
    const std::vector<strings::String>& _vec,
    containers::DataFrame* _df) const {
  auto col = containers::Column<strings::String>(_df->pool());

  for (size_t i = 0; i < _vec.size(); ++i) {
    col.push_back(_vec[i]);
  }

  col.set_name(_name);

  col.set_subroles(_subroles);

  col.set_unit(_unit);

  _df->add_string_column(col, _role);
}

// ----------------------------------------------------------------------------

void ViewParser::drop_columns(const ViewOp& _cmd,
                              containers::DataFrame* _df) const {
  if (!_cmd.get<"dropped_">()) {
    return;
  }

  const auto& dropped = *_cmd.get<"dropped_">();

  for (const auto& name : dropped) {
    _df->remove_column(name);
  }
}

// ----------------------------------------------------------------------------

typename ViewParser::ColumnViewVariant ViewParser::make_column_view(
    Poco::JSON::Object::Ptr _ptr) const {
  assert_true(_ptr);

  const auto type = JSON::get_value<std::string>(*_ptr, "type_");

  if (type == STRING_COLUMN || type == STRING_COLUMN_VIEW) {
    return StringOpParser(categories_, join_keys_encoding_, data_frames_)
        .parse(*_ptr);
  }

  return FloatOpParser(categories_, join_keys_encoding_, data_frames_)
      .parse(*_ptr);
}

// ----------------------------------------------------------------------------

Poco::JSON::Array::Ptr ViewParser::make_data(
    const std::vector<std::vector<std::string>>& _string_vectors) const {
  auto data = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

  if (_string_vectors.size() == 0) {
    return data;
  }

  for (size_t i = 0; i < _string_vectors.at(0).size(); ++i) {
    auto row = Poco::JSON::Array::Ptr(new Poco::JSON::Array());

    for (size_t j = 0; j < _string_vectors.size(); ++j) {
      if (i == 0 &&
          _string_vectors.at(j).size() != _string_vectors.at(0).size()) {
        throw std::runtime_error(
            "ViewParser: Resulting columns do not all have "
            "the same length!");
      }

      row->add(_string_vectors.at(j).at(i));
    }

    data->add(row);
  }

  return data;
}

// ----------------------------------------------------------------------------

std::optional<size_t> ViewParser::make_nrows(
    const std::vector<ColumnViewVariant>& _column_views,
    const size_t _force) const {
  const auto has_nrows = [](const ColumnViewVariant& _column_view) -> bool {
    if (std::holds_alternative<containers::ColumnView<Float>>(_column_view)) {
      const auto float_col =
          std::get<containers::ColumnView<Float>>(_column_view);
      return std::holds_alternative<size_t>(float_col.nrows());
    }
    const auto str_col =
        std::get<containers::ColumnView<strings::String>>(_column_view);
    return std::holds_alternative<size_t>(str_col.nrows());
  };

  const auto get_nrows = [](const ColumnViewVariant& _column_view) -> size_t {
    if (std::holds_alternative<containers::ColumnView<Float>>(_column_view)) {
      const auto float_col =
          std::get<containers::ColumnView<Float>>(_column_view);
      return std::get<size_t>(float_col.nrows());
    }

    const auto str_col =
        std::get<containers::ColumnView<strings::String>>(_column_view);
    return std::get<size_t>(str_col.nrows());
  };

  const auto calc_nrows = [](const ColumnViewVariant& _column_view) -> size_t {
    if (std::holds_alternative<containers::ColumnView<Float>>(_column_view)) {
      const auto float_col =
          std::get<containers::ColumnView<Float>>(_column_view)
              .to_column(0, std::nullopt, false);
      return float_col.nrows();
    }

    const auto str_col =
        std::get<containers::ColumnView<strings::String>>(_column_view)
            .to_column(0, std::nullopt, false);
    return str_col.nrows();
  };

  const auto it =
      std::find_if(_column_views.begin(), _column_views.end(), has_nrows);

  if (it != _column_views.end()) {
    return std::make_optional(get_nrows(*it));
  }

  if (!_force) {
    return std::nullopt;
  }

  const auto to_float = [](const size_t _nrows) -> Float {
    return static_cast<Float>(_nrows);
  };

  const auto calc_nrows_float = fct::compose(calc_nrows, to_float);

  auto range = _column_views | VIEWS::transform(calc_nrows_float);

  return static_cast<size_t>(
      helpers::Aggregations::assert_equal(range.begin(), range.end()));
}

// ----------------------------------------------------------------------------

std::vector<std::string> ViewParser::make_string_vector(
    const size_t _start, const size_t _length,
    const ColumnViewVariant& _column_view) const {
  const bool is_string_column =
      std::holds_alternative<containers::ColumnView<strings::String>>(
          _column_view);

  if (is_string_column) {
    const auto str_col =
        std::get<containers::ColumnView<strings::String>>(_column_view);

    const auto to_str = [](const strings::String& _str) -> std::string {
      return _str.str();
    };

    return fct::collect::vector<std::string>(
        *str_col.to_vector(_start, _length, false) | VIEWS::transform(to_str));
  }

  const auto float_col = std::get<containers::ColumnView<Float>>(_column_view);

  const auto float_vec = *float_col.to_vector(_start, _length, false);

  if (float_col.unit().find("time stamp") != std::string::npos) {
    return fct::collect::vector<std::string>(
        float_vec | VIEWS::transform(io::Parser::ts_to_string));
  }

  const auto to_string = [](const Float _val) {
    return io::Parser::to_string(_val);
  };

  return fct::collect::vector<std::string>(float_vec |
                                           VIEWS::transform(to_string));
}

// ----------------------------------------------------------------------------

Poco::JSON::Object ViewParser::get_content(
    const size_t _draw, const size_t _start, const size_t _length,
    const bool _force_nrows, const Poco::JSON::Array::Ptr& _cols) const {
  const auto get_object = [_cols](const size_t _i) {
    return _cols->getObject(_i);
  };

  const auto to_column_view =
      [this](Poco::JSON::Object::Ptr _ptr) -> ColumnViewVariant {
    return make_column_view(_ptr);
  };

  const auto to_string_vector =
      [this, _start, _length](
          const ColumnViewVariant& _column_view) -> std::vector<std::string> {
    return make_string_vector(_start, _length, _column_view);
  };

  const auto make_column_view = fct::compose(get_object, to_column_view);

  const auto iota = fct::iota<size_t>(0, _cols->size());

  const auto column_views = fct::collect::vector<ColumnViewVariant>(
      iota | VIEWS::transform(make_column_view));

  const auto string_vectors = fct::collect::vector<std::vector<std::string>>(
      column_views | VIEWS::transform(to_string_vector));

  const auto data = make_data(string_vectors);

  const auto nrows = make_nrows(column_views, _force_nrows);

  Poco::JSON::Object obj;

  obj.set("draw", _draw);

  obj.set("data", data);

  if (nrows) {
    obj.set("recordsTotal", *nrows);

    obj.set("recordsFiltered", *nrows);
  }

  return obj;
}

// ----------------------------------------------------------------------------

containers::DataFrame ViewParser::parse(
    const commands::DataFrameOrView& _cmd) const {
  const auto handle = [this](const auto& _cmd) -> containers::DataFrame {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, DataFrameOp>()) {
      const auto name = fct::get<"name_">(_cmd);
      return utils::Getter::get(name, *data_frames_);
    } else {
      const auto& base = *fct::get<"base_">(_cmd);

      auto df = parse(base);

      add_column(_cmd, &df);

      drop_columns(_cmd, &df);

      subselection(_cmd, &df);

      // TODO: Handle the build history.
      /*df.set_build_history(
          Poco::JSON::Object::Ptr(new Poco::JSON::Object(_obj)));*/

      return df;
    }
  };

  return std::visit(handle, _cmd.val_);
}

// ----------------------------------------------------------------------------

std::tuple<containers::DataFrame, std::vector<containers::DataFrame>,
           std::optional<containers::DataFrame>>
ViewParser::parse_all(const commands::DataFramesOrViews& _cmd) const {
  const auto to_df = [this](const auto& _obj) -> containers::DataFrame {
    return parse(_obj);
  };

  const auto population_obj = _cmd.get<"population_df_">();

  const auto peripheral_objs = _cmd.get<"peripheral_dfs_">();

  const auto validation_obj = _cmd.get<"validation_df_">();

  const auto population = to_df(population_obj);

  const auto peripheral = fct::collect::vector<containers::DataFrame>(
      peripheral_objs | VIEWS::transform(to_df));

  const auto validation =
      validation_obj
          ? std::make_optional<containers::DataFrame>(to_df(*validation_obj))
          : std::optional<containers::DataFrame>();

  return std::make_tuple(population, peripheral, validation);
}

// ----------------------------------------------------------------------------

void ViewParser::subselection(const ViewOp& _cmd,
                              containers::DataFrame* _df) const {
  if (!_cmd.get<"subselection_">()) {
    return;
  }

  const auto handle = [this, &_cmd, _df](const auto& _json_col) {
    using Type = std::decay_t<decltype(_json_col)>;

    if constexpr (std::is_same<Type, commands::BooleanColumnView>()) {
      const auto column_view =
          BoolOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(_json_col);

      const auto data_ptr = column_view.to_vector(0, _df->nrows(), true);

      assert_true(data_ptr);

      _df->where(*data_ptr);
    } else {
      const auto data_ptr =
          FloatOpParser(categories_, join_keys_encoding_, data_frames_)
              .parse(_json_col)
              .to_vector(0, std::nullopt, false);

      assert_true(data_ptr);

      const auto& key_float = *data_ptr;

      auto key = std::vector<size_t>(key_float.size());

      for (size_t i = 0; i < key_float.size(); ++i) {
        const auto ix_float = key_float[i];

        if (ix_float < 0.0) {
          throw std::runtime_error(
              "Index on a numerical subselection cannot be "
              "smaller than zero!");
        }

        const auto ix = static_cast<size_t>(ix_float);

        if (ix >= _df->nrows()) {
          throw std::runtime_error(
              "Index on a numerical subselection out of "
              "bounds!");
        }

        key[i] = ix;
      }

      _df->sort_by_key(key);
    }
  };

  return std::visit(handle, *_cmd.get<"subselection_">());
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

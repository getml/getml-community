// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_VIEWPARSER_HPP_
#define ENGINE_HANDLERS_VIEWPARSER_HPP_

#include "commands/DataFrameCommand.hpp"
#include "commands/DataFrameOrView.hpp"
#include "commands/DataFramesOrViews.hpp"
#include "containers/ColumnView.hpp"
#include "containers/ViewContent.hpp"
#include "engine/handlers/ArrowHandler.hpp"

#include <Poco/Net/StreamSocket.h>
#include <rfl/json/read.hpp>

#include <map>
#include <memory>
#include <string>

namespace engine {
namespace handlers {

class ViewParser {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  static constexpr const char* FLOAT_COLUMN_VIEW =
      containers::Column<bool>::FLOAT_COLUMN_VIEW;
  static constexpr const char* STRING_COLUMN_VIEW =
      containers::Column<bool>::STRING_COLUMN_VIEW;
  static constexpr const char* BOOLEAN_COLUMN_VIEW =
      containers::Column<bool>::BOOLEAN_COLUMN_VIEW;

  typedef std::variant<containers::ColumnView<strings::String>,
                       containers::ColumnView<Float>>
      ColumnViewVariant;
  typedef typename commands::DataFrameOrView::DataFrameOp DataFrameOp;
  typedef typename commands::DataFrameCommand::ViewCol ViewCol;
  typedef typename commands::DataFrameOrView::ViewOp ViewOp;

 public:
  ViewParser(const rfl::Ref<containers::Encoding>& _categories,
             const rfl::Ref<containers::Encoding>& _join_keys_encoding,
             const rfl::Ref<const std::map<std::string, containers::DataFrame>>&
                 _data_frames,
             const config::Options& _options);

  ~ViewParser() = default;

 public:
  /// Returns the content of a view.
  containers::ViewContent get_content(const size_t _draw, const size_t _start,
                                      const size_t _length,
                                      const bool _force_nrows,
                                      const std::vector<ViewCol>& _cols) const;

  /// Parses a view and turns it into a DataFrame.
  containers::DataFrame parse(const commands::DataFrameOrView& _cmd) const;

  /// Returns the population and peripheral data frames.
  std::tuple<containers::DataFrame, std::vector<containers::DataFrame>,
             std::optional<containers::DataFrame>>
  parse_all(const commands::DataFramesOrViews& _cmd) const;

  /// Returns the population and peripheral data frames.
  auto parse_all(const std::string& _cmd) const {
    const auto cmd = rfl::json::read<commands::DataFramesOrViews>(_cmd).value();
    return parse_all(cmd);
  }

  /// Expresses the View as a arrow::Table.
  std::shared_ptr<arrow::Table> to_table(
      const commands::DataFrameOrView& _cmd) {
    return ArrowHandler(categories_, join_keys_encoding_, options_)
        .df_to_table(parse(_cmd));
  }

 private:
  /// Adds a new column to the data frame.
  void add_column(const ViewOp& _cmd, containers::DataFrame* _df) const;

  /// Adds a new int column to the data frame.
  void add_int_column_to_df(const std::string& _name, const std::string& _role,
                            const std::vector<std::string>& _subroles,
                            const std::string& _unit,
                            const std::vector<strings::String>& _vec,
                            containers::DataFrame* _df) const;

  /// Adds a new string column to the DataFrame.
  void add_string_column_to_df(const std::string& _name,
                               const std::string& _role,
                               const std::vector<std::string>& _subroles,
                               const std::string& _unit,
                               const std::vector<strings::String>& _vec,
                               containers::DataFrame* _df) const;

  /// Drop a set of columns.
  void drop_columns(const ViewOp& _cmd, containers::DataFrame* _df) const;

  /// Generates a column view from a ViewCol - used by
  /// get_content(...).
  ColumnViewVariant make_column_view(const ViewCol& _col) const;

  /// Generates nrows for get_content(...).
  std::optional<size_t> make_nrows(
      const std::vector<ColumnViewVariant>& _column_views,
      const size_t _force) const;

  /// Extracts a string vector from a column view - used by get_content(...).
  std::vector<std::string> make_string_vector(
      const size_t _start, const size_t _length,
      const ColumnViewVariant& _column_view) const;

  /// Applies the subselection.
  void subselection(const ViewOp& _cmd, containers::DataFrame* _df) const;

 private:
  /// Encodes the categories used.
  const rfl::Ref<containers::Encoding> categories_;

  /// The DataFrames this is based on.
  const rfl::Ref<const std::map<std::string, containers::DataFrame>>
      data_frames_;

  /// Encodes the join keys used.
  const rfl::Ref<containers::Encoding> join_keys_encoding_;

  /// Settings for the engine and the monitor
  const config::Options options_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_VIEWPARSER_HPP_

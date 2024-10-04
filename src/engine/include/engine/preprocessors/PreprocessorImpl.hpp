// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_

#include <rfl/Ref.hpp>
#include <vector>

#include "engine/preprocessors/data_model_checking.hpp"
#include "helpers/ColumnDescription.hpp"

namespace engine {
namespace preprocessors {

struct PreprocessorImpl {
  using MarkerType = typename helpers::ColumnDescription::MarkerType;

  /// Retrieves the column names of all column descriptions that match the
  /// marker and table.
  static std::vector<std::string> retrieve_names(
      const MarkerType _marker, const size_t _table,
      const std::vector<rfl::Ref<helpers::ColumnDescription>>& _desc);

  /// Adds a new column description.
  static void add(const MarkerType _marker, const size_t _table,
                  const std::string& _name,
                  std::vector<rfl::Ref<helpers::ColumnDescription>>* _desc) {
    _desc->push_back(rfl::Ref<helpers::ColumnDescription>::make(
        _marker, std::to_string(_table), _name));
  }

  /// Determines whether a categorical column generates warning.
  static bool has_warnings(const containers::Column<Int>& _col) {
    auto warner = communication::Warner();
    data_model_checking::check_categorical_column(_col, "DUMMY", &warner);
    return (warner.warnings().size() != 0);
  }

  /// Determines whether a categorical column generates warning.
  static bool has_warnings(const containers::Column<Float>& _col) {
    auto warner = communication::Warner();
    data_model_checking::check_float_column(_col, "DUMMY", &warner);
    return (warner.warnings().size() != 0);
  }
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_

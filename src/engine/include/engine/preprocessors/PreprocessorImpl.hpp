#ifndef ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_
#define ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/helpers.hpp"

// ----------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/preprocessors/DataModelChecker.hpp"
#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/TransformParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace preprocessors {
// ----------------------------------------------------

struct PreprocessorImpl {
  /// Generates a vector of column of descriptions from an array.
  static std::vector<std::shared_ptr<helpers::ColumnDescription>> from_array(
      const Poco::JSON::Array::Ptr& _arr);

  /// Retrieves the column names of all column descriptions that match the
  /// marker and table.
  static std::vector<std::string> retrieve_names(
      const std::string& _marker, const size_t _table,
      const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc);

  /// Transforms a vector of columns descriptions to an array.
  static Poco::JSON::Array::Ptr to_array(
      const std::vector<std::shared_ptr<helpers::ColumnDescription>>& _desc);

  /// Adds a new column description.
  static void add(
      const std::string& _marker, const size_t _table, const std::string& _name,
      std::vector<std::shared_ptr<helpers::ColumnDescription>>* _desc) {
    _desc->push_back(std::make_shared<helpers::ColumnDescription>(
        _marker, std::to_string(_table), _name));
  }

  /// Determines whether a categorical column generates warning.
  static bool has_warnings(const containers::Column<Int>& _col) {
    auto warner = communication::Warner();
    DataModelChecker::check_categorical_column(_col, "DUMMY", &warner);
    return (warner.warnings().size() != 0);
  }

  /// Determines whether a categorical column generates warning.
  static bool has_warnings(const containers::Column<Float>& _col) {
    auto warner = communication::Warner();
    DataModelChecker::check_float_column(_col, "DUMMY", &warner);
    return (warner.warnings().size() != 0);
  }
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_PREPROCESSORIMPL_HPP_


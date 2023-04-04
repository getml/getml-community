// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_
#define ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_

#include <memory>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "commands/DataModel.hpp"
#include "debug/debug.hpp"
#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/containers/containers.hpp"
#include "fct/Ref.hpp"
#include "helpers/helpers.hpp"

namespace engine {
namespace pipelines {

class PlaceholderMaker {
 private:
  using RelationshipLiteral = typename commands::DataModel::RelationshipLiteral;

 public:
  /// Creates the placeholder, including transforming memory into upper time
  /// stamps.
  static fct::Ref<const helpers::Placeholder> make_placeholder(
      const commands::DataModel& _data_model, const std::string& _alias,
      const std::shared_ptr<size_t> _num_alias = nullptr,
      const bool _is_population = true);

  /// Returns a list of all peripheral tables used in the placeholder.
  static std::vector<std::string> make_peripheral(
      const helpers::Placeholder& _placeholder);

  /// Generates the name for the time stamp that is produced using
  /// memory.
  static std::string make_ts_name(const std::string& _ts_used,
                                  const Float _diff);

 private:
  template <typename T>
  static void append(const std::vector<T>& _vec2, std::vector<T>* _vec1);

  static void extract_joined_tables(const helpers::Placeholder& _placeholder,
                                    std::set<std::string>* _names);

  static std::vector<std::string> handle_horizon(
      const commands::DataModel& _data_model,
      const std::vector<Float>& _horizon);

  static fct::Ref<const helpers::Placeholder> handle_joined_tables(
      const commands::DataModel& _data_model, const std::string& _alias,
      const std::shared_ptr<size_t> _num_alias,
      const std::vector<commands::DataModel>& _joined_tables,
      const std::vector<RelationshipLiteral>& _relationship,
      const std::vector<std::string>& _other_time_stamps_used,
      const std::vector<std::string>& _upper_time_stamps_used,
      const bool _is_population);

  static std::vector<std::string> handle_memory(
      const commands::DataModel& _data_model,
      const std::vector<Float>& _horizon, const std::vector<Float>& _memory);

  static std::vector<std::string> make_colnames(
      const std::string& _tname, const std::string& _alias,
      const std::vector<std::string>& _old_colnames);

 private:
  static bool is_to_many(const RelationshipLiteral& _relationship) {
    return (_relationship.value() ==
                RelationshipLiteral::value_of<"many-to-many">() ||
            _relationship.value() ==
                RelationshipLiteral::value_of<"propositionalization">() ||
            _relationship.value() ==
                RelationshipLiteral::value_of<"one-to-many">());
  }

  static std::string make_alias(const std::shared_ptr<size_t> _num_alias) {
    assert_true(_num_alias);
    auto& num_alias = *_num_alias;
    return "t" + std::to_string(++num_alias);
  }
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <typename T>
void PlaceholderMaker::append(const std::vector<T>& _vec2,
                              std::vector<T>* _vec1) {
  for (const auto& elem : _vec2) {
    _vec1->push_back(elem);
  }
}

}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_PLACEHOLDERMAKER_HPP_

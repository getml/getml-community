// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/utils/SQLDependencyTracker.hpp"

#include "helpers/StringSplitter.hpp"
#include "transpilation/SQLGenerator.hpp"

#include <rfl/json/write.hpp>
#include <rfl/make_named_tuple.hpp>

#include <cstddef>
#include <filesystem>
#include <fstream>
#include <ranges>

namespace engine {
namespace utils {

SQLDependencyTracker::SQLDependencyTracker(const std::string& _folder)
    : folder_(_folder) {}

typename SQLDependencyTracker::SQLDependency
SQLDependencyTracker::find_dependencies(const Tuples& _tuples,
                                        const size_t _i) const {
  const auto& sql_code = std::get<2>(_tuples.at(_i));

  const auto is_dependency = [&sql_code, &_tuples](const size_t _j) -> bool {
    const auto& table_name = std::get<0>(_tuples.at(_j));
    return sql_code.find("\"" + table_name + "\"") != std::string::npos;
  };

  const auto dependencies = std::views::iota(0uz, _i) |
                            std::views::filter(is_dependency) |
                            std::ranges::to<std::vector>();

  return rfl::make_field<"table_name_">(std::get<0>(_tuples.at(_i))) *
         rfl::make_field<"file_name_">(std::get<1>(_tuples.at(_i))) *
         rfl::make_field<"dependencies_">(dependencies);
}

// ------------------------------------------------------------------------

std::string SQLDependencyTracker::infer_table_name(
    const std::string& _sql) const {
  const std::string drop_table = "DROP TABLE IF EXISTS \"";

  const auto pos = _sql.find(drop_table);

  if (pos == std::string::npos) {
    throw std::runtime_error(
        "Could not find beginning of DROP TABLE statement.");
  }

  const auto begin = pos + drop_table.size();

  const auto end = _sql.find("\";", begin);

  if (end == std::string::npos) {
    throw std::runtime_error("Could not find end of DROP TABLE statement.");
  }

  return transpilation::SQLGenerator::to_lower(_sql.substr(begin, end - begin));
}

// ------------------------------------------------------------------------

void SQLDependencyTracker::save_dependencies(const std::string& _sql) const {
  const auto tuples = save_sql(_sql);

  const auto to_obj = [this, &tuples](const size_t _i) -> SQLDependency {
    return find_dependencies(tuples, _i);
  };

  const auto dependencies = std::views::iota(0uz, tuples.size()) |
                            std::views::transform(to_obj) |
                            std::ranges::to<std::vector>();

  const auto obj =
      rfl::make_named_tuple(rfl::make_field<"dependencies_">(dependencies));

  const auto json_str = rfl::json::write(obj);

  write_to_file("dependencies.json", json_str);
}

// ------------------------------------------------------------------------

typename SQLDependencyTracker::Tuples SQLDependencyTracker::save_sql(
    const std::string& _sql) const {
  const auto sql = helpers::StringSplitter::split(_sql, "\n\n\n");

  const auto get_table_name = [this](const std::string& _sql) -> std::string {
    return infer_table_name(_sql);
  };

  const auto table_names = sql | std::views::transform(get_table_name) |
                           std::ranges::to<std::vector>();

  const auto to_file_name = [](const size_t _i) -> std::string {
    return std::to_string(_i) + ".sql";
  };

  const auto iota = std::views::iota(0uz, table_names.size());

  const auto file_names = iota | std::views::transform(to_file_name) |
                          std::ranges::to<std::vector>();

  for (size_t i = 0; i < file_names.size(); ++i) {
    write_to_file(file_names.at(i), sql.at(i));
  }

  const auto make_tuple = [&table_names, &file_names, &sql](const size_t _i) {
    return std::make_tuple(table_names.at(_i), file_names.at(_i),
                           transpilation::SQLGenerator::to_lower(sql.at(_i)));
  };

  return iota | std::views::transform(make_tuple) |
         std::ranges::to<std::vector>();
}

// ------------------------------------------------------------------------

void SQLDependencyTracker::write_to_file(const std::string& _fname,
                                         const std::string& _content) const {
  std::filesystem::create_directories(folder_);

  std::ofstream file;

  file.open(folder_ + _fname);
  file << _content;
  file.close();
}

// ------------------------------------------------------------------------
}  // namespace utils
}  // namespace engine

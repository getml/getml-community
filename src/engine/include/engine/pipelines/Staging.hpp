// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_PIPELINES_STAGING_HPP_
#define ENGINE_PIPELINES_STAGING_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "engine/Float.hpp"
#include "engine/Int.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace pipelines {
// ----------------------------------------------------------------------------

class Staging {
 public:
  /// Parses the joined names to execute the many-to-one joins required in the
  /// data model.
  static void join_tables(
      const std::vector<std::string>& _origin_peripheral_names,
      const std::string& _joined_population_name,
      const std::vector<std::string>& _joined_peripheral_names,
      containers::DataFrame* _population_df,
      std::vector<containers::DataFrame>* _peripheral_dfs);

 private:
  static containers::Column<Int> extract_join_key(
      const containers::DataFrame& _df, const std::string& _tname,
      const std::string& _alias, const std::string& _colname);

  static containers::DataFrameIndex extract_index(
      const containers::DataFrame& _df, const std::string& _tname,
      const std::string& _alias, const std::string& _colname);

  static std::optional<containers::Column<Float>> extract_time_stamp(
      const containers::DataFrame& _df, const std::string& _tname,
      const std::string& _alias, const std::string& _colname);

  static containers::DataFrame find_peripheral(
      const std::string& _name,
      const std::vector<std::string>& _peripheral_names,
      const std::vector<containers::DataFrame>& _peripheral_dfs);

  static containers::DataFrame join_all(
      const size_t _number, const bool _is_population,
      const std::string& _joined_name,
      const std::vector<std::string>& _origin_peripheral_names,
      const containers::DataFrame& _population_df,
      const std::vector<containers::DataFrame>& _peripheral_dfs);

  static containers::DataFrame join_one(
      const std::string& _splitted, const containers::DataFrame& _population,
      const std::vector<containers::DataFrame>& _peripheral_dfs,
      const std::vector<std::string>& _peripheral_names);

  static std::vector<size_t> make_index(
      const std::string& _name, const std::string& _alias,
      const std::string& _join_key, const std::string& _other_join_key,
      const std::string& _time_stamp, const std::string& _other_time_stamp,
      const std::string& _upper_time_stamp, const std::string& _joined_to_name,
      const std::string& _joined_to_alias, const bool _one_to_one,
      const containers::DataFrame& _population,
      const containers::DataFrame& _peripheral);

  static std::pair<size_t, bool> retrieve_index(
      const size_t _nrows, const Int _jk, const Float _ts,
      const containers::DataFrameIndex& _peripheral_index,
      const std::optional<containers::Column<Float>>& _other_time_stamp,
      const std::optional<containers::Column<Float>>& _upper_time_stamp);

 private:
  static std::optional<containers::Column<Float>> extract_time_stamp(
      const containers::DataFrame& _df, const std::string& _name) {
    if (_name == "") {
      return std::nullopt;
    }

    return _df.time_stamp(_name);
  }
};

// ----------------------------------------------------------------------------
}  // namespace pipelines
}  // namespace engine

#endif  // ENGINE_PIPELINES_STAGING_HPP_


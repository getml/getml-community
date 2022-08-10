// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_CONTAINER_DATAFRAMEPRINTER_HPP_
#define ENGINE_CONTAINER_DATAFRAMEPRINTER_HPP_

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------
namespace engine {
namespace containers {

class DataFramePrinter {
  // -------------------------------

 public:
  DataFramePrinter(const size_t _ncols) : ncols_(_ncols) {}

  ~DataFramePrinter() = default;

  // -------------------------------

 public:
  ///  Returns the data in HTML format, optimized for a Jupyter notebook.
  std::string get_html(const std::vector<std::string> &_colnames,
                       const std::vector<std::string> &_roles,
                       const std::vector<std::string> &_units,
                       const std::vector<std::vector<std::string>> &_rows,
                       const std::int32_t _border) const;

  /// Returns the data in string format, to be printed in a console.
  std::string get_string(
      const std::vector<std::string> &_colnames,
      const std::vector<std::string> &_roles,
      const std::vector<std::string> &_units,
      const std::vector<std::vector<std::string>> &_rows) const;

  // -------------------------------

 private:
  /// Finds the maximum size of a field, for each field in the row.
  std::vector<size_t> calc_max_sizes(
      const std::vector<size_t> &_max_sizes,
      const std::vector<std::string> &_row) const;

  /// Whether the row consists entirely of empty strings.
  bool is_empty(const std::vector<std::string> &_row) const;

  /// Makes a line in the head of an HTML table.
  std::string make_html_head_line(const std::vector<std::string> &_row) const;

  /// Makes a line in the HTML body.
  std::string make_html_body_line(const std::vector<std::string> &_row) const;

  /// Makes a row for the string representation.
  std::string make_row_string(const std::vector<size_t> _max_sizes,
                              const std::vector<std::string> &_row) const;

  /// If the row is longer than 9 fields, this returns the first 8 fields,
  /// followed by '...'.
  std::vector<std::string> truncate_row(
      const std::vector<std::string> &_row) const;

  // -------------------------------

 private:
  /// The number of columns of the data frame to be printed.
  const size_t ncols_;

  // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace containers
}  // namespace engine

#endif  // ENGINE_CONTAINER_DATAFRAMEPRINTER_HPP_

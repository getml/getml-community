// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_DATABASEREADER_HPP_
#define DATABASE_DATABASEREADER_HPP_

#include "database/Iterator.hpp"
#include "io/Reader.hpp"

#include <rfl/Ref.hpp>

#include <vector>

namespace database {

/// DatabaseReader implements the io::Reader interface to a table from a
/// database.
class DatabaseReader final : public io::Reader {
 public:
  explicit DatabaseReader(const rfl::Ref<Iterator>& _iterator);

  ~DatabaseReader() final = default;

 public:
  /// Returns the colnames.
  std::vector<std::string> colnames() final { return iterator()->colnames(); }

  /// Whether we have reached the end of the iterator.
  bool eof() const final { return iterator()->end(); }

  /// Returns the next line.
  std::vector<std::string> next_line() final {
    auto line = std::vector<std::string>(ncols_);
    for (auto& field : line) {
      field = iterator()->get_string();
    }
    return line;
  }

  /// Implements the quotechar.
  char quotechar() const final { return '"'; }

  /// Implements the sep.
  char sep() const final { return '|'; }

 private:
  /// Trivial accessor.
  const rfl::Ref<Iterator>& iterator() const { return iterator_; }

 private:
  /// The underlying database iterator.
  const rfl::Ref<Iterator> iterator_;

  /// The number of columns.
  const size_t ncols_;
};

}  // namespace database

#endif  // DATABASE_DATABASEREADER_HPP_

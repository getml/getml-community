// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef CONTAINERS_DATAFRAMEREADER_HPP_
#define CONTAINERS_DATAFRAMEREADER_HPP_

#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"

#include <memory>
#include <string>
#include <vector>

namespace containers {

class DataFrameReader final : public io::Reader {
 public:
  DataFrameReader(
      const DataFrame& _df,
      const std::shared_ptr<containers::Encoding>& _categories,
      const std::shared_ptr<containers::Encoding>& _join_keys_encoding,
      const char _quotechar, const char _sep);

  ~DataFrameReader() final = default;

  // -------------------------------

 public:
  /// Returns the next line.
  std::vector<std::string> next_line() final;

  // -------------------------------

 public:
  /// Returns the column names.
  std::vector<std::string> colnames() final { return colnames_; }

  /// Trivial accessor.
  const std::vector<std::string>& colnames() const { return colnames_; }

  /// Trivial accessor.
  const std::vector<io::Datatype>& coltypes() const { return coltypes_; }

  /// Whether the end of the file has been reached.
  bool eof() const final { return (rownum_ >= df_.nrows()); }

  /// Trivial getter.
  char quotechar() const final { return quotechar_; }

  /// Trivial getter.
  char sep() const final { return sep_; }

 private:
  /// Generates the column names.
  static std::vector<std::string> make_colnames(const DataFrame& _df,
                                                char _quotechar);

  /// Generates the column types.
  static std::vector<io::Datatype> make_coltypes(const DataFrame& _df);

  /// Updates the counts of the colnames.
  static void update_counts(const std::string& _colname,
                            std::map<std::string, Int>* _counts);

 private:
  /// Trivial (private const) accessor.
  const containers::Encoding& categories() const {
    assert_true(categories_);
    return *categories_;
  }

  /// Trivial (private const) accessor.
  const containers::Encoding& join_keys_encoding() const {
    assert_true(join_keys_encoding_);
    return *join_keys_encoding_;
  }

 private:
  /// The encoding used for the categorical data.
  const std::shared_ptr<const containers::Encoding> categories_;

  /// The colnames of table to be generated.
  const std::vector<std::string> colnames_;

  /// The coltypes of table to be generated.
  const std::vector<io::Datatype> coltypes_;

  /// The filestream of the CSV source file.
  const DataFrame df_;

  /// The encoding used for the join keys.
  const std::shared_ptr<const containers::Encoding> join_keys_encoding_;

  /// The row we are currently in.
  size_t rownum_;

  /// The character used for quotes.
  const char quotechar_;

  /// The character used for separating fields.
  const char sep_;
};

}  // namespace containers

#endif  // CONTAINERS_DATAFRAMEREADER_HPP_

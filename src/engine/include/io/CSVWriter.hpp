// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef IO_CSVWRITER_HPP_
#define IO_CSVWRITER_HPP_

// ----------------------------------------------------------------------------

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

#include "io/Reader.hpp"

// ----------------------------------------------------------------------------

namespace io {
// ----------------------------------------------------------------------------

class CSVWriter {
  // -------------------------------

 public:
  CSVWriter(const std::string& _fname, const size_t _batch_size,
            const std::vector<std::string>& _colnames,
            const std::string& _quotechar, const std::string& _sep)
      : batch_size_(_batch_size),
        colnames_(_colnames),
        filestream_(std::make_shared<std::ofstream>(
            std::ofstream(_fname, std::ofstream::out))),
        quotechar_(_quotechar),
        sep_(_sep) {
    if (!filestream_->is_open()) {
      throw std::runtime_error("'" + _fname + "' could not be opened!");
    }

    if (quotechar_.size() != 1) {
      throw std::runtime_error(
          "The quotechar must consist of exactly one "
          "character!");
    }

    if (sep_.size() != 1) {
      throw std::runtime_error(
          "The separator must consist of exactly one "
          "character!");
    }
  }

  ~CSVWriter() = default;

  // -------------------------------

 public:
  /// Writes the content of the reader to the file.
  void write(Reader* _reader);

  // -------------------------------
 private:
  /// Creates the next line to write to the file.
  std::string make_buffer(const std::vector<std::string>& _line) const;

  /// Creates the next field.
  std::string parse_field(const std::string& _raw_field) const;

  // -------------------------------

 private:
  /// Trivial (private) accessor
  std::ofstream& filestream() {
    assert_true(filestream_);
    return *filestream_;
  }

  /// Trivial (private) accessor
  char quotechar() const {
    assert_true(quotechar_.size() == 1);
    return quotechar_[0];
  }

  /// Trivial (private) accessor
  char sep() const {
    assert_true(sep_.size() == 1);
    return sep_[0];
  }

  // -------------------------------

 private:
  /// The maximum number of lines per file.
  const size_t batch_size_;

  /// The column names to use.
  const std::vector<std::string> colnames_;

  /// The filestream of the CSV file.
  const std::shared_ptr<std::ofstream> filestream_;

  /// The character used for quotes.
  const std::string quotechar_;

  /// The character used for separating fields.
  const std::string sep_;

  // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_CSVWRITER_HPP_

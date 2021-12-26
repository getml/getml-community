#ifndef IO_CSVREADER_HPP_
#define IO_CSVREADER_HPP_

// ----------------------------------------------------------------------------

#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "io/Reader.hpp"

// ----------------------------------------------------------------------------

namespace io {
// ----------------------------------------------------------------------------

class CSVReader : public Reader {
  // -------------------------------

 public:
  CSVReader(const std::optional<std::vector<std::string>>& _colnames,
            const std::string& _fname, const size_t _limit,
            const char _quotechar, const char _sep)
      : colnames_(_colnames),
        filestream_(std::make_shared<std::ifstream>(
            std::ifstream(_fname, std::ifstream::in))),
        limit_(_limit),
        num_lines_read_(0),
        quotechar_(_quotechar),
        sep_(_sep) {
    if (!filestream_->is_open()) {
      throw std::runtime_error("'" + _fname + "' could not be opened!");
    }
  }

  ~CSVReader() = default;

  // -------------------------------

 public:
  /// Returns the next line in the CSV file.
  std::vector<std::string> next_line() final;

  // -------------------------------

 public:
  /// Colnames are either passed by the user or they are the first line of the
  /// CSV file.
  std::vector<std::string> colnames() final {
    if (colnames_) {
      return *colnames_;
    } else {
      return next_line();
    }
  }

  /// Whether the end of the file has been reached.
  bool eof() const final {
    return (limit_ > 0 && num_lines_read_ >= limit_) || filestream_->eof();
  }

  /// Trivial getter.
  char quotechar() const final { return quotechar_; }

  /// Trivial getter.
  char sep() const final { return sep_; }

  // -------------------------------

 private:
  /// Colnames are either passed by the user or they are the first line of the
  /// CSV file.
  const std::optional<std::vector<std::string>> colnames_;

  /// The filestream of the CSV source file.
  const std::shared_ptr<std::ifstream> filestream_;

  /// The number of lines read is limited to limit_. Set to 0 for an unlimited
  /// number of lines read.
  const size_t limit_;

  /// The number of lines already read.
  size_t num_lines_read_;

  /// The character used for quotes.
  const char quotechar_;

  /// The character used for separating fields.
  const char sep_;

  // -------------------------------
};

// ----------------------------------------------------------------------------
}  // namespace io

#endif  // IO_CSVREADER_HPP_

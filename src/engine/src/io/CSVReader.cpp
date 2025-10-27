// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "io/CSVReader.hpp"

#include "io/Parser.hpp"

namespace io {

CSVReader::CSVReader(const std::optional<std::vector<std::string>>& _colnames,
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
// ----------------------------------------------------------------------------

std::vector<std::string> CSVReader::next_line() {
  // ------------------------------------------------------------------------
  // Usually the calling function should make sure that we haven't reached
  // the end of file. But just to be sure, we do it again.

  if (eof()) {
    return std::vector<std::string>();
  }

  // ------------------------------------------------------------------------
  // Read the next line from the filestream - if it is empty, return an
  // empty vector.

  std::string line;

  std::getline(*filestream_, line);

  num_lines_read_++;

  if (line.size() == 0) {
    return std::vector<std::string>();
  }

  // ------------------------------------------------------------------------
  // Chop up lines into fields.

  std::vector<std::string> result;

  std::string field;

  bool is_quoted = false;

  for (char c : line) {
    if (c == sep_ && !is_quoted) {
      result.push_back(field);
      field.clear();
    } else if (c == quotechar_) {
      is_quoted = !is_quoted;
    } else {
      field += c;
    }
  }

  result.push_back(Parser::trim(field));

  // ------------------------------------------------------------------------

  return result;

  // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace io

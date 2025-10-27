// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "io/CSVWriter.hpp"

#include "io/Parser.hpp"

namespace io {

CSVWriter::CSVWriter(const std::string& _fname, const size_t _batch_size,
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
// ----------------------------------------------------------------------------

std::string CSVWriter::make_buffer(
    const std::vector<std::string>& _line) const {
  std::string buffer;

  assert_true(_line.size() == colnames_.size());

  for (size_t i = 0; i < _line.size(); ++i) {
    buffer += parse_field(_line[i]);

    if (i < _line.size() - 1) {
      buffer += sep_;
    } else {
      buffer += '\n';
    }
  }

  return buffer;
}

// ----------------------------------------------------------------------------

std::string CSVWriter::parse_field(const std::string& _raw_field) const {
  auto field = io::Parser::remove_quotechars(_raw_field, quotechar());

  if (field.find(sep()) != std::string::npos) {
    field = quotechar() + field + quotechar();
  }

  return field;
}

// ----------------------------------------------------------------------------

void CSVWriter::write(io::Reader* _reader) {
  //  ------------------------------------------------------------------------

  const std::string buffer = make_buffer(colnames_);

  filestream() << buffer;

  // ----------------------------------------------------------------
  // Insert line by line.

  size_t line_count = 0;

  while (!_reader->eof() && (batch_size_ == 0 || line_count < batch_size_)) {
    const std::vector<std::string> line = _reader->next_line();

    ++line_count;

    if (line.size() != colnames_.size()) {
      std::cout << "Corrupted line: " << line_count << ". Expected "
                << colnames_.size() << " fields, saw " << line.size() << "."
                << std::endl;

      continue;
    }

    const std::string buffer = make_buffer(line);

    filestream() << buffer;
  }

  filestream().close();
}

// ------------------------------------------------------------------------
}  // namespace io

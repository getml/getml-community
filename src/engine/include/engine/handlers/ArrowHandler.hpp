// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_ARROWHANDLER_HPP_
#define ENGINE_HANDLERS_ARROWHANDLER_HPP_

#include "containers/DataFrame.hpp"
#include "containers/Encoding.hpp"
#include "engine/Float.hpp"
#include "engine/config/Options.hpp"
#include "strings/String.hpp"

#include <Poco/Net/StreamSocket.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <rfl/Ref.hpp>

#include <cctype>
#include <memory>
#include <optional>
#include <vector>

namespace engine {
namespace handlers {

class ArrowHandler {
  using FloatFunction = std::function<Float(const std::int64_t)>;
  using StringFunction = std::function<strings::String(const std::int64_t)>;

 public:
  ArrowHandler(const rfl::Ref<containers::Encoding>& _categories,
               const rfl::Ref<containers::Encoding>& _join_keys_encoding,
               const config::Options& _options)
      : categories_(_categories),
        join_keys_encoding_(_join_keys_encoding),
        options_(_options) {}

  ~ArrowHandler() = default;

 public:
  /// Extracts an arrow::Table from a DataFrame.
  std::shared_ptr<arrow::Table> df_to_table(
      const containers::DataFrame& _df) const;

  /// Reads a parquet file and returns an arrow::Table.
  std::shared_ptr<arrow::Table> read_parquet(
      const std::string& _filename) const;

  /// Receives an arrow::Table from a stream socket.
  template <class T>
  containers::Column<T> recv_column(const std::shared_ptr<memmap::Pool>& _pool,
                                    const std::string& _colname,
                                    Poco::Net::StreamSocket* _socket) const;

  /// Receives an arrow::Table from a stream socket.
  std::shared_ptr<arrow::Table> recv_table(
      Poco::Net::StreamSocket* _socket) const;

  /// Send an array via the socket.
  void send_array(const std::shared_ptr<arrow::ChunkedArray>& _array,
                  const std::shared_ptr<arrow::Field>& _field,
                  Poco::Net::StreamSocket* _socket) const;

  /// Sends a table via the socket.
  void send_table(const std::shared_ptr<arrow::Table>& _table,
                  Poco::Net::StreamSocket* _socket) const;

  /// Stores an arrow table as a parquet file.
  void to_parquet(const std::shared_ptr<arrow::Table>& _table,
                  const std::string& _filename,
                  const std::string& _compression) const;

  /// Extracts a DataFrame from an arrow::Table.
  containers::DataFrame table_to_df(const std::shared_ptr<arrow::Table>& _table,
                                    const std::string& _name,
                                    const containers::Schema& _schema) const;

 private:
  /// Extracts the arrow::Schema from a DataFrame.
  std::shared_ptr<arrow::Schema> df_to_schema(
      const containers::DataFrame& _df) const;

  /// Extracts the arrays from a DataFrame.
  std::vector<std::shared_ptr<arrow::ChunkedArray>> extract_arrays(
      const containers::DataFrame& _df) const;

  /// Returns the appropriate compression format.
  parquet::Compression::type parse_compression(
      const std::string& _compression) const;

  /// Converts a chunked array to a float column.
  template <class T>
  containers::Column<T> to_column(
      const std::shared_ptr<memmap::Pool>& _pool, const std::string& _name,
      const std::shared_ptr<arrow::ChunkedArray>& _arr) const;

  /// Returns a function that writes a boolean chunk to a float column or
  /// std::nullopt, if the _chunk is not a boolean chunk.
  std::optional<FloatFunction> write_boolean_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a boolean chunk to a string column or
  /// std::nullopt, if the _chunk is not a boolean chunk.
  std::optional<StringFunction> write_boolean_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Writes a dict chunk to a float column. Returns true on success.
  std::optional<FloatFunction> write_dict_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Writes a dict chunk to a string column. Returns true on success.
  std::optional<StringFunction> write_dict_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a float chunk to a string column or
  /// std::nullopt, if the _chunk is not a float chunk.
  std::optional<StringFunction> write_float_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes an int chunk to a string column or
  /// std::nullopt, if the _chunk is not an int chunk.
  std::optional<StringFunction> write_int_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a null chunk to a float column or
  /// std::nullopt, if the _chunk is not a null chunk.
  std::optional<FloatFunction> write_null_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a null chunk to a string column or
  /// std::nullopt, if the _chunk is not a null chunk.
  std::optional<StringFunction> write_null_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a numeric chunk to a float column or
  /// std::nullopt, if the _chunk is not a numeric chunk.
  std::optional<FloatFunction> write_numeric_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a string chunk to a float column or
  /// std::nullopt, if the _chunk is not a string chunk.
  std::optional<FloatFunction> write_string_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function writes a string chunk to a string column or
  /// std::nullopt, if the _chunk is not a string chunk.
  std::optional<StringFunction> write_string_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function that writes a time chunk to a float column or
  /// std::nullopt, if the _chunk is not a time chunk.
  std::optional<FloatFunction> write_time_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a function writes a time chunk to a string column or
  /// std::nullopt, if the _chunk is not a time chunk.
  std::optional<StringFunction> write_time_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk) const;

  /// Returns a functions that writes a chunk to a float column.
  FloatFunction write_to_float_column(
      const std::shared_ptr<arrow::Array>& _chunk,
      const std::string& _name) const;

  /// Returns a functions that writes a chunk to a string column.
  StringFunction write_to_string_column(
      const std::shared_ptr<arrow::Array>& _chunk,
      const std::string& _name) const;

 private:
  /// Encodes the categories used.
  const rfl::Ref<containers::Encoding> categories_;

  /// Encodes the join keys used.
  const rfl::Ref<containers::Encoding> join_keys_encoding_;

  /// Settings for the engine and the monitor
  const config::Options options_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
containers::Column<T> ArrowHandler::recv_column(
    const std::shared_ptr<memmap::Pool>& _pool, const std::string& _colname,
    Poco::Net::StreamSocket* _socket) const {
  const auto table = recv_table(_socket);

  assert_true(table);

  const auto schema = table->schema();

  assert_true(schema);

  const auto arr = table->GetColumnByName(_colname);

  return to_column<T>(_pool, _colname, arr);
}

// ----------------------------------------------------------------------------

template <class T>
containers::Column<T> ArrowHandler::to_column(
    const std::shared_ptr<memmap::Pool>& _pool, const std::string& _name,
    const std::shared_ptr<arrow::ChunkedArray>& _arr) const {
  static_assert(std::is_same<T, Float>() || std::is_same<T, strings::String>(),
                "T must be Float or String");

  if (!_arr) {
    throw std::runtime_error("Column '" + _name + "' not found!");
  }

  auto col = containers::Column<T>(_pool);

  if constexpr (std::is_same<T, Float>()) {
    col = containers::Column<T>(_pool, _arr->length());
  }

  for (std::int64_t nchunk = 0, begin = 0; nchunk < _arr->num_chunks();
       ++nchunk) {
    const auto chunk = _arr->chunk(nchunk);

    throw_unless(chunk, "Could not extract chunk from field '" + _name + "'!");

    throw_unless(chunk->type(),
                 "Could not extract type from field '" + _name + "'!");

    throw_unless(
        begin + chunk->length() <= _arr->length(),
        "Sum of chunks greater than the length of the chunked array in "
        "field '" +
            _name + "'!");

    if constexpr (std::is_same<T, Float>()) {
      const auto func = write_to_float_column(chunk, _name);

      for (std::int64_t i = 0; i < chunk->length(); ++i) {
        col[begin + i] = func(i);
      }
    }

    if constexpr (std::is_same<T, strings::String>()) {
      const auto func = write_to_string_column(chunk, _name);

      for (std::int64_t i = 0; i < chunk->length(); ++i) {
        col.push_back(func(i));
      }
    }

    begin += chunk->length();
  }

  col.set_name(_name);

  return col;
}

// -------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

// -------------------------------------------------------------------------

#endif  // ENGINE_HANDLERS_ARROWHANDLER_HPP_

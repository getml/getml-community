// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "engine/handlers/ArrowHandler.hpp"

// ----------------------------------------------------------------------------

#include "engine/handlers/ArrowSocketInputStream.hpp"
#include "engine/handlers/ArrowSocketOutputStream.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace handlers {

std::shared_ptr<arrow::Table> ArrowHandler::df_to_table(
    const containers::DataFrame& _df) const {
  const auto schema = df_to_schema(_df);

  const auto chunked_arrays = extract_arrays(_df);

  return arrow::Table::Make(schema, chunked_arrays,
                            static_cast<std::int64_t>(_df.nrows()));
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<arrow::ChunkedArray>> ArrowHandler::extract_arrays(
    const containers::DataFrame& _df) const {
  using Array = std::shared_ptr<arrow::ChunkedArray>;

  const auto categoricals_to_string_array = [this](const auto& _col) -> Array {
    const auto to_str = [this](const Int _i) -> std::string {
      return (*categories_)[_i].str();
    };
    auto range = _col | VIEWS::transform(to_str);
    return containers::ArrayMaker::make_string_array(range.begin(),
                                                     range.end());
  };

  const auto join_keys_to_string_array = [this](const auto& _col) -> Array {
    const auto to_str = [this](const Int _i) -> std::string {
      return (*join_keys_encoding_)[_i].str();
    };
    auto range = _col | VIEWS::transform(to_str);
    return containers::ArrayMaker::make_string_array(range.begin(),
                                                     range.end());
  };

  const auto to_float_or_ts_array = [](const auto& _col) -> Array {
    if (_col.unit().find("time stamp") != std::string::npos) {
      return containers::ArrayMaker::make_time_stamp_array(_col.begin(),
                                                           _col.end());
    }
    return containers::ArrayMaker::make_float_array(_col.begin(), _col.end());
  };

  const auto to_string_array = [](const auto& _col) -> Array {
    const auto to_str = [](const strings::String& _str) -> std::string {
      return _str.str();
    };
    auto range = _col | VIEWS::transform(to_str);
    return containers::ArrayMaker::make_string_array(range.begin(),
                                                     range.end());
  };

  const auto categoricals = fct::collect::vector<Array>(
      _df.categoricals() | VIEWS::transform(categoricals_to_string_array));

  const auto join_keys = fct::collect::vector<Array>(
      _df.join_keys() | VIEWS::transform(join_keys_to_string_array));

  const auto numericals = fct::collect::vector<Array>(
      _df.numericals() | VIEWS::transform(to_float_or_ts_array));

  const auto targets = fct::collect::vector<Array>(
      _df.targets() | VIEWS::transform(to_float_or_ts_array));

  const auto text = fct::collect::vector<Array>(
      _df.text() | VIEWS::transform(to_string_array));

  const auto time_stamps = fct::collect::vector<Array>(
      _df.time_stamps() | VIEWS::transform(to_float_or_ts_array));

  const auto unused_floats = fct::collect::vector<Array>(
      _df.unused_floats() | VIEWS::transform(to_float_or_ts_array));

  const auto unused_strings = fct::collect::vector<Array>(
      _df.unused_strings() | VIEWS::transform(to_string_array));

  return fct::join::vector<Array>({categoricals, join_keys, numericals, targets,
                                   text, time_stamps, unused_floats,
                                   unused_strings});
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> ArrowHandler::df_to_schema(
    const containers::DataFrame& _df) const {
  using Field = std::shared_ptr<arrow::Field>;

  const auto to_float_or_ts_field = [](const auto& _col) -> Field {
    if (_col.unit().find("time stamp") != std::string::npos) {
      return arrow::field(_col.name(), arrow::timestamp(arrow::TimeUnit::NANO));
    }

    return arrow::field(_col.name(), arrow::float64());
  };

  const auto to_string_field = [](const auto& _col) -> Field {
    return arrow::field(_col.name(), arrow::utf8());
  };

  const auto categoricals = fct::collect::vector<Field>(
      _df.categoricals() | VIEWS::transform(to_string_field));

  const auto join_keys = fct::collect::vector<Field>(
      _df.join_keys() | VIEWS::transform(to_string_field));

  const auto numericals = fct::collect::vector<Field>(
      _df.numericals() | VIEWS::transform(to_float_or_ts_field));

  const auto targets = fct::collect::vector<Field>(
      _df.targets() | VIEWS::transform(to_float_or_ts_field));

  const auto text = fct::collect::vector<Field>(
      _df.text() | VIEWS::transform(to_string_field));

  const auto time_stamps = fct::collect::vector<Field>(
      _df.time_stamps() | VIEWS::transform(to_float_or_ts_field));

  const auto unused_floats = fct::collect::vector<Field>(
      _df.unused_floats() | VIEWS::transform(to_float_or_ts_field));

  const auto unused_strings = fct::collect::vector<Field>(
      _df.unused_strings() | VIEWS::transform(to_string_field));

  const auto all_fields = fct::join::vector<Field>(
      {categoricals, join_keys, numericals, targets, text, time_stamps,
       unused_floats, unused_strings});

  return arrow::schema(all_fields);
}

// ----------------------------------------------------------------------------

parquet::Compression::type ArrowHandler::parse_compression(
    const std::string& _compression) const {
  if (_compression == "brotli") {
    return parquet::Compression::BROTLI;
  }

  if (_compression == "gzip") {
    return parquet::Compression::GZIP;
  }

  if (_compression == "lz4") {
    return parquet::Compression::LZ4;
  }

  if (_compression == "snappy") {
    return parquet::Compression::SNAPPY;
  }

  if (_compression == "zstd") {
    return parquet::Compression::ZSTD;
  }

  throw std::runtime_error("Unknown compression format: '" + _compression +
                           "'.");
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Table> ArrowHandler::read_parquet(
    const std::string& _filename) const {
  const auto pool = arrow::default_memory_pool();

  const auto result = arrow::io::ReadableFile::Open(_filename);

  if (!result.ok()) {
    throw std::runtime_error("Could not find or open file '" + _filename +
                             "': " + result.status().message());
  }

  const auto input = result.ValueOrDie();

  std::unique_ptr<parquet::arrow::FileReader> arrow_reader;

  auto status = parquet::arrow::OpenFile(input, pool, &arrow_reader);

  if (!status.ok()) {
    throw std::runtime_error("Could not open parquet file '" + _filename +
                             "': " + status.message());
  }

  std::shared_ptr<arrow::Table> table;

  status = arrow_reader->ReadTable(&table);

  if (!status.ok()) {
    throw std::runtime_error("Could not read table: " + status.message());
  }

  return table;
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Table> ArrowHandler::recv_table(
    Poco::Net::StreamSocket* _socket) const {
  const auto input_stream = std::make_shared<ArrowSocketInputStream>(_socket);

  const auto stream_reader_result =
      arrow::ipc::RecordBatchStreamReader::Open(input_stream);

  if (!stream_reader_result.ok()) {
    throw std::runtime_error(stream_reader_result.status().message());
  }

  const auto stream_reader = stream_reader_result.ValueOrDie();

  const auto table_result =
      arrow::Table::FromRecordBatchReader(stream_reader.get());

  if (!table_result.ok()) {
    throw std::runtime_error(table_result.status().message());
  }

  return table_result.ValueOrDie();
}

// ----------------------------------------------------------------------------

containers::DataFrame ArrowHandler::table_to_df(
    const std::shared_ptr<arrow::Table>& _table, const std::string& _name,
    const containers::Schema& _schema) const {
  throw_unless(_table, "No table passed");

  const auto schema = _table->schema();

  throw_unless(schema, "_table has no schema");

  const auto to_int_column =
      [](const containers::Column<strings::String>& _col,
         const std::shared_ptr<containers::Encoding>& _encoding)
      -> containers::Column<Int> {
    assert_true(_encoding);
    const auto data_ptr = std::make_shared<std::vector<Int>>(_col.nrows());
    for (size_t i = 0; i < _col.nrows(); ++i) {
      (*data_ptr)[i] = (*_encoding)[_col[i]];
    }
    return containers::Column<Int>(data_ptr, _col.name());
  };

  const auto pool = options_.make_pool();

  // TODO
  auto df = containers::DataFrame(_name, categories_.ptr(),
                                  join_keys_encoding_.ptr(), pool);

  for (const auto& colname : _schema.categoricals_) {
    const auto arr = _table->GetColumnByName(colname);
    const auto col =
        to_int_column(to_column<strings::String>(pool, colname, arr),
                      categories_.ptr());  // TODO
    df.add_int_column(col, containers::DataFrame::ROLE_CATEGORICAL);
  }

  for (const auto& colname : _schema.join_keys_) {
    const auto arr = _table->GetColumnByName(colname);
    const auto col =
        to_int_column(to_column<strings::String>(pool, colname, arr),
                      join_keys_encoding_.ptr());  // TODO
    df.add_int_column(col, containers::DataFrame::ROLE_JOIN_KEY);
  }

  for (const auto& colname : _schema.numericals_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_float_column(to_column<Float>(pool, colname, arr),
                        containers::DataFrame::ROLE_NUMERICAL);
  }

  for (const auto& colname : _schema.targets_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_float_column(to_column<Float>(pool, colname, arr),
                        containers::DataFrame::ROLE_TARGET);
  }

  for (const auto& colname : _schema.text_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_string_column(to_column<strings::String>(pool, colname, arr),
                         containers::DataFrame::ROLE_TEXT);
  }

  for (const auto& colname : _schema.time_stamps_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_float_column(to_column<Float>(pool, colname, arr),
                        containers::DataFrame::ROLE_TIME_STAMP);
  }

  for (const auto& colname : _schema.unused_floats_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_float_column(to_column<Float>(pool, colname, arr),
                        containers::DataFrame::ROLE_UNUSED_FLOAT);
  }

  for (const auto& colname : _schema.unused_strings_) {
    const auto arr = _table->GetColumnByName(colname);
    df.add_string_column(to_column<strings::String>(pool, colname, arr),
                         containers::DataFrame::ROLE_UNUSED_STRING);
  }

  return df;
}

// ----------------------------------------------------------------------------

void ArrowHandler::send_array(
    const std::shared_ptr<arrow::ChunkedArray>& _array,
    const std::shared_ptr<arrow::Field>& _field,
    Poco::Net::StreamSocket* _socket) const {
  const auto schema = arrow::schema({_field});

  const auto table = arrow::Table::Make(schema, {_array}, -1);

  send_table(table, _socket);
}

// ----------------------------------------------------------------------------

void ArrowHandler::send_table(const std::shared_ptr<arrow::Table>& _table,
                              Poco::Net::StreamSocket* _socket) const {
  assert_true(_table);

  const auto stream = std::make_shared<ArrowSocketOutputStream>(_socket);

  const auto writer_result =
      arrow::ipc::MakeStreamWriter(stream, _table->schema());

  if (!writer_result.status().ok()) {
    throw std::runtime_error(writer_result.status().message());
  }

  const auto writer = writer_result.ValueOrDie();

  communication::Sender::send_string("Success!", _socket);

  auto status = writer->WriteTable(*_table);

  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }

  status = writer->Close();

  if (!status.ok()) {
    throw std::runtime_error(status.message());
  }
}

// ----------------------------------------------------------------------------

void ArrowHandler::to_parquet(const std::shared_ptr<arrow::Table>& _table,
                              const std::string& _filename,
                              const std::string& _compression) const {
  const auto filename = _filename.find(".parquet") == std::string::npos
                            ? _filename + ".parquet"
                            : _filename;

  const auto result = arrow::io::FileOutputStream::Open(filename);

  if (!result.ok()) {
    throw std::runtime_error("Could not create file '" + filename +
                             "': " + result.status().message());
  }

  const auto outfile = result.ValueOrDie();

  parquet::WriterProperties::Builder builder;

  builder.compression(parse_compression(_compression));

  const auto props = builder.build();

  assert_true(_table);

  const auto status = parquet::arrow::WriteTable(
      *_table, arrow::default_memory_pool(), outfile, 100000, props);

  if (!status.ok()) {
    throw std::runtime_error("Could not write table: " + status.message());
  }
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_boolean_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto boolean_to_float = [](const bool val) -> Float {
    return val ? 1.0 : 0.0;
  };

  const auto transform_chunk = [boolean_to_float](const std::int64_t _i,
                                                  const auto _chunk) -> Float {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return NAN;
    }

    return boolean_to_float(_chunk->Value(_i));
  };

  if (_chunk->type()->Equals(arrow::boolean())) {
    const auto chunk = std::static_pointer_cast<arrow::BooleanArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_boolean_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i,
                                  const auto _chunk) -> strings::String {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return strings::String(nullptr);
    }

    return strings::String(io::Parser::to_string(_chunk->Value(_i)));
  };

  if (_chunk->type()->Equals(arrow::boolean())) {
    const auto chunk = std::static_pointer_cast<arrow::BooleanArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_dict_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto dict_name =
      arrow::dictionary(arrow::int32(), arrow::utf8())->name();

  if (_chunk->type()->name() == dict_name) {
    const auto chunk = std::static_pointer_cast<arrow::DictionaryArray>(_chunk);

    assert_true(chunk);
    assert_true(chunk->indices());

    const auto func = write_to_float_column(chunk->dictionary(), "dictionary");

    return [func, chunk](const std::int64_t _i) -> Float {
      assert_true(_i >= 0);
      assert_true(_i < chunk->indices()->length());
      if (chunk->indices()->IsNull(_i)) {
        return NAN;
      }
      return func(chunk->GetValueIndex(_i));
    };
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_dict_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto dict_name =
      arrow::dictionary(arrow::int32(), arrow::utf8())->name();

  if (_chunk->type()->name() == dict_name) {
    const auto chunk = std::static_pointer_cast<arrow::DictionaryArray>(_chunk);

    assert_true(chunk);
    assert_true(chunk->indices());

    const auto func = write_to_string_column(chunk->dictionary(), "dictionary");

    return [func, chunk](const std::int64_t _i) -> strings::String {
      assert_true(_i >= 0);
      assert_true(_i < chunk->indices()->length());
      if (chunk->indices()->IsNull(_i)) {
        return strings::String(nullptr);
      }
      return func(chunk->GetValueIndex(_i));
    };
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_float_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i,
                                  const auto _chunk) -> strings::String {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return strings::String(nullptr);
    }

    return strings::String(
        io::Parser::to_string(static_cast<Float>(_chunk->Value(_i))));
  };

  if (_chunk->type()->Equals(arrow::float16())) {
    const auto chunk = std::static_pointer_cast<arrow::HalfFloatArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::float32())) {
    const auto chunk = std::static_pointer_cast<arrow::FloatArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::float64())) {
    const auto chunk = std::static_pointer_cast<arrow::DoubleArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_int_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i,
                                  const auto _chunk) -> strings::String {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return strings::String(nullptr);
    }

    return strings::String(std::to_string(_chunk->Value(_i)));
  };

  if (_chunk->type()->Equals(arrow::uint8())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt8Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int8())) {
    const auto chunk = std::static_pointer_cast<arrow::Int8Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint16())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt16Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int16())) {
    const auto chunk = std::static_pointer_cast<arrow::Int16Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint32())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int32())) {
    const auto chunk = std::static_pointer_cast<arrow::Int32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint64())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int64())) {
    const auto chunk = std::static_pointer_cast<arrow::Int64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->name() ==
      arrow::duration(arrow::TimeUnit::SECOND)
          ->name())  // the name will always be 'duration', regardless of
                     // the TimeUnit.
  {
    const auto chunk = std::static_pointer_cast<arrow::DurationArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_null_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  if (_chunk->type()->Equals(arrow::null())) {
    return [](const std::int64_t _i) { return NAN; };
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_null_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  if (_chunk->type()->Equals(arrow::null())) {
    return [](const std::int64_t _i) { return strings::String(nullptr); };
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_numeric_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  assert_true(_chunk->type());

  const auto transform_chunk = [](const std::int64_t _i,
                                  const auto _chunk) -> Float {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return NAN;
    }

    return static_cast<Float>(_chunk->Value(_i));
  };

  if (_chunk->type()->Equals(arrow::uint8())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt8Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int8())) {
    const auto chunk = std::static_pointer_cast<arrow::Int8Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint16())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt16Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int16())) {
    const auto chunk = std::static_pointer_cast<arrow::Int16Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint32())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int32())) {
    const auto chunk = std::static_pointer_cast<arrow::Int32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::uint64())) {
    const auto chunk = std::static_pointer_cast<arrow::UInt64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::int64())) {
    const auto chunk = std::static_pointer_cast<arrow::Int64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::float16())) {
    const auto chunk = std::static_pointer_cast<arrow::HalfFloatArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::float32())) {
    const auto chunk = std::static_pointer_cast<arrow::FloatArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::float64())) {
    const auto chunk = std::static_pointer_cast<arrow::DoubleArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->name() ==
      arrow::duration(arrow::TimeUnit::SECOND)
          ->name())  // the name will always be 'duration', regardless of
                     // the TimeUnit.
  {
    const auto chunk = std::static_pointer_cast<arrow::DurationArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_string_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto string_to_float = [](const std::string& str) -> Float {
    const auto [val, success] = io::Parser::to_double(str);
    return success ? val : NAN;
  };

  const auto transform_chunk = [string_to_float](const std::int64_t _i,
                                                 const auto _chunk) -> Float {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return NAN;
    }

    return string_to_float(_chunk->GetString(_i));
  };

  if (_chunk->type()->Equals(arrow::utf8())) {
    const auto chunk = std::static_pointer_cast<arrow::StringArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::large_utf8())) {
    const auto chunk =
        std::static_pointer_cast<arrow::LargeStringArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::binary())) {
    const auto chunk = std::static_pointer_cast<arrow::BinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::large_binary())) {
    const auto chunk =
        std::static_pointer_cast<arrow::LargeBinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (arrow::is_fixed_size_binary(_chunk->type()->id())) {
    const auto chunk =
        std::static_pointer_cast<arrow::FixedSizeBinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_string_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i,
                                  const auto _chunk) -> strings::String {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return strings::String(nullptr);
    }

    return strings::String(_chunk->GetString(_i));
  };

  if (_chunk->type()->Equals(arrow::utf8())) {
    const auto chunk = std::static_pointer_cast<arrow::StringArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::large_utf8())) {
    const auto chunk =
        std::static_pointer_cast<arrow::LargeStringArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::binary())) {
    const auto chunk = std::static_pointer_cast<arrow::BinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (_chunk->type()->Equals(arrow::large_binary())) {
    const auto chunk =
        std::static_pointer_cast<arrow::LargeBinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  if (arrow::is_fixed_size_binary(_chunk->type()->id())) {
    const auto chunk =
        std::static_pointer_cast<arrow::FixedSizeBinaryArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::FloatFunction>
ArrowHandler::write_time_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i, const auto _chunk,
                                  const Float _factor) -> Float {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return NAN;
    }

    return static_cast<Float>(_chunk->Value(_i)) * _factor;
  };

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-6);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::NANO))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-9);
  }

  if (_chunk->type()->Equals(arrow::time32(arrow::TimeUnit::SECOND))) {
    const auto chunk = std::static_pointer_cast<arrow::Time32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0);
  }

  if (_chunk->type()->Equals(arrow::time32(arrow::TimeUnit::MILLI))) {
    const auto chunk = std::static_pointer_cast<arrow::Time32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  if (_chunk->type()->Equals(arrow::time64(arrow::TimeUnit::MICRO))) {
    const auto chunk = std::static_pointer_cast<arrow::Time64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-6);
  }

  if (_chunk->type()->Equals(arrow::time64(arrow::TimeUnit::NANO))) {
    const auto chunk = std::static_pointer_cast<arrow::Time64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-9);
  }

  if (_chunk->type()->Equals(arrow::date32())) {
    const auto chunk = std::static_pointer_cast<arrow::Date32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 86400.0);
  }

  if (_chunk->type()->Equals(arrow::date64())) {
    const auto chunk = std::static_pointer_cast<arrow::Date64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

std::optional<typename ArrowHandler::StringFunction>
ArrowHandler::write_time_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk) const {
  assert_true(_chunk);

  const auto transform_chunk = [](const std::int64_t _i, const auto _chunk,
                                  const Float _factor) -> strings::String {
    assert_true(_i >= 0);
    assert_true(_i < _chunk->length());

    if (_chunk->IsNull(_i)) {
      return strings::String(nullptr);
    }

    return strings::String(io::Parser::ts_to_string(
        static_cast<Float>(_chunk->Value(_i)) * _factor));
  };

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::SECOND))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::MILLI))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::MICRO))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-6);
  }

  if (_chunk->type()->Equals(arrow::timestamp(arrow::TimeUnit::NANO))) {
    const auto chunk = std::static_pointer_cast<arrow::TimestampArray>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-9);
  }

  if (_chunk->type()->Equals(arrow::time32(arrow::TimeUnit::SECOND))) {
    const auto chunk = std::static_pointer_cast<arrow::Time32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0);
  }

  if (_chunk->type()->Equals(arrow::time32(arrow::TimeUnit::MILLI))) {
    const auto chunk = std::static_pointer_cast<arrow::Time32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  if (_chunk->type()->Equals(arrow::time64(arrow::TimeUnit::MICRO))) {
    const auto chunk = std::static_pointer_cast<arrow::Time64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-6);
  }

  if (_chunk->type()->Equals(arrow::time64(arrow::TimeUnit::NANO))) {
    const auto chunk = std::static_pointer_cast<arrow::Time64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-9);
  }

  if (_chunk->type()->Equals(arrow::date32())) {
    const auto chunk = std::static_pointer_cast<arrow::Date32Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 86400.0);
  }

  if (_chunk->type()->Equals(arrow::date64())) {
    const auto chunk = std::static_pointer_cast<arrow::Date64Array>(_chunk);
    assert_true(chunk);
    return std::bind(transform_chunk, std::placeholders::_1, chunk, 1.0e-3);
  }

  return std::nullopt;
}

// ----------------------------------------------------------------------------

typename ArrowHandler::FloatFunction ArrowHandler::write_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name) const {
  auto func = write_boolean_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_dict_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_null_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_numeric_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_string_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_time_to_float_column(_chunk);

  if (func) {
    return *func;
  }

  throw std::runtime_error("Unsupported field type for field '" + _name +
                           "': " + _chunk->type()->name() + ".");

  return [](const std::int64_t _i) -> Float { return 0.0; };
}

// ----------------------------------------------------------------------------

typename ArrowHandler::StringFunction ArrowHandler::write_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name) const {
  auto func = write_boolean_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_dict_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_float_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_int_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_null_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_string_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  func = write_time_to_string_column(_chunk);

  if (func) {
    return *func;
  }

  throw std::runtime_error("Unsupported field type for field '" + _name +
                           "': " + _chunk->type()->name() + ".");

  return [](const std::int64_t _i) -> strings::String {
    return strings::String("");
  };
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

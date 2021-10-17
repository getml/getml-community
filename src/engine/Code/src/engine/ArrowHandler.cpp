#include "engine/handlers/handlers.hpp"

namespace engine
{
namespace handlers
{
// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Table> ArrowHandler::df_to_table(
    const containers::DataFrame& _df ) const
{
    const auto schema = df_to_schema( _df );

    const auto chunked_arrays = extract_arrays( _df );

    return arrow::Table::Make(
        schema, chunked_arrays, static_cast<std::int64_t>( _df.nrows() ) );
}

// ----------------------------------------------------------------------------

std::vector<std::shared_ptr<arrow::ChunkedArray>> ArrowHandler::extract_arrays(
    const containers::DataFrame& _df ) const
{
    using Array = std::shared_ptr<arrow::ChunkedArray>;

    assert_true( categories_ );

    assert_true( join_keys_encoding_ );

    const auto categoricals_to_string_array =
        [this]( const auto& _col ) -> Array {
        const auto to_str = [this]( const Int _i ) -> std::string {
            return ( *categories_ )[_i].str();
        };
        auto range = _col | std::views::transform( to_str );
        return containers::ArrayMaker::make_string_array(
            range.begin(), range.end() );
    };

    const auto join_keys_to_string_array = [this]( const auto& _col ) -> Array {
        const auto to_str = [this]( const Int _i ) -> std::string {
            return ( *join_keys_encoding_ )[_i].str();
        };
        auto range = _col | std::views::transform( to_str );
        return containers::ArrayMaker::make_string_array(
            range.begin(), range.end() );
    };

    const auto to_float_or_ts_array = []( const auto& _col ) -> Array {
        if ( _col.unit().find( "time stamp" ) != std::string::npos )
            {
                return containers::ArrayMaker::make_time_stamp_array(
                    _col.begin(), _col.end() );
            }
        return containers::ArrayMaker::make_float_array(
            _col.begin(), _col.end() );
    };

    const auto to_string_array = []( const auto& _col ) -> Array {
        const auto to_str = []( const strings::String& _str ) -> std::string {
            return _str.str();
        };
        auto range = _col | std::views::transform( to_str );
        return containers::ArrayMaker::make_string_array(
            range.begin(), range.end() );
    };

    const auto categoricals = stl::collect::vector<Array>(
        _df.categoricals() |
        std::views::transform( categoricals_to_string_array ) );

    const auto join_keys = stl::collect::vector<Array>(
        _df.join_keys() | std::views::transform( join_keys_to_string_array ) );

    const auto numericals = stl::collect::vector<Array>(
        _df.numericals() | std::views::transform( to_float_or_ts_array ) );

    const auto targets = stl::collect::vector<Array>(
        _df.targets() | std::views::transform( to_float_or_ts_array ) );

    const auto text = stl::collect::vector<Array>(
        _df.text() | std::views::transform( to_string_array ) );

    const auto time_stamps = stl::collect::vector<Array>(
        _df.time_stamps() | std::views::transform( to_float_or_ts_array ) );

    const auto unused_floats = stl::collect::vector<Array>(
        _df.unused_floats() | std::views::transform( to_float_or_ts_array ) );

    const auto unused_strings = stl::collect::vector<Array>(
        _df.unused_strings() | std::views::transform( to_string_array ) );

    return stl::join::vector<Array>(
        { categoricals,
          join_keys,
          numericals,
          targets,
          text,
          time_stamps,
          unused_floats,
          unused_strings } );
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Schema> ArrowHandler::df_to_schema(
    const containers::DataFrame& _df ) const
{
    using Field = std::shared_ptr<arrow::Field>;

    const auto to_float_or_ts_field = [&_df]( const auto& _col ) -> Field {
        if ( _col.unit().find( "time stamp" ) != std::string::npos )
            {
                return arrow::field(
                    _col.name(), arrow::timestamp( arrow::TimeUnit::NANO ) );
            }

        return arrow::field( _col.name(), arrow::float64() );
    };

    const auto to_string_field = []( const auto& _col ) -> Field {
        return arrow::field( _col.name(), arrow::utf8() );
    };

    const auto categoricals = stl::collect::vector<Field>(
        _df.categoricals() | std::views::transform( to_string_field ) );

    const auto join_keys = stl::collect::vector<Field>(
        _df.join_keys() | std::views::transform( to_string_field ) );

    const auto numericals = stl::collect::vector<Field>(
        _df.numericals() | std::views::transform( to_float_or_ts_field ) );

    const auto targets = stl::collect::vector<Field>(
        _df.targets() | std::views::transform( to_float_or_ts_field ) );

    const auto text = stl::collect::vector<Field>(
        _df.text() | std::views::transform( to_string_field ) );

    const auto time_stamps = stl::collect::vector<Field>(
        _df.time_stamps() | std::views::transform( to_float_or_ts_field ) );

    const auto unused_floats = stl::collect::vector<Field>(
        _df.unused_floats() | std::views::transform( to_float_or_ts_field ) );

    const auto unused_strings = stl::collect::vector<Field>(
        _df.unused_strings() | std::views::transform( to_string_field ) );

    const auto all_fields = stl::join::vector<Field>(
        { categoricals,
          join_keys,
          numericals,
          targets,
          text,
          time_stamps,
          unused_floats,
          unused_strings } );

    return arrow::schema( all_fields );
}

// ----------------------------------------------------------------------------

parquet::Compression::type ArrowHandler::parse_compression(
    const std::string& _compression ) const
{
    if ( _compression == "brotli" )
        {
            return parquet::Compression::BROTLI;
        }

    if ( _compression == "gzip" )
        {
            return parquet::Compression::GZIP;
        }

    if ( _compression == "lz4" )
        {
            return parquet::Compression::LZ4;
        }

    if ( _compression == "snappy" )
        {
            return parquet::Compression::SNAPPY;
        }

    if ( _compression == "zstd" )
        {
            return parquet::Compression::ZSTD;
        }

    throw std::invalid_argument(
        "Unknown compression format: '" + _compression + "'." );
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Table> ArrowHandler::read_parquet(
    const std::string& _filename ) const
{
    const auto pool = arrow::default_memory_pool();

    const auto result = arrow::io::ReadableFile::Open( _filename );

    if ( !result.ok() )
        {
            throw std::runtime_error(
                "Could not find or open file '" + _filename +
                "': " + result.status().message() );
        }

    const auto input = result.ValueOrDie();

    std::unique_ptr<parquet::arrow::FileReader> arrow_reader;

    auto status = parquet::arrow::OpenFile( input, pool, &arrow_reader );

    if ( !status.ok() )
        {
            throw std::runtime_error(
                "Could not open parquet file '" + _filename +
                "': " + status.message() );
        }

    std::shared_ptr<arrow::Table> table;

    status = arrow_reader->ReadTable( &table );

    if ( !status.ok() )
        {
            throw std::runtime_error(
                "Could not read table: " + status.message() );
        }

    return table;
}

// ----------------------------------------------------------------------------

std::shared_ptr<arrow::Table> ArrowHandler::recv_table(
    Poco::Net::StreamSocket* _socket ) const
{
    const auto input_stream =
        std::make_shared<ArrowSocketInputStream>( _socket );

    const auto stream_reader_result =
        arrow::ipc::RecordBatchStreamReader::Open( input_stream );

    if ( !stream_reader_result.ok() )
        {
            throw std::runtime_error( stream_reader_result.status().message() );
        }

    const auto stream_reader = stream_reader_result.ValueOrDie();

    const auto table_result =
        arrow::Table::FromRecordBatchReader( stream_reader.get() );

    if ( !table_result.ok() )
        {
            throw std::runtime_error( table_result.status().message() );
        }

    return table_result.ValueOrDie();
}

// ----------------------------------------------------------------------------

containers::DataFrame ArrowHandler::table_to_df(
    const std::shared_ptr<arrow::Table>& _table,
    const std::string& _name,
    const containers::Schema& _schema ) const
{
    throw_unless( _table, "No table passed" );

    const auto schema = _table->schema();

    throw_unless( schema, "_table has no schema" );

    const auto to_int_column =
        []( const containers::Column<strings::String>& _col,
            const std::shared_ptr<containers::Encoding>& _encoding )
        -> containers::Column<Int> {
        assert_true( _encoding );
        const auto data_ptr =
            std::make_shared<std::vector<Int>>( _col.nrows() );
        for ( size_t i = 0; i < _col.nrows(); ++i )
            {
                ( *data_ptr )[i] = ( *_encoding )[_col[i]];
            }
        return containers::Column<Int>( data_ptr, _col.name() );
    };

    auto df = containers::DataFrame( _name, categories_, join_keys_encoding_ );

    for ( const auto& colname : _schema.categoricals_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            const auto col = to_int_column(
                to_column<strings::String>( colname, field, arr ),
                categories_ );
            df.add_int_column( col, containers::DataFrame::ROLE_CATEGORICAL );
        }

    for ( const auto& colname : _schema.join_keys_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            const auto col = to_int_column(
                to_column<strings::String>( colname, field, arr ),
                join_keys_encoding_ );
            df.add_int_column( col, containers::DataFrame::ROLE_JOIN_KEY );
        }

    for ( const auto& colname : _schema.numericals_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_float_column(
                to_column<Float>( colname, field, arr ),
                containers::DataFrame::ROLE_NUMERICAL );
        }

    for ( const auto& colname : _schema.targets_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_float_column(
                to_column<Float>( colname, field, arr ),
                containers::DataFrame::ROLE_TARGET );
        }

    for ( const auto& colname : _schema.text_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_string_column(
                to_column<strings::String>( colname, field, arr ),
                containers::DataFrame::ROLE_TEXT );
        }

    for ( const auto& colname : _schema.time_stamps_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_float_column(
                to_column<Float>( colname, field, arr ),
                containers::DataFrame::ROLE_TIME_STAMP );
        }

    for ( const auto& colname : _schema.unused_floats_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_float_column(
                to_column<Float>( colname, field, arr ),
                containers::DataFrame::ROLE_UNUSED_FLOAT );
        }

    for ( const auto& colname : _schema.unused_strings_ )
        {
            const auto field = schema->GetFieldByName( colname );
            const auto arr = _table->GetColumnByName( colname );
            df.add_string_column(
                to_column<strings::String>( colname, field, arr ),
                containers::DataFrame::ROLE_UNUSED_STRING );
        }

    return df;
}

// ----------------------------------------------------------------------------

void ArrowHandler::send_array(
    const std::shared_ptr<arrow::ChunkedArray>& _array,
    const std::shared_ptr<arrow::Field>& _field,
    Poco::Net::StreamSocket* _socket ) const
{
    const auto schema = arrow::schema( { _field } );

    const auto table = arrow::Table::Make( schema, { _array }, -1 );

    send_table( table, _socket );
}

// ----------------------------------------------------------------------------

void ArrowHandler::send_table(
    const std::shared_ptr<arrow::Table>& _table,
    Poco::Net::StreamSocket* _socket ) const
{
    assert_true( _table );

    const auto stream = std::make_shared<ArrowSocketOutputStream>( _socket );

    const auto writer_result =
        arrow::ipc::MakeStreamWriter( stream, _table->schema() );

    if ( !writer_result.status().ok() )
        {
            throw std::runtime_error( writer_result.status().message() );
        }

    const auto writer = writer_result.ValueOrDie();

    communication::Sender::send_string( "Success!", _socket );

    auto status = writer->WriteTable( *_table );

    if ( !status.ok() )
        {
            throw std::runtime_error( status.message() );
        }

    status = writer->Close();

    if ( !status.ok() )
        {
            throw std::runtime_error( status.message() );
        }
}

// ----------------------------------------------------------------------------

void ArrowHandler::to_parquet(
    const std::shared_ptr<arrow::Table>& _table,
    const std::string& _filename,
    const std::string& _compression ) const
{
    const auto filename = _filename.find( ".parquet" ) == std::string::npos
                              ? _filename + ".parquet"
                              : _filename;

    const auto result = arrow::io::FileOutputStream::Open( filename );

    if ( !result.ok() )
        {
            throw std::runtime_error(
                "Could not create file '" + filename +
                "': " + result.status().message() );
        }

    const auto outfile = result.ValueOrDie();

    parquet::WriterProperties::Builder builder;

    builder.compression( parse_compression( _compression ) );

    const auto props = builder.build();

    assert_true( _table );

    const auto status = parquet::arrow::WriteTable(
        *_table, arrow::default_memory_pool(), outfile, 100000, props );

    if ( !status.ok() )
        {
            throw std::runtime_error(
                "Could not write table: " + status.message() );
        }
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_boolean_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto boolean_to_float = []( const bool val ) -> Float {
        return val ? 1.0 : 0.0;
    };

    // ------------------------------------------------------------------------

    const auto transform = [boolean_to_float,
                            _out]( const arrow::BooleanArray& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = NAN;
                    }
                else
                    {
                        *( _out + i ) = boolean_to_float( chunk.Value( i ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::boolean() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::BooleanArray>( _chunk );
            assert_true( chunk );
            transform( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_boolean_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_boolean_chunk =
        [_out]( const arrow::BooleanArray& chunk ) {
            for ( std::int64_t i = 0; i < chunk.length(); ++i )
                {
                    if ( chunk.IsNull( i ) )
                        {
                            *( _out + i ) = strings::String( "NULL" );
                        }
                    else
                        {
                            *( _out + i ) = strings::String(
                                io::Parser::to_string( chunk.Value( i ) ) );
                        }
                }
        };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::boolean() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::BooleanArray>( _chunk );
            assert_true( chunk );
            transform_boolean_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_float_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_float_chunk = [_out]( const auto& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = strings::String( "NULL" );
                    }
                else
                    {
                        *( _out + i ) = strings::String( io::Parser::to_string(
                            static_cast<Float>( chunk.Value( i ) ) ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::float16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::HalfFloatArray>( _chunk );
            assert_true( chunk );
            transform_float_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::float32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::FloatArray>( _chunk );
            assert_true( chunk );
            transform_float_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::float64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::DoubleArray>( _chunk );
            assert_true( chunk );
            transform_float_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_int_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_int_chunk = [_out]( const auto& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = strings::String( "NULL" );
                    }
                else
                    {
                        *( _out + i ) = strings::String(
                            std::to_string( chunk.Value( i ) ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::uint8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt8Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int8Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt16Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int16Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt32Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int32Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt64Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int64Array>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else if (
        _field_type.name() ==
        arrow::duration( arrow::TimeUnit::SECOND )
            ->name() )  // the name will always be 'duration', regardless of the
                        // TimeUnit.
        {
            const auto chunk =
                std::static_pointer_cast<arrow::DurationArray>( _chunk );
            assert_true( chunk );
            transform_int_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_null_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::null() ) )
        {
            std::fill( _out, _out + _chunk->length(), NAN );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_null_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::null() ) )
        {
            std::fill( _out, _out + _chunk->length(), "NULL" );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_numeric_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_numeric_chunk = [_out]( const auto& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = NAN;
                    }
                else
                    {
                        *( _out + i ) = static_cast<Float>( chunk.Value( i ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::uint8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt8Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int8Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt16Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int16Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt32Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int32Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::uint64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::UInt64Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::int64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Int64Array>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::float16() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::HalfFloatArray>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::float32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::FloatArray>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::float64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::DoubleArray>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else if (
        _field_type.name() ==
        arrow::duration( arrow::TimeUnit::SECOND )
            ->name() )  // the name will always be 'duration', regardless of the
                        // TimeUnit.
        {
            const auto chunk =
                std::static_pointer_cast<arrow::DurationArray>( _chunk );
            assert_true( chunk );
            transform_numeric_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_string_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto string_to_float = []( const std::string& str ) -> Float {
        const auto [val, success] = io::Parser::to_double( str );
        return success ? val : NAN;
    };

    // ------------------------------------------------------------------------

    const auto transform_string_chunk = [string_to_float,
                                         _out]( const auto& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = NAN;
                    }
                else
                    {
                        *( _out + i ) = string_to_float( chunk.GetString( i ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::utf8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::StringArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::large_utf8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::LargeStringArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::binary() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::BinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::large_binary() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::LargeBinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( arrow::is_fixed_size_binary( _field_type.id() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::FixedSizeBinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_string_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_string_chunk = [_out]( const auto& chunk ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = strings::String( "NULL" );
                    }
                else
                    {
                        *( _out + i ) = strings::String( chunk.GetString( i ) );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::utf8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::StringArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::large_utf8() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::LargeStringArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::binary() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::BinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( _field_type.Equals( arrow::large_binary() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::LargeBinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else if ( arrow::is_fixed_size_binary( _field_type.id() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::FixedSizeBinaryArray>( _chunk );
            assert_true( chunk );
            transform_string_chunk( *chunk );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_time_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_time_chunk = [_out](
                                          const auto& chunk,
                                          const Float& _factor ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = NAN;
                    }
                else
                    {
                        *( _out + i ) =
                            static_cast<Float>( chunk.Value( i ) ) / _factor;
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::SECOND ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::MILLI ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::MICRO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e6 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::NANO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e9 );
        }
    else if ( _field_type.Equals( arrow::time32( arrow::TimeUnit::SECOND ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 );
        }
    else if ( _field_type.Equals( arrow::time32( arrow::TimeUnit::MILLI ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else if ( _field_type.Equals( arrow::time64( arrow::TimeUnit::MICRO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e6 );
        }
    else if ( _field_type.Equals( arrow::time64( arrow::TimeUnit::NANO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e9 );
        }
    else if ( _field_type.Equals( arrow::date32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Date32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 / 86400.0 );
        }
    else if ( _field_type.Equals( arrow::date64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Date64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

bool ArrowHandler::write_time_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    assert_true( _chunk );

    // ------------------------------------------------------------------------

    const auto transform_time_chunk = [_out](
                                          const auto& chunk,
                                          const Float& _factor ) {
        for ( std::int64_t i = 0; i < chunk.length(); ++i )
            {
                if ( chunk.IsNull( i ) )
                    {
                        *( _out + i ) = "NULL";
                    }
                else
                    {
                        *( _out + i ) = io::Parser::ts_to_string(
                            static_cast<Float>( chunk.Value( i ) ) / _factor );
                    }
            }
    };

    // ------------------------------------------------------------------------

    if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::SECOND ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::MILLI ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::MICRO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e6 );
        }
    else if ( _field_type.Equals( arrow::timestamp( arrow::TimeUnit::NANO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::TimestampArray>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e9 );
        }
    else if ( _field_type.Equals( arrow::time32( arrow::TimeUnit::SECOND ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 );
        }
    else if ( _field_type.Equals( arrow::time32( arrow::TimeUnit::MILLI ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else if ( _field_type.Equals( arrow::time64( arrow::TimeUnit::MICRO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e6 );
        }
    else if ( _field_type.Equals( arrow::time64( arrow::TimeUnit::NANO ) ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Time64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e9 );
        }
    else if ( _field_type.Equals( arrow::date32() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Date32Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0 / 86400.0 );
        }
    else if ( _field_type.Equals( arrow::date64() ) )
        {
            const auto chunk =
                std::static_pointer_cast<arrow::Date64Array>( _chunk );
            assert_true( chunk );
            transform_time_chunk( *chunk, 1.0e3 );
        }
    else
        {
            return false;
        }

    // ------------------------------------------------------------------------

    return true;
}

// ----------------------------------------------------------------------------

void ArrowHandler::write_to_float_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    Float* _out ) const
{
    // ------------------------------------------------------------------------

    bool success =
        write_boolean_to_float_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_null_to_float_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_numeric_to_float_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_string_to_float_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_time_to_float_column( _chunk, _name, _field_type, _out );

    // ------------------------------------------------------------------------

    if ( !success )
        {
            throw std::invalid_argument(
                "Unsupported field type for field '" + _name +
                "': " + _field_type.name() + "." );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------

void ArrowHandler::write_to_string_column(
    const std::shared_ptr<arrow::Array>& _chunk,
    const std::string& _name,
    const arrow::DataType& _field_type,
    strings::String* _out ) const
{
    // ------------------------------------------------------------------------

    bool success =
        write_boolean_to_string_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_float_to_string_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_int_to_string_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_null_to_string_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_string_to_string_column( _chunk, _name, _field_type, _out );

    success = success ||
              write_time_to_string_column( _chunk, _name, _field_type, _out );

    // ------------------------------------------------------------------------

    if ( !success )
        {
            throw std::invalid_argument(
                "Unsupported field type for field '" + _name +
                "': " + _field_type.name() + "." );
        }

    // ------------------------------------------------------------------------
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

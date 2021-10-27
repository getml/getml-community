#ifndef ENGINE_HANDLERS_ARROWHANDLER_HPP_
#define ENGINE_HANDLERS_ARROWHANDLER_HPP_

namespace engine
{
namespace handlers
{
// -------------------------------------------------------------------------

class ArrowHandler
{
   public:
    ArrowHandler(
        const std::shared_ptr<containers::Encoding>& _categories,
        const std::shared_ptr<containers::Encoding>& _join_keys_encoding )
        : categories_( _categories ), join_keys_encoding_( _join_keys_encoding )
    {
        assert_true( categories_ );
        assert_true( join_keys_encoding_ );
    }

    ~ArrowHandler() = default;

   public:
    /// Extracts an arrow::Table from a DataFrame.
    std::shared_ptr<arrow::Table> df_to_table(
        const containers::DataFrame& _df ) const;

    /// Reads a parquet file and returns an arrow::Table.
    std::shared_ptr<arrow::Table> read_parquet(
        const std::string& _filename ) const;

    /// Receives an arrow::Table from a stream socket.
    template <class T>
    containers::Column<T> recv_column(
        const std::string& _colname, Poco::Net::StreamSocket* _socket ) const;

    /// Receives an arrow::Table from a stream socket.
    std::shared_ptr<arrow::Table> recv_table(
        Poco::Net::StreamSocket* _socket ) const;

    /// Send an array via the socket.
    void send_array(
        const std::shared_ptr<arrow::ChunkedArray>& _array,
        const std::shared_ptr<arrow::Field>& _field,
        Poco::Net::StreamSocket* _socket ) const;

    /// Sends a table via the socket.
    void send_table(
        const std::shared_ptr<arrow::Table>& _table,
        Poco::Net::StreamSocket* _socket ) const;

    /// Stores an arrow table as a parquet file.
    void to_parquet(
        const std::shared_ptr<arrow::Table>& _table,
        const std::string& _filename,
        const std::string& _compression ) const;

    /// Extracts a DataFrame from an arrow::Table.
    containers::DataFrame table_to_df(
        const std::shared_ptr<arrow::Table>& _table,
        const std::string& _name,
        const containers::Schema& _schema ) const;

   private:
    /// Extracts the arrow::Schema from a DataFrame.
    std::shared_ptr<arrow::Schema> df_to_schema(
        const containers::DataFrame& _df ) const;

    /// Extracts the arrays from a DataFrame.
    std::vector<std::shared_ptr<arrow::ChunkedArray>> extract_arrays(
        const containers::DataFrame& _df ) const;

    /// Returns the appropriate compression format.
    parquet::Compression::type parse_compression(
        const std::string& _compression ) const;

    /// Converts a chunked array to a float column.
    template <class T>
    containers::Column<T> to_column(
        const std::string& _name,
        const std::shared_ptr<arrow::ChunkedArray>& _arr ) const;

    /// Writes a boolean chunk to a float column. Returns true on success.
    bool write_boolean_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a boolean chunk to a string column. Returns true on success.
    bool write_boolean_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes a float chunk to a string column. Returns true on success.
    bool write_float_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes an int chunk to a string column. Returns true on success.
    bool write_int_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes a null chunk to a float column. Returns true on success.
    bool write_null_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a null chunk to a float column. Returns true on success.
    bool write_null_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes a numeric chunk to a float column. Returns true on success.
    bool write_numeric_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a string chunk to a float column. Returns true on success.
    bool write_string_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a string chunk to a string column. Returns true on success.
    bool write_string_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes a time chunk to a float column. Returns true on success.
    bool write_time_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a time chunk to a string column. Returns true on success.
    bool write_time_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

    /// Writes a chunk to a float column.
    void write_to_float_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        Float* _out ) const;

    /// Writes a chunk to a string column.
    void write_to_string_column(
        const std::shared_ptr<arrow::Array>& _chunk,
        const std::string& _name,
        strings::String* _out ) const;

   private:
    /// Encodes the categories used.
    const std::shared_ptr<containers::Encoding> categories_;

    /// Encodes the join keys used.
    const std::shared_ptr<containers::Encoding> join_keys_encoding_;
};

// -------------------------------------------------------------------------
// -------------------------------------------------------------------------

template <class T>
containers::Column<T> ArrowHandler::recv_column(
    const std::string& _colname, Poco::Net::StreamSocket* _socket ) const
{
    const auto table = recv_table( _socket );

    assert_true( table );

    const auto schema = table->schema();

    assert_true( schema );

    const auto arr = table->GetColumnByName( _colname );

    return to_column<T>( _colname, arr );
}

// ----------------------------------------------------------------------------

template <class T>
containers::Column<T> ArrowHandler::to_column(
    const std::string& _name,
    const std::shared_ptr<arrow::ChunkedArray>& _arr ) const
{
    static_assert(
        std::is_same<T, Float>() || std::is_same<T, strings::String>(),
        "T must be Float or String" );

    if ( !_arr )
        {
            throw std::runtime_error( "Column '" + _name + "' not found!" );
        }

    const auto data_ptr = std::make_shared<std::vector<T>>( _arr->length() );

    for ( std::int64_t nchunk = 0, begin = 0; nchunk < _arr->num_chunks();
          ++nchunk )
        {
            const auto chunk = _arr->chunk( nchunk );

            throw_unless(
                chunk, "Could not extract chunk from field '" + _name + "'!" );

            throw_unless(
                chunk->type(),
                "Could not extract type from field '" + _name + "'!" );

            throw_unless(
                begin + chunk->length() <= _arr->length(),
                "Sum of chunks greater than the length of the chunked array in "
                "field '" +
                    _name + "'!" );

            if constexpr ( std::is_same<T, Float>() )
                {
                    write_to_float_column(
                        chunk, _name, data_ptr->data() + begin );
                }

            if constexpr ( std::is_same<T, strings::String>() )
                {
                    write_to_string_column(
                        chunk, _name, data_ptr->data() + begin );
                }

            begin += chunk->length();
        }

    return containers::Column<T>( data_ptr, _name );
}

// -------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

// -------------------------------------------------------------------------

#endif  // ENGINE_HANDLERS_ARROWHANDLER_HPP_

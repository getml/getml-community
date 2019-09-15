#ifndef DATABASE_POSTGRES_HPP_
#define DATABASE_POSTGRES_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class Postgres : public Connector
{
    // -------------------------------

   public:
    Postgres(
        const Poco::JSON::Object& _obj,
        const std::vector<std::string>& _time_formats )
        : connection_string_( make_connection_string( _obj ) ),
          time_formats_( _time_formats )
    {
    }

    Postgres( const std::vector<std::string>& _time_formats )
        : time_formats_( _time_formats )
    {
    }

    ~Postgres() = default;

    // -------------------------------

   public:
    /// Returns the names of the table columns.
    std::vector<std::string> get_colnames(
        const std::string& _table ) const final;

    /// Returns the types of the table columns.
    std::vector<csv::Datatype> get_coltypes(
        const std::string& _table,
        const std::vector<std::string>& _colnames ) const;

    /// Returns the content of a table in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::string& _tname,
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) final;

    /// Lists the name of the tables held in the database.
    std::vector<std::string> list_tables() final;

    /// Reads a CSV file or another data source into a table.
    void read(
        const std::string& _table,
        const bool _header,
        const size_t _skip,
        csv::Reader* _reader ) final;

    // -------------------------------

   public:
    /// Returns the dialect of the connector.
    std::string dialect() const final { return "postgres"; }

    /// Drops a table and cleans up, if necessary.
    void drop_table( const std::string& _tname ) final
    {
        execute( "DROP TABLE \"" + _tname + "\";" );
    }

    /// Executes an SQL query.
    void execute( const std::string& _sql ) final
    {
        auto conn = make_connection();
        exec( _sql, conn.get() );
        if ( PQtransactionStatus( conn.get() ) == PQTRANS_INTRANS )
            {
                exec( "COMMIT", conn.get() );
            }
    }

    /// Returns the number of rows in the table signified by _tname.
    std::int32_t get_nrows( const std::string& _tname ) final
    {
        return select( {"COUNT(*)"}, _tname, "" )->get_int();
    }

    /// Returns a shared_ptr containing a PostgresIterator.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) final
    {
        return std::make_shared<PostgresIterator>(
            make_connection(), _colnames, time_formats_, _tname, _where );
    }

    /// Returns a shared_ptr containing a PostgresIterator.
    std::shared_ptr<Iterator> select( const std::string& _sql ) final
    {
        return std::make_shared<PostgresIterator>(
            make_connection(), _sql, time_formats_ );
    }

    /// Returns the time formats used.
    const std::vector<std::string>& time_formats() const
    {
        return time_formats_;
    }

    // -------------------------------

   private:
    /// Makes sure that the colnames of the CSV file match the colnames of the
    /// target table.
    /*    void check_colnames(
            const std::vector<std::string>& _colnames, csv::Reader* _reader );*/

    /// Returns the csv::Datatype associated with a oid.
    csv::Datatype interpret_oid( Oid _oid ) const;

    /// Prepares a shared ptr to the connection object
    /// Called by the constructor.
    static std::string make_connection_string( const Poco::JSON::Object& _obj );

    /// Turns a line into a buffer to be read by PQputCopyData.
    std::string make_buffer(
        const std::vector<std::string>& _line,
        const std::vector<csv::Datatype>& _coltypes,
        const char _sep,
        const char _quotechar );

    /// Parses a raw field according to its datatype.
    std::string parse_field(
        const std::string& _raw_field,
        const csv::Datatype _datatype,
        const char _sep,
        const char _quotechar ) const;

    // -------------------------------

   private:
    /// Executes and SQL command given a connection.
    std::shared_ptr<PGresult> exec(
        const std::string& _sql, PGconn* _conn ) const
    {
        auto raw_ptr = PQexec( _conn, _sql.c_str() );

        auto result = std::shared_ptr<PGresult>( raw_ptr, PQclear );

        const auto status = PQresultStatus( result.get() );

        if ( status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK )
            {
                const std::string error_msg =
                    PQresultErrorMessage( result.get() );

                throw std::runtime_error(
                    "Executing command in postgres failed: " + error_msg );
            }

        return result;
    }

    /// Returns a new connection based on the connection_string_
    std::shared_ptr<PGconn> make_connection() const
    {
        auto raw_ptr = PQconnectdb( connection_string_.c_str() );

        auto conn = std::shared_ptr<PGconn>( raw_ptr, PQfinish );

        if ( PQstatus( conn.get() ) != CONNECTION_OK )
            {
                throw std::runtime_error(
                    std::string( "Connection to postgres failed:" ) +
                    PQerrorMessage( conn.get() ) );
            }

        return conn;
    }

    /// List of all typnames that will be interpreted as double precision.
    static std::vector<std::string> typnames_double_precision()
    {
        return {
            "float4", "float8", "_float4", "_float8", "numeric", "_numeric"};
    }

    /// List of all typnames that will be interpreted as int.
    static std::vector<std::string> typnames_int()
    {
        return {"int8", "int2", "int4", "_int2", "_int4"};
    }

    // -------------------------------

   private:
    /// String containing the meta-information related to the connection.
    std::string connection_string_;

    /// Vector containing the time formats.
    const std::vector<std::string> time_formats_;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_POSTGRES_HPP_

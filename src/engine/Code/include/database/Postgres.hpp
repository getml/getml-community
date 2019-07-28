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
    /// Returns the content of a table in a format that is compatible
    /// with the DataTables.js server-side processing API.
    Poco::JSON::Object get_content(
        const std::string& _tname,
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) final
    {
        assert( false && "ToDo" );
        return Poco::JSON::Object();
    };

    /// Reads a CSV file into a table.
    void read_csv(
        const std::string& _table,
        const bool _header,
        csv::Reader* _reader ) final;

    /// Returns the names of the table columns.
    std::vector<std::string> get_colnames(
        const std::string& _table ) const final;

    /// Returns the types of the table columns.
    std::vector<csv::Datatype> get_coltypes(
        const std::string& _table,
        const std::vector<std::string>& _colnames ) const;

    /// Lists the name of the tables held in the database.
    std::vector<std::string> list_tables() final
    {
        assert( false && "ToDo" );
        return std::vector<std::string>( 0 );
    };

    /// Returns a shared_ptr containing a Sqlite3Iterator where
    /// _join_key is any one of _values.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _join_key,
        const std::vector<std::string>& _values ) final
    {
        assert( false && "ToDo" );
        return std::shared_ptr<Iterator>();
    };

    // -------------------------------

   public:
    /// Returns the dialect of the connector.
    std::string dialect() const final { return "postgres"; }

    /// Drops a table and cleans up, if necessary.
    void drop_table( const std::string& _tname ) final
    {
        execute( "DROP TABLE \"" + _tname + "\"; VACUUM;" );
    }

    /// Executes an SQL query.
    void execute( const std::string& _sql ) final
    {
        auto connection = make_connection();
        auto work = pqxx::work( *connection );
        work.exec( _sql );
        work.commit();
    }

    /// Returns the number of rows in the table signified by _tname.
    std::int32_t get_nrows( const std::string& _tname ) final
    {
        return select( {"COUNT(*)"}, _tname, "" )->get_int();
    }

    /// Returns a shared_ptr containing a Sqlite3Iterator.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) final
    {
        return std::make_shared<PostgresIterator>(
            make_connection(), _colnames, time_formats_, _tname, _where );
    }

    // -------------------------------

   private:
    /// Makes sure that the colnames of the CSV file match the colnames of the
    /// target table.
    /*    void check_colnames(
            const std::vector<std::string>& _colnames, csv::Reader* _reader );

        /// Inserts a single line from a CSV file into a table.
        void insert_line(
            const std::vector<std::string>& _line,
            const std::vector<csv::Datatype>& _coltypes,
            sqlite3_stmt* stmt ) const;

        /// Inserts a column in double format.
        void insert_double(
            const std::vector<std::string>& _line,
            const int _colnum,
            sqlite3_stmt* _stmt ) const;

        /// Inserts a column in int format.
        void insert_int(
            const std::vector<std::string>& _line,
            const int _colnum,
            sqlite3_stmt* _stmt ) const;

        /// Inserts a column in text format.
        void insert_text(
            const std::vector<std::string>& _line,
            const int _colnum,
            sqlite3_stmt* _stmt ) const;*/

    /// Returns the csv::Datatype associated with a oid.
    csv::Datatype interpret_oid( pqxx::oid _oid ) const;

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

    /// Prepares an insert statement for reading in CSV data.
    /*  std::unique_ptr<sqlite3_stmt, int ( * )( sqlite3_stmt* )>
      make_insert_statement(
          const std::string& _table,
          const std::vector<std::string>& _colnames ) const;*/

    // -------------------------------

   private:
    /// Returns a new connection based on the connection_string_
    std::shared_ptr<pqxx::connection> make_connection() const
    {
        if ( connection_string_ == "" )
            {
                return std::make_shared<pqxx::connection>();
            }
        else
            {
                return std::make_shared<pqxx::connection>( connection_string_ );
            }
    }

    /// Returns a new connection based on the connection_string_
    std::shared_ptr<PGconn> make_raw_connection() const
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

    /// List of all typnames that will be interpreted as a timestamp.
    static std::vector<std::string> typnames_timestamp()
    {
        return {"timestamp",
                "date",
                "time",
                "_timestamp",
                "_date",
                "_time",
                "timestamptz",
                "timetz",
                "_timestamptz",
                "_timetz"};
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

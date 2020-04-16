#ifndef DATABASE_ODBC_HPP_
#define DATABASE_ODBC_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class ODBC : public Connector
{
    // -------------------------------

   public:
    ODBC(
        const Poco::JSON::Object& _obj,
        const std::string& _passwd,
        const std::vector<std::string>& _time_formats )
        : env_( std::make_shared<ODBCEnv>() ),
          passwd_( _passwd ),
          server_name_(
              jsonutils::JSON::get_value<std::string>( _obj, "server_name_" ) ),
          time_formats_( _time_formats ),
          user_( jsonutils::JSON::get_value<std::string>( _obj, "user_" ) )
    {
        std::tie( escape_char1_, escape_char2_ ) = extract_escape_chars( _obj );
    }

    ODBC(
        const std::string& _passwd,
        const std::string& _server_name,
        const std::string& _user,
        const std::vector<std::string>& _time_formats,
        const char _escape_char1,
        const char _escape_char2 )
        : env_( std::make_shared<ODBCEnv>() ),
          escape_char1_( _escape_char1 ),
          escape_char2_( _escape_char2 ),
          passwd_( _passwd ),
          server_name_( _server_name ),
          time_formats_( _time_formats ),
          user_( _user )
    {
    }

    ~ODBC() = default;

    // -------------------------------

   public:
    /// Drops a table from the data base.
    void drop_table( const std::string& _tname ) final;

    /// Returns the names of the table columns.
    std::vector<std::string> get_colnames(
        const std::string& _table ) const final;

    /// Returns the types of the table columns.
    std::vector<io::Datatype> get_coltypes(
        const std::string& _table,
        const std::vector<std::string>& _colnames ) const final;

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
        io::Reader* _reader ) final
    {
        // TODO
    }

    // -------------------------------

   public:
    /// Returns the dialect of the connector.
    std::string dialect() const final { return "odbc"; }

    /// Executes an SQL query.
    void execute( const std::string& _query ) final
    {
        const auto conn = make_connection();
        ODBCStmt( *conn, _query );
    }

    /// Returns the number of rows in the table signified by _tname.
    std::int32_t get_nrows( const std::string& _tname ) final
    {
        return select( {"COUNT(*)"}, _tname, "" )->get_int();
    }

    /// Returns a shared_ptr containing a MySQLIterator.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) final
    {
        return std::make_shared<ODBCIterator>(
            make_connection(),
            _colnames,
            time_formats_,
            _tname,
            _where,
            escape_char1_,
            escape_char2_ );
    }

    /// Returns a shared_ptr containing an ORBCIterator.
    std::shared_ptr<Iterator> select( const std::string& _query ) final
    {
        return std::make_shared<ODBCIterator>(
            make_connection(), _query, time_formats_ );
    }

    /// Returns the time formats used.
    const std::vector<std::string>& time_formats() const
    {
        return time_formats_;
    }

    // -------------------------------

   private:
    /// Extract the escapte characters.
    std::pair<char, char> extract_escape_chars(
        const Poco::JSON::Object& _obj ) const;

    /// Returns the catalogs.
    std::vector<std::string> get_catalogs() const;

    /// Returns a list of all schemas, given the catalog
    std::vector<std::string> get_schemas( const std::string& _catalog ) const;

    /// Returns a list of all schemas, given the catalog and the schema.
    std::vector<std::string> get_tables(
        const std::string& _catalog, const std::string& _schema ) const;

    /// Parses a field for the CSV reader.
    io::Datatype interpret_field_type( const SQLSMALLINT _type ) const;

    /// Prepares a INSERT INTO .. VALUES ... query
    /// to insert a large CSV file.
    /*    std::string make_bulk_insert_query(
            const std::string& _table,
            const std::vector<std::string>& _colnames ) const;
*/
    /// Prepares a query to get the content of a table.
    std::string make_get_content_query(
        const std::string& _table,
        const std::vector<std::string>& _colnames,
        const std::int32_t _begin,
        const std::int32_t _end ) const;

    // -------------------------------

   private:
    /// Trivial accessor
    ODBCEnv& env()
    {
        assert_true( env_ );
        return *env_;
    }

    /// Trivial accessor
    const ODBCEnv& env() const
    {
        assert_true( env_ );
        return *env_;
    }

    /// Returns a new connection.
    std::shared_ptr<ODBCConn> make_connection() const
    {
        return std::make_shared<ODBCConn>(
            env(), server_name_, user_, passwd_ );
    }

    /// Helper function to turn a string into a ptr that can be passed to ODBC
    std::unique_ptr<SQLCHAR[]> to_ptr( const std::string& _str ) const
    {
        auto ptr = std::make_unique<SQLCHAR[]>( _str.size() + 1 );

        std::copy(
            reinterpret_cast<const SQLCHAR*>( _str.c_str() ),
            reinterpret_cast<const SQLCHAR*>( _str.c_str() ) + _str.size(),
            ptr.get() );

        return ptr;
    }

    // -------------------------------

   private:
    /// The environment handle.
    const std::shared_ptr<ODBCEnv> env_;

    /// The first escape character - used to envelop table, schema, column
    /// names. According to ANSI SQL this should be '"', but we don't rely on
    /// that.
    char escape_char1_;

    /// The second escape character - used to envelop table, schema, column
    /// names. According to ANSI SQL this should be '"', but we don't rely on
    /// that.
    char escape_char2_;

    /// The password used.
    const std::string passwd_;

    /// The server to be connect to.
    const std::string server_name_;

    /// Vector containing the time formats.
    const std::vector<std::string> time_formats_;

    /// The user name.
    const std::string user_;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_ODBC_HPP_


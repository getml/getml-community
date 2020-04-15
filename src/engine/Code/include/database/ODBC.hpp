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
    }

    ODBC(
        const std::string& _passwd,
        const std::string& _server_name,
        const std::string& _user,
        const std::vector<std::string>& _time_formats )
        : env_( std::make_shared<ODBCEnv>() ),
          passwd_( _passwd ),
          server_name_( _server_name ),
          time_formats_( _time_formats ),
          user_( _user )
    {
    }

    ~ODBC() = default;

    // -------------------------------

   public:
    /// Returns the names of the table columns.
    std::vector<std::string> get_colnames(
        const std::string& _table ) const final
    {
        const auto query =
            std::string( "SELECT * FROM `" + _table + "` LIMIT 1;" );
        const auto iter =
            ODBCIterator( make_connection(), query, time_formats_ );
        return iter.colnames();
    }

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
        const std::int32_t _length ) final
    {
        // TODO
        return Poco::JSON::Object();
    }

    /// Lists the name of the tables held in the database.
    std::vector<std::string> list_tables() final
    {
        // TODO
        return std::vector<std::string>();
    }

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

    /// Drops a table and cleans up, if necessary.
    void drop_table( const std::string& _tname ) final
    {
        // TODO
        // execute( "DROP TABLE `" + _tname + "`;" );
    }

    /// Executes an SQL query.
    void execute( const std::string& _sql ) final
    {
        // TODO
        /*const auto conn = make_connection();
        exec( _sql, conn );*/
    }

    /// Returns the number of rows in the table signified by _tname.
    std::int32_t get_nrows( const std::string& _tname ) final
    {
        // TODO
        return 0;
        // return select( {"COUNT(*)"}, _tname, "" )->get_int();
    }

    /// Returns a shared_ptr containing a MySQLIterator.
    std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) final
    {
        return std::make_shared<ODBCIterator>(
            make_connection(), _colnames, time_formats_, _tname, _where );
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
    /// Executes and SQL command given a connection.
    std::shared_ptr<MYSQL_RES> exec(
        const std::string& _sql, const std::shared_ptr<MYSQL>& _conn ) const;

    /// Parses a field for the CSV reader.
    io::Datatype interpret_field_type( const SQLSMALLINT _type ) const;

    /// Prepares a INSERT INTO .. VALUES ... query
    /// to insert a large CSV file.
    /*    std::string make_bulk_insert_query(
            const std::string& _table,
            const std::vector<std::string>& _colnames ) const;

        /// Prepares a query to get the content of a table.
        std::string make_get_content_query(
            const std::string& _table,
            const std::vector<std::string>& _colnames,
            const std::int32_t _begin,
            const std::int32_t _end ) const;*/

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

    // -------------------------------

   private:
    /// The environment handle.
    const std::shared_ptr<ODBCEnv> env_;

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


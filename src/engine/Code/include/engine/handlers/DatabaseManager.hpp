#ifndef ENGINE_HANDLERS_DATABASEMANAGER_HPP_
#define ENGINE_HANDLERS_DATABASEMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class DatabaseManager
{
   private:
    typedef std::map<std::string, std::shared_ptr<database::Connector>>
        ConnectorMap;

   public:
    DatabaseManager(
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<const monitoring::Monitor>& _monitor )
        : logger_( _logger ),
          monitor_( _monitor ),
          read_write_lock_( std::make_shared<multithreading::ReadWriteLock>() )
    {
        connector_map_["default"] =
            std::make_shared<database::Sqlite3>( database::Sqlite3(
                "../database.db",
                {"%Y-%m-%dT%H:%M:%s%z",
                 "%Y/%m/%d %H:%M:%S",
                 "%Y-%m-%d %H:%M:%S"} ) );

        post_tables();
    }

    ~DatabaseManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Copy a table from one database connection to another.
    void copy_table(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Drops the table signified by _name.
    void drop_table(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Describes the connection signified by _name.
    void describe_connection(
        const std::string& _name, Poco::Net::StreamSocket* _socket ) const;

    /// Runs a query on the database.
    void execute( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Returns the contents of an SQL query in JSON format.
    void get( const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Lists the column names of the table signified by _name.
    void get_colnames(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sends the content of a table in a format that is compatible with
    /// DataTables.js server-side processing.
    void get_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the number of rows of the table signified by _name.
    void get_nrows(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns a list of all active connections.
    void list_connections( Poco::Net::StreamSocket* _socket ) const;

    /// Lists all tables contained in the database.
    void list_tables(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Creates a new database connector.
    void new_db(
        const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket );

    /// Sends the name of all tables currently held in the database to the
    /// monitor.
    void post_tables();

    /// Reads CSV files into the database.
    void read_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Reads CSV files located in an S3 bucket into the database.
    void read_s3(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sniffs one or several CSV files and returns the CREATE TABLE statement
    /// to the client.
    void sniff_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) const;

    /// Sniffs one or several CSV files in an S3 bucket and returns the CREATE
    /// TABLE statement to the client.
    void sniff_s3(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) const;

    /// Sniffs a table and generates suitable keyword arguments
    /// to build a DataFrame.
    void sniff_table(
        const std::string& _table_name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) const;

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    const std::shared_ptr<database::Connector> connector(
        const std::string& _name )
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        const auto conn = utils::Getter::get( _name, connector_map_ );
        assert_true( conn );
        return conn;
    }

    /// Trivial accessor
    const std::shared_ptr<const database::Connector> connector(
        const std::string& _name ) const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        const auto conn = utils::Getter::get( _name, connector_map_ );
        assert_true( conn );
        return conn;
    }

    /// Sets the S3 Access Key ID
    void set_s3_access_key_id( Poco::Net::StreamSocket* _socket ) const
    {
        const auto value = communication::Receiver::recv_string( _socket );
        goutils::S3::set_access_key_id( value );
        communication::Sender::send_string( "Success!", _socket );
    }

    /// Sets the S3 Access Key ID
    void set_s3_secret_access_key( Poco::Net::StreamSocket* _socket ) const
    {
        const auto value = communication::Receiver::recv_string( _socket );
        goutils::S3::set_secret_access_key( value );
        communication::Sender::send_string( "Success!", _socket );
    }

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    const monitoring::Logger& logger() { return *logger_; }

    // ------------------------------------------------------------------------

   private:
    /// Keeps the connectors to the databases.
    /// The type ConnectorMap is a private typedef.
    ConnectorMap connector_map_;

    /// For logging
    const std::shared_ptr<const monitoring::Logger> logger_;

    /// For communication with the monitor
    const std::shared_ptr<const monitoring::Monitor> monitor_;

    /// Protects the shared_ptr of the connector - the connector might have to
    /// implement its own locking strategy!
    const std::shared_ptr<multithreading::ReadWriteLock> read_write_lock_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATABASEMANAGER_HPP_

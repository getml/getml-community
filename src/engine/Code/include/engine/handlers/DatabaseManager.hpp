#ifndef ENGINE_HANDLERS_DATABASEMANAGER_HPP_
#define ENGINE_HANDLERS_DATABASEMANAGER_HPP_

namespace engine
{
namespace handlers
{
// ------------------------------------------------------------------------

class DatabaseManager
{
   public:
    DatabaseManager(
        const std::shared_ptr<const monitoring::Logger>& _logger,
        const std::shared_ptr<const monitoring::Monitor>& _monitor )
        : connector_( std::make_shared<database::Sqlite3>( database::Sqlite3(
              "../database.db",
              {"%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"} ) ) ),
          logger_( _logger ),
          monitor_( _monitor ),
          read_write_lock_( std::make_shared<multithreading::ReadWriteLock>() )
    {
        post_tables();
    }

    ~DatabaseManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Drops the table signified by _name.
    void drop_table(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Runs a query on the database.
    void execute( Poco::Net::StreamSocket* _socket );

    /// Lists the column names of the table signified by _name.
    void get_colnames(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Sends the content of a table in a format that is compatible with
    /// DataTables.js server-side processing.
    void get_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Returns the number of rows of the table signified by _name.
    void get_nrows(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Lists all tables contained in the database.
    void list_tables( Poco::Net::StreamSocket* _socket );

    /// Reads a CSV file into the database.
    void read_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

    /// Sniffs one or several CSV files and returns the CREATE TABLE statement
    /// to the client.
    void sniff_csv(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket ) const;

    // ------------------------------------------------------------------------

   public:
    /// Trivial accessor
    const std::shared_ptr<database::Connector> connector()
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        assert( connector_ );
        return connector_;
    }

    /// Trivial accessor
    const std::shared_ptr<const database::Connector> connector() const
    {
        multithreading::ReadLock read_lock( read_write_lock_ );
        assert( connector_ );
        return connector_;
    }

    /// Creates a new database connector.
    void new_db(
        const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket )
    {
        multithreading::WriteLock write_lock( read_write_lock_ );
        connector_ = database::DatabaseParser::parse( _cmd );
        write_lock.unlock();
        post_tables();
        communication::Sender::send_string( "Success!", _socket );
    }

    // ------------------------------------------------------------------------

   private:
    /// Sends the name of all tables currently held in the database to the
    /// monitor.
    std::string post_tables();

    // ------------------------------------------------------------------------

   private:
    /// Trivial accessor
    const monitoring::Logger& logger() { return *logger_; }

    // ------------------------------------------------------------------------

   private:
    /// Connector to the underlying database.
    std::shared_ptr<database::Connector> connector_;

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

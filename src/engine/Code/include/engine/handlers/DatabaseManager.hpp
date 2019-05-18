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
        const std::shared_ptr<database::Connector> _connector,
        const std::shared_ptr<const logging::Logger>& _logger )
        : connector_( _connector ), logger_( _logger )
    // monitor_( _monitor ),
    {
    }

    ~DatabaseManager() = default;

    // ------------------------------------------------------------------------

   public:
    /// Drops the table signified by _name.
    void drop_table(
        const std::string& _name, Poco::Net::StreamSocket* _socket );

    /// Runs a query on the database.
    void execute( Poco::Net::StreamSocket* _socket );

    /// Sends the content of a table in a format that is compatible with
    /// DataTables.js server-side processing.
    void get_content(
        const std::string& _name,
        const Poco::JSON::Object& _cmd,
        Poco::Net::StreamSocket* _socket );

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

   private:
    /// Trivial accessor
    const std::shared_ptr<database::Connector>& connector()
    {
        assert( connector_ );
        return connector_;
    }

    /// Trivial accessor
    const std::shared_ptr<const database::Connector> connector() const
    {
        assert( connector_ );
        return connector_;
    }

    /// Trivial accessor
    const logging::Logger& logger() { return *logger_; }

    // ------------------------------------------------------------------------

   private:
    /// Connector to the underlying database.
    const std::shared_ptr<database::Connector> connector_;

    /// For logging
    const std::shared_ptr<const logging::Logger> logger_;

    /// For communication with the monitor
    /// const std::shared_ptr<const logging::Monitor> monitor_;

    // ------------------------------------------------------------------------
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATABASEMANAGER_HPP_

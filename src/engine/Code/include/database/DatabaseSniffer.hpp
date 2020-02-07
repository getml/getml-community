#ifndef DATABASE_DATABASESNIFFER_HPP_
#define DATABASE_DATABASESNIFFER_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct DatabaseSniffer
{
    /// Returns the datatype associate
    static std::string sniff(
        const std::shared_ptr<Connector>& _conn,
        const std::string& _table_name );
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_DATABASESNIFFER_HPP_

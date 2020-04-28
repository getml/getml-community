#ifndef DATABASE_DATABASESNIFFER_HPP_
#define DATABASE_DATABASESNIFFER_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct DatabaseSniffer
{
    /// Returns the datatype associate
    static std::string sniff(
        const std::shared_ptr<const Connector>& _conn,
        const std::string& _dialect,
        const std::string& _source_table_name,
        const std::string& _target_table_name );
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_DATABASESNIFFER_HPP_

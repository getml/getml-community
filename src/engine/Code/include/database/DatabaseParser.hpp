#ifndef DATABASE_DATABASEPARSER_HPP_
#define DATABASE_DATABASEPARSER_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct DatabaseParser
{
    static constexpr const char* GREENPLUM = "greenplum";
    static constexpr const char* MARIADB = "mariadb";
    static constexpr const char* MYSQL = io::StatementMaker::MYSQL;
    static constexpr const char* ODBC_DIALECT = io::StatementMaker::ODBC;
    static constexpr const char* POSTGRES = io::StatementMaker::POSTGRES;
    static constexpr const char* SAP_HANA = io::StatementMaker::SAP_HANA;
    static constexpr const char* SQLITE3 = io::StatementMaker::SQLITE3;

    /// Given a Poco::JSON::Object, the DatabaseParser returns the correct
    /// database connector.
    static std::shared_ptr<Connector> parse(
        const Poco::JSON::Object& _obj, const std::string& _password );
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_DATABASEPARSER_HPP_

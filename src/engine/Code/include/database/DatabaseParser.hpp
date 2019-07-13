#ifndef DATABASE_DATABASEPARSER_HPP_
#define DATABASE_DATABASEPARSER_HPP_

namespace database
{
// ----------------------------------------------------------------------------

struct DatabaseParser
{
    /// Given a Poco::JSON::Object, the DatabaseParser returns the correct
    /// database connector.
    static std::shared_ptr<Connector> parse( const Poco::JSON::Object& _obj );
};

// ----------------------------------------------------------------------------
}  // namespace database

#endif  // DATABASE_DATABASEPARSER_HPP_

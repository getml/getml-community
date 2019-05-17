#ifndef DATABASE_CONNECTOR_HPP_
#define DATABASE_CONNECTOR_HPP_

namespace database
{
// ----------------------------------------------------------------------------

class Connector
{
    // -------------------------------

   public:
    Connector() {}

    virtual ~Connector() = default;

    // -------------------------------

   public:
    /// Describes the dialect used by the connector.
    virtual std::string dialect() const = 0;

    /// Drops a table and cleans up, if necessary.
    virtual void drop_table( const std::string& _tname ) = 0;

    /// Executes an SQL query.
    virtual void execute( const std::string& _sql ) = 0;

    /// Reads a CSV file into a table.
    virtual void read_csv(
        const std::string& _table,
        const bool _header,
        csv::Reader* _reader ) = 0;

    /// Returns a shared_ptr containing the corresponding iterator.
    virtual std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname ) = 0;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_CONNECTOR_HPP_

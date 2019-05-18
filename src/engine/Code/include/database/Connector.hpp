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

    /// Returns the content of a table in a format that is compatible
    /// with the DataTables.js server-side processing API.
    virtual Poco::JSON::Object get_content(
        const std::string& _tname,
        const std::int32_t _draw,
        const std::int32_t _start,
        const std::int32_t _length ) = 0;

    /// Returns the number of rows in the table signified by _tname.
    virtual std::int32_t get_nrows( const std::string& _tname ) = 0;

    /// Reads a CSV file into a table.
    virtual void read_csv(
        const std::string& _table,
        const bool _header,
        csv::Reader* _reader ) = 0;

    /// Returns a shared_ptr containing the corresponding iterator.
    virtual std::shared_ptr<Iterator> select(
        const std::vector<std::string>& _colnames,
        const std::string& _tname,
        const std::string& _where ) = 0;

    // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_CONNECTOR_HPP_

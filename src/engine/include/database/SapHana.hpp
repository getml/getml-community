#ifndef DATABASE_SAP_HANA_HPP_
#define DATABASE_SAP_HANA_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <memory>
#include <string>
#include <vector>

// ----------------------------------------------------------------------------

#include "io/io.hpp"
#include "jsonutils/jsonutils.hpp"

// ----------------------------------------------------------------------------

#include "database/Connector.hpp"
#include "database/DatabaseParser.hpp"
#include "database/Float.hpp"
#include "database/Int.hpp"
#include "database/Iterator.hpp"

// ----------------------------------------------------------------------------
namespace database {
// ----------------------------------------------------------------------------

class SapHana : public Connector {
  // -------------------------------

  typedef goutils::Helpers::RecordType RecordType;

  // -------------------------------

 public:
  SapHana(const Poco::JSON::Object& _obj, const std::string& _password,
          const std::vector<std::string>& _time_formats)
      : default_schema_(
            jsonutils::JSON::get_value<std::string>(_obj, "default_schema_")),
        host_(jsonutils::JSON::get_value<std::string>(_obj, "host_")),
        password_(_password),
        ping_interval_(jsonutils::JSON::get_value<Int>(_obj, "ping_interval_")),
        port_(jsonutils::JSON::get_value<Int>(_obj, "port_")),
        time_formats_(_time_formats),
        user_(jsonutils::JSON::get_value<std::string>(_obj, "user_")) {}

  SapHana(const std::string& _default_schema, const std::string& _host,
          const std::string& _password, const Int _ping_interval,
          const Int _port, const std::vector<std::string>& _time_formats,
          const std::string& _user)
      : default_schema_(_default_schema),
        host_(_host),
        password_(_password),
        ping_interval_(_ping_interval),
        port_(_port),
        time_formats_(_time_formats),
        user_(_user) {}

  ~SapHana() = default;

  // -------------------------------

 public:
  /// Returns a Poco::JSON::Object describing the connection.
  Poco::JSON::Object describe() const final;

  /// Executes an SQL query.
  void execute(const std::string& _sql) final;

  /// Returns the names of the table columns.
  std::vector<std::string> get_colnames(const std::string& _table) const final;

  /// Returns the types of the table columns.
  std::vector<io::Datatype> get_coltypes(
      const std::string& _table,
      const std::vector<std::string>& _colnames) const final;

  /// Returns the content of a table in a format that is compatible
  /// with the DataTables.js server-side processing API.
  Poco::JSON::Object get_content(const std::string& _tname,
                                 const std::int32_t _draw,
                                 const std::int32_t _start,
                                 const std::int32_t _length) final;

  /// Lists the name of the tables held in the database.
  std::vector<std::string> list_tables() final;

  /// Reads a CSV file or another data source into a table.
  void read(const std::string& _table, const size_t _skip,
            io::Reader* _reader) final;

  /// Returns a shared_ptr containing a GoUtilsIterator.
  std::shared_ptr<Iterator> select(const std::vector<std::string>& _colnames,
                                   const std::string& _tname,
                                   const std::string& _where) final;

  /// Returns a shared_ptr containing a GoUtilsIterator.
  std::shared_ptr<Iterator> select(const std::string& _sql) final;

  // -------------------------------

 public:
  /// Returns the dialect of the connector.
  std::string dialect() const final { return DatabaseParser::SAP_HANA; }

  /// Drops a table and cleans up, if necessary.
  void drop_table(const std::string& _tname) final {
    execute("DROP TABLE \"" + _tname + "\";");
  }

  /// Returns the number of rows in the table signified by _tname.
  std::int32_t get_nrows(const std::string& _tname) final {
    return select({"COUNT(*)"}, _tname, "")->get_int();
  }

  /// Returns the time formats used.
  const std::vector<std::string>& time_formats() const { return time_formats_; }

  // -------------------------------

 private:
  /// Extracts pointers from the current batch of data.
  std::vector<char*> extract_ptrs(
      const std::vector<typename SapHana::RecordType>& _batch) const;

  /// Generates a new batch of data we want to read into the database.
  std::vector<typename SapHana::RecordType> make_batch(
      io::Reader* _reader) const;

  /// Merge the procedures into one.
  std::vector<std::string> merge_procedures(
      const std::vector<std::string>& _splitted) const;

  /// Generates the query necessary to extract a batch of data.
  std::string make_get_content_query(const std::string& _table,
                                     const std::vector<std::string>& _colnames,
                                     const std::int32_t _begin,
                                     const std::int32_t _end) const;

  /// Split the procedures.
  std::vector<std::string> split(const std::string& _sql) const;

  // -------------------------------

 private:
  /// The default schema to use.
  const std::string default_schema_;

  /// The host address.
  const std::string host_;

  /// The password used.
  const std::string password_;

  /// Interval in seconds at which you
  /// want to ping the server.
  const Int ping_interval_;

  /// The port to be accessed.
  const Int port_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  /// The user name.
  const std::string user_;

  // -------------------------------
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_SAP_HANA_HPP_


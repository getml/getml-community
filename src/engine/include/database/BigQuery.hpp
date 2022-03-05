#ifndef DATABASE_BIGQUERY_HPP_
#define DATABASE_BIGQUERY_HPP_

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

namespace database {

class BigQuery : public Connector {
  typedef goutils::Helpers::RecordType RecordType;

 public:
  BigQuery(const Poco::JSON::Object& _obj,
           const std::vector<std::string>& _time_formats)
      : database_id_(
            jsonutils::JSON::get_value<std::string>(_obj, "database_id_")),
        google_application_credentials_(jsonutils::JSON::get_value<std::string>(
            _obj, "google_application_credentials_")),
        project_id_(
            jsonutils::JSON::get_value<std::string>(_obj, "project_id_")),
        time_formats_(_time_formats) {}

  BigQuery(const std::string& _project_id, const std::string& _database_id,
           const std::string& _google_application_credentials,
           const std::vector<std::string>& _time_formats)
      : database_id_(_database_id),
        google_application_credentials_(_google_application_credentials),
        project_id_(_project_id),
        time_formats_(_time_formats) {}

  ~BigQuery() = default;

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

 public:
  /// Returns the dialect of the connector.
  std::string dialect() const final { return DatabaseParser::BIGQUERY; }

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

 private:
  /// Extracts pointers from the current batch of data.
  std::vector<char*> extract_ptrs(const std::vector<RecordType>& _batch) const;

  /// Generates a new batch of data we want to read into the database.
  std::vector<RecordType> make_batch(io::Reader* _reader) const;

  /// Merge the procedures into one.
  std::vector<std::string> merge_procedures(
      const std::vector<std::string>& _splitted) const;

  /// Generates the query necessary to extract a batch of data.
  std::string make_get_content_query(const std::string& _table,
                                     const std::vector<std::string>& _colnames,
                                     const std::int32_t _begin,
                                     const std::int32_t _end) const;

 private:
  /// Mock select query (for retrieving colnames or coltypes)
  const std::string mock_query(const std::string& _table) const {
    return "SELECT * FROM `" + database_id_ + "." + _table + "` LIMIT 1;";
  }

 private:
  /// The database ID to use.
  const std::string database_id_;

  /// The location of the google application credentials
  const std::string google_application_credentials_;

  /// The project ID to use.
  const std::string project_id_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;
};

// ----------------------------------------------------------------------------

}  // namespace database

#endif  // DATABASE_BIGQUERY_HPP_


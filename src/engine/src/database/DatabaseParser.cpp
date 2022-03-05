#include "database/DatabaseParser.hpp"

#include "database/BigQuery.hpp"
#include "database/MySQL.hpp"
#include "database/ODBC.hpp"
#include "database/Postgres.hpp"
#include "database/SapHana.hpp"
#include "database/Sqlite3.hpp"

namespace database {

std::shared_ptr<Connector> DatabaseParser::parse(const Poco::JSON::Object& _obj,
                                                 const std::string& _password) {
  const auto db = jsonutils::JSON::get_value<std::string>(_obj, "db_");

  const auto time_formats = jsonutils::JSON::array_to_vector<std::string>(
      jsonutils::JSON::get_array(_obj, "time_formats_"));

  if (db == BIGQUERY) {
    return std::make_shared<BigQuery>(_obj, time_formats);
  }

  if (db == MYSQL || db == MARIADB) {
    return std::make_shared<MySQL>(_obj, _password, time_formats);
  }

  if (db == ODBC_DIALECT) {
    return std::make_shared<ODBC>(_obj, _password, time_formats);
  }

  if (db == POSTGRES || db == GREENPLUM) {
    return std::make_shared<Postgres>(_obj, _password, time_formats);
  }

  if (db == SAP_HANA) {
    return std::make_shared<SapHana>(_obj, _password, time_formats);
  }

  if (db == SQLITE3) {
    const auto name = jsonutils::JSON::get_value<std::string>(_obj, "name_");

    return std::make_shared<Sqlite3>(name, time_formats);
  }

  throw std::invalid_argument("Database of type '" + db + "' not recognized.");

  return std::shared_ptr<Connector>();
}

// ----------------------------------------------------------------------------
}  // namespace database

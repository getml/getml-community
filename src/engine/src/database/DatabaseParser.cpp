// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#include "database/DatabaseParser.hpp"

#include "database/MySQL.hpp"
#include "database/Postgres.hpp"
#include "database/Sqlite3.hpp"

namespace database {

fct::Ref<Connector> DatabaseParser::parse(const Poco::JSON::Object& _obj,
                                          const std::string& _password) {
  const auto db = jsonutils::JSON::get_value<std::string>(_obj, "db_");

  const auto time_formats = jsonutils::JSON::array_to_vector<std::string>(
      jsonutils::JSON::get_array(_obj, "time_formats_"));

  if (db == MYSQL || db == MARIADB) {
    return fct::Ref<MySQL>::make(_obj, _password, time_formats);
  }

  if (db == POSTGRES) {
    return fct::Ref<Postgres>::make(_obj, _password, time_formats);
  }

  if (db == SQLITE3) {
    const auto name = jsonutils::JSON::get_value<std::string>(_obj, "name_");
    return fct::Ref<Sqlite3>::make(name, time_formats);
  }

  if (db == BIGQUERY || db == GREENPLUM || db == ODBC_DIALECT ||
      db == SAP_HANA) {
    throw std::runtime_error(
        "Database of type '" + db +
        "' is not supported in the getML community edition.");
  }

  throw std::runtime_error("Database of type '" + db + "' not recognized.");
}

// ----------------------------------------------------------------------------
}  // namespace database

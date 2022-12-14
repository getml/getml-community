// Copyright 2022 The SQLNet Company GmbH
// 
// This file is licensed under the Elastic License 2.0 (ELv2). 
// Refer to the LICENSE.txt file in the root of the repository 
// for details.
// 

#ifndef ENGINE_HANDLERS_DATABASEMANAGER_HPP_
#define ENGINE_HANDLERS_DATABASEMANAGER_HPP_

// ------------------------------------------------------------------------

#include <Poco/JSON/Object.h>
#include <Poco/Net/StreamSocket.h>

// ------------------------------------------------------------------------

#include <map>
#include <memory>
#include <string>
#include <vector>

// ------------------------------------------------------------------------

#include "database/database.hpp"
#include "debug/debug.hpp"
#include "fct/Ref.hpp"

// ------------------------------------------------------------------------

#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/utils/utils.hpp"

// ------------------------------------------------------------------------

namespace engine {
namespace handlers {

class DatabaseManager {
 private:
  typedef std::map<std::string, fct::Ref<database::Connector>> ConnectorMap;

 public:
  DatabaseManager(const fct::Ref<const communication::Logger>& _logger,
                  const fct::Ref<const communication::Monitor>& _monitor,
                  const config::Options& _options);

  ~DatabaseManager();

 public:
  /// Copy a table from one database connection to another.
  void copy_table(const Poco::JSON::Object& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Drops the table signified by _name.
  void drop_table(const std::string& _name, const Poco::JSON::Object& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Describes the connection signified by _name.
  void describe_connection(const std::string& _name,
                           Poco::Net::StreamSocket* _socket) const;

  /// Runs a query on the database.
  void execute(const std::string& _name, Poco::Net::StreamSocket* _socket);

  /// Returns the contents of an SQL query in JSON format.
  void get(const std::string& _name, Poco::Net::StreamSocket* _socket);

  /// Lists the column names of the table signified by _name.
  void get_colnames(const std::string& _name, const Poco::JSON::Object& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Sends the content of a table in a format that is compatible with
  /// DataTables.js server-side processing.
  void get_content(const std::string& _name, const Poco::JSON::Object& _cmd,
                   Poco::Net::StreamSocket* _socket);

  /// Returns the number of rows of the table signified by _name.
  void get_nrows(const std::string& _name, const Poco::JSON::Object& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Returns a list of all active connections.
  void list_connections(Poco::Net::StreamSocket* _socket) const;

  /// Lists all tables contained in the database.
  void list_tables(const std::string& _name, Poco::Net::StreamSocket* _socket);

  /// Creates a new database connector.
  void new_db(const Poco::JSON::Object& _cmd, Poco::Net::StreamSocket* _socket);

  /// Sends the name of all tables currently held in the database to the
  /// monitor.
  void post_tables();

  /// Reads CSV files into the database.
  void read_csv(const std::string& _name, const Poco::JSON::Object& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Posts a list of all tables in all databases.
  void refresh(Poco::Net::StreamSocket* _socket);

  /// Sniffs one or several CSV files and returns the CREATE TABLE statement
  /// to the client.
  void sniff_csv(const std::string& _name, const Poco::JSON::Object& _cmd,
                 Poco::Net::StreamSocket* _socket) const;

  /// Sniffs a table and generates suitable keyword arguments
  /// to build a DataFrame.
  void sniff_table(const std::string& _table_name,
                   const Poco::JSON::Object& _cmd,
                   Poco::Net::StreamSocket* _socket) const;

 public:
  /// Trivial accessor
  const fct::Ref<database::Connector> connector(const std::string& _name) {
    multithreading::ReadLock read_lock(read_write_lock_);
    const auto conn = utils::Getter::get(_name, connector_map_);
    return conn;
  }

  /// Trivial accessor
  const fct::Ref<const database::Connector> connector(
      const std::string& _name) const {
    multithreading::ReadLock read_lock(read_write_lock_);
    const auto conn = utils::Getter::get(_name, connector_map_);
    return conn;
  }

 private:
  /// Trivial accessor
  const communication::Logger& logger() { return *logger_; }

 private:
  /// Keeps the connectors to the databases.
  /// The type ConnectorMap is a private typedef.
  ConnectorMap connector_map_;

  /// For logging
  const fct::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const fct::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// Protects the shared_ptr of the connector - the connector might have to
  /// implement its own locking strategy!
  const fct::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATABASEMANAGER_HPP_

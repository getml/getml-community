// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_DATABASEMANAGER_HPP_
#define ENGINE_HANDLERS_DATABASEMANAGER_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <rfl/Ref.hpp>
#include <string>

#include "commands/DatabaseCommand.hpp"
#include "communication/Logger.hpp"
#include "database/Connector.hpp"
#include "engine/utils/Getter.hpp"
#include "multithreading/ReadLock.hpp"

namespace engine {
namespace handlers {

class DatabaseManager {
 private:
  typedef std::map<std::string, rfl::Ref<database::Connector>> ConnectorMap;
  typedef commands::DatabaseCommand Command;

 public:
  DatabaseManager(const rfl::Ref<const communication::Logger>& _logger,
                  const rfl::Ref<const communication::Monitor>& _monitor,
                  const config::Options& _options);

  ~DatabaseManager();

 public:
  /// Executes a command related to a database operation.
  void execute_command(const Command& _command,
                       Poco::Net::StreamSocket* _socket);

  /// Sends the name of all tables currently held in the database to the
  /// monitor.
  void post_tables();

 public:
  /// Trivial accessor
  const rfl::Ref<database::Connector> connector(const std::string& _name) {
    multithreading::ReadLock read_lock(read_write_lock_);
    const auto conn = utils::Getter::get(_name, connector_map_);
    return conn;
  }

  /// Trivial accessor
  const rfl::Ref<const database::Connector> connector(
      const std::string& _name) const {
    multithreading::ReadLock read_lock(read_write_lock_);
    const auto conn = utils::Getter::get(_name, connector_map_);
    return conn;
  }

 private:
  /// Copy a table from one database connection to another.
  void copy_table(const typename Command::CopyTableOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Describes the connection signified by _name.
  void describe_connection(const typename Command::DescribeConnectionOp& _cmd,
                           Poco::Net::StreamSocket* _socket) const;

  /// Drops the table signified by _name.
  void drop_table(const typename Command::DropTableOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Runs a query on the database.
  void execute(const typename Command::ExecuteOp& _cmd,
               Poco::Net::StreamSocket* _socket);

  /// Returns the contents of an SQL query in JSON format.
  void get(const typename Command::GetOp& _cmd,
           Poco::Net::StreamSocket* _socket);

  /// Lists the column names of the table signified by _name.
  void get_colnames(const typename Command::GetColnamesOp& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Sends the content of a table in a format that is compatible with
  /// DataTables.js server-side processing.
  void get_content(const typename Command::GetContentOp& _cmd,
                   Poco::Net::StreamSocket* _socket);

  /// Returns the number of rows of the table signified by _name.
  void get_nrows(const typename Command::GetNRowsOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Returns a list of all active connections.
  void list_connections(const typename Command::ListConnectionsOp& _cmd,
                        Poco::Net::StreamSocket* _socket) const;

  /// Lists all tables contained in the database.
  void list_tables(const typename Command::ListTablesOp& _cmd,
                   Poco::Net::StreamSocket* _socket);

  /// Creates a new database connector.
  void new_db(const typename Command::NewDBOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Reads CSV files into the database.
  void read_csv(const typename Command::ReadCSVOp& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Posts a list of all tables in all databases.
  void refresh(const typename Command::RefreshOp& _cmd,
               Poco::Net::StreamSocket* _socket);

  /// Sniffs one or several CSV files and returns the CREATE TABLE statement
  /// to the client.
  void sniff_csv(const typename Command::SniffCSVOp& _cmd,
                 Poco::Net::StreamSocket* _socket) const;

  /// Sniffs a query and generates suitable keyword arguments
  /// to build a DataFrame.
  void sniff_query(const typename Command::SniffQueryOp& _cmd,
                   Poco::Net::StreamSocket* _socket) const;

  /// Sniffs a table and generates suitable keyword arguments
  /// to build a DataFrame.
  void sniff_table(const typename Command::SniffTableOp& _cmd,
                   Poco::Net::StreamSocket* _socket) const;

 private:
  /// Trivial accessor
  const communication::Logger& logger() { return *logger_; }

 private:
  /// Keeps the connectors to the databases.
  /// The type ConnectorMap is a private typedef.
  ConnectorMap connector_map_;

  /// For logging
  const rfl::Ref<const communication::Logger> logger_;

  /// For communication with the monitor
  const rfl::Ref<const communication::Monitor> monitor_;

  /// Settings for the engine and the monitor
  const config::Options options_;

  /// Protects the shared_ptr of the connector - the connector might have to
  /// implement its own locking strategy!
  const rfl::Ref<multithreading::ReadWriteLock> read_write_lock_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATABASEMANAGER_HPP_

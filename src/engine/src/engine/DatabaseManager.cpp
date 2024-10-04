// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/DatabaseManager.hpp"

#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/Ref.hpp>
#include <rfl/always_false.hpp>
#include <rfl/json/write.hpp>
#include <rfl/visit.hpp>

#include "communication/Sender.hpp"
#include "database/Command.hpp"
#include "database/DatabaseParser.hpp"
#include "database/DatabaseReader.hpp"
#include "database/QuerySplitter.hpp"
#include "database/Sqlite3.hpp"
#include "database/sniff.hpp"
#include "io/Parser.hpp"

namespace engine {
namespace handlers {

DatabaseManager::DatabaseManager(
    const rfl::Ref<const communication::Logger>& _logger,
    const rfl::Ref<const communication::Monitor>& _monitor,
    const config::Options& _options)
    : logger_(_logger),
      monitor_(_monitor),
      options_(_options),
      read_write_lock_(rfl::Ref<multithreading::ReadWriteLock>::make()) {
  const auto obj = database::Command::SQLite3Op{
      .type = rfl::Literal<"Database.new">(),
      .conn_id = "default",
      .db = rfl::Literal<"sqlite3">(),
      .name = options_.project_directory() + "database.db",
      .time_formats = std::vector<std::string>(
          {"%Y-%m-%dT%H:%M:%s%z", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"})};

  connector_map_.emplace(std::make_pair(
      "default", rfl::Ref<database::Sqlite3>::make(database::Sqlite3(obj))));

  post_tables();
}

// ----------------------------------------------------------------------------

DatabaseManager::~DatabaseManager() = default;

// ----------------------------------------------------------------------------

void DatabaseManager::copy_table(const typename Command::CopyTableOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto& source_conn_id = _cmd.source_conn_id();

  const auto& source_table_name = _cmd.source_table();

  const auto& target_conn_id = _cmd.target_conn_id();

  const auto& target_table_name = _cmd.target_table();

  if (source_conn_id == target_conn_id) {
    throw std::runtime_error(
        "Tables must be copied from different database connections!");
  }

  const auto source_conn = connector(source_conn_id);

  const auto target_conn = connector(target_conn_id);

  // Infer the table schema from the source connection and create an
  // appropriate table in the target database.
  const auto stmt =
      database::sniff::table(source_conn, target_conn->dialect(),
                             source_table_name, target_table_name);

  target_conn->execute(stmt);

  const auto colnames = source_conn->get_colnames_from_table(source_table_name);

  const auto iterator = source_conn->select(colnames, source_table_name, "");

  auto reader = database::DatabaseReader(iterator);

  target_conn->read(target_table_name, 0, &reader);

  communication::Sender::send_string("Success!", _socket);

  post_tables();
}

// ----------------------------------------------------------------------------

void DatabaseManager::drop_table(const typename Command::DropTableOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto& conn_id = _cmd.conn_id();

  connector(conn_id)->drop_table(_cmd.name());

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::describe_connection(
    const typename Command::DescribeConnectionOp& _cmd,
    Poco::Net::StreamSocket* _socket) const {
  const auto description = connector(_cmd.name())->describe();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(description, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::execute(const typename Command::ExecuteOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  // TODO: Remove this, splitting should be done by the database connectors
  // themselves.
  const auto splitted = database::QuerySplitter::split_queries(
      communication::Receiver::recv_string(_socket));

  const auto is_not_empty = [](const std::string& _str) -> bool {
    return _str != "";
  };

  auto queries = splitted | std::views::transform(io::Parser::trim) |
                 std::views::filter(is_not_empty);

  for (const auto query : queries) {
    logger().log(query);
    connector(name)->execute(query);
  }

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::execute_command(const Command& _command,
                                      Poco::Net::StreamSocket* _socket) {
  const auto handle_other_command = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, typename Command::CopyTableOp>()) {
      copy_table(_cmd, _socket);
    } else if constexpr (std::is_same<
                             Type, typename Command::DescribeConnectionOp>()) {
      describe_connection(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::DropTableOp>()) {
      drop_table(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::ExecuteOp>()) {
      execute(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::GetOp>()) {
      get(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::GetColnamesOp>()) {
      get_colnames(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::GetContentOp>()) {
      get_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::GetNRowsOp>()) {
      get_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::ListConnectionsOp>()) {
      list_connections(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::ListTablesOp>()) {
      list_tables(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::ReadCSVOp>()) {
      read_csv(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::RefreshOp>()) {
      refresh(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::SniffCSVOp>()) {
      sniff_csv(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::SniffQueryOp>()) {
      sniff_query(_cmd, _socket);
    } else if constexpr (std::is_same<Type, typename Command::SniffTableOp>()) {
      sniff_table(_cmd, _socket);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  const auto handle = [this, _socket, handle_other_command](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;
    if constexpr (std::is_same<Type, Command::NewDBOp>()) {
      new_db(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      typename Command::TaggedUnionType>()) {
      rfl::visit(handle_other_command, _cmd);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  std::visit(handle, _command.val_);
}

// ------------------------------------------------------------------------

void DatabaseManager::get(const typename Command::GetOp& _cmd,
                          Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto query = communication::Receiver::recv_string(_socket);

  auto db_iterator = connector(name)->select(query);

  const auto colnames = db_iterator->colnames();

  std::vector<std::vector<std::string>> columns(colnames.size());

  while (!db_iterator->end()) {
    for (auto& col : columns) {
      col.push_back(db_iterator->get_string());
    }
  }

  std::map<std::string, std::vector<std::string>> result;

  for (size_t i = 0; i < columns.size(); ++i) {
    result[colnames[i]] = std::move(columns[i]);
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(result), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_colnames(const typename Command::GetColnamesOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto& conn_id = _cmd.conn_id();

  const auto query = (name == "") ? _cmd.query() : std::nullopt;

  const auto colnames =
      query ? connector(conn_id)->get_colnames_from_query(*query)
            : connector(conn_id)->get_colnames_from_table(name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(colnames), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_content(const typename Command::GetContentOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto& draw = _cmd.draw();

  const auto& length = _cmd.length();

  const auto& start = _cmd.start();

  const auto& conn_id = _cmd.conn_id();

  const auto& name = _cmd.name();

  const auto table_content =
      connector(conn_id)->get_content(name, draw, start, length);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(table_content), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_nrows(const typename Command::GetNRowsOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& conn_id = _cmd.conn_id();

  const auto& name = _cmd.name();

  const std::int32_t nrows = connector(conn_id)->get_nrows(name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send(sizeof(std::int32_t), &nrows, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::list_connections(
    const typename Command::ListConnectionsOp&,
    Poco::Net::StreamSocket* _socket) const {
  multithreading::ReadLock read_lock(read_write_lock_);

  const auto connections =
      connector_map_ | std::views::keys | fct::ranges::to<std::vector>();

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(rfl::json::write(connections), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::list_tables(const typename Command::ListTablesOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto tables_str = rfl::json::write(connector(name)->list_tables());

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(tables_str, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::new_db(const typename Command::NewDBOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto get_conn_id = [](const auto& _c) { return _c.conn_id(); };

  const auto conn_id = rfl::visit(get_conn_id, _cmd);

  const auto password = communication::Receiver::recv_string(_socket);

  multithreading::WriteLock write_lock(read_write_lock_);

  const auto it = connector_map_.find(conn_id);

  if (it != connector_map_.end()) {
    connector_map_.erase(it);
  }

  connector_map_.emplace(conn_id,
                         database::DatabaseParser::parse(_cmd, password));

  write_lock.unlock();

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::post_tables() {
  multithreading::ReadLock read_lock(read_write_lock_);

  std::map<std::string, std::vector<std::string>> table_map;

  for (const auto& [name, conn] : connector_map_) {
    const auto tables = conn->list_tables();
    table_map[name] = tables;
  }

  monitor_->send_tcp("postdatabasetables", rfl::json::write(table_map),
                     communication::Monitor::TIMEOUT_ON);
}

// ----------------------------------------------------------------------------

void DatabaseManager::read_csv(const typename Command::ReadCSVOp& _cmd,
                               Poco::Net::StreamSocket* _socket) {
  const auto& colnames = _cmd.colnames();

  const auto& conn_id = _cmd.conn_id();

  const auto& fnames = _cmd.fnames();

  const auto& name = _cmd.name();

  const auto num_lines_read = _cmd.num_lines_read();

  const auto quotechar = _cmd.quotechar();

  const auto sep = _cmd.sep();

  const auto skip = _cmd.skip();

  if (quotechar.size() != 1) {
    throw std::runtime_error(
        "The quotechar must consist of exactly one character!");
  }

  if (sep.size() != 1) {
    throw std::runtime_error(
        "The separator (sep) must consist of exactly one character!");
  }

  auto limit = num_lines_read > 0 ? num_lines_read + skip : num_lines_read;

  if (!colnames && limit > 0) {
    ++limit;
  }

  for (const auto& fname : fnames) {
    auto reader = io::CSVReader(colnames, fname, limit, quotechar[0], sep[0]);

    connector(conn_id)->read(name, skip, &reader);

    logger().log("Read '" + fname + "'.");
  }

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::refresh(const typename Command::RefreshOp&,
                              Poco::Net::StreamSocket* _socket) {
  post_tables();
  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_csv(const typename Command::SniffCSVOp& _cmd,
                                Poco::Net::StreamSocket* _socket) const {
  const auto& colnames = _cmd.colnames();

  const auto& conn_id = _cmd.conn_id();

  const auto& fnames = _cmd.fnames();

  const auto& name = _cmd.name();

  const auto quotechar = _cmd.quotechar();

  const auto sep = _cmd.sep();

  const auto skip = _cmd.skip();

  const auto num_lines_sniffed = _cmd.num_lines_sniffed();

  const auto dialect =
      _cmd.dialect() ? _cmd.dialect()->name() : connector(conn_id)->dialect();

  if (quotechar.size() != 1) {
    throw std::runtime_error(
        "The quotechar must consist of exactly one character!");
  }

  if (sep.size() != 1) {
    throw std::runtime_error(
        "The separator (sep) must consist of exactly one character!");
  }

  auto sniffer = io::CSVSniffer(colnames, dialect, fnames, num_lines_sniffed,
                                quotechar[0], sep[0], skip, name);

  const auto create_table_statement = sniffer.sniff();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(create_table_statement, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_query(const typename Command::SniffQueryOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) const {
  const auto& conn_id = _cmd.conn_id();

  const auto& name = _cmd.name();

  const auto query = communication::Receiver::recv_string(_socket);

  const auto roles =
      database::sniff::query(connector(conn_id), "python", query, name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(roles, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_table(const typename Command::SniffTableOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) const {
  const auto& conn_id = _cmd.conn_id();

  const auto& name = _cmd.name();

  const auto roles =
      database::sniff::table(connector(conn_id), "python", name, name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(roles, _socket);
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

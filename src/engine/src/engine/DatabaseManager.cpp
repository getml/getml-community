// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/DatabaseManager.hpp"

#include "database/Command.hpp"
#include "database/QuerySplitter.hpp"
#include "fct/Field.hpp"
#include "fct/Ref.hpp"
#include "helpers/StringSplitter.hpp"
#include "io/Parser.hpp"

namespace engine {
namespace handlers {

DatabaseManager::DatabaseManager(
    const fct::Ref<const communication::Logger>& _logger,
    const fct::Ref<const communication::Monitor>& _monitor,
    const config::Options& _options)
    : logger_(_logger),
      monitor_(_monitor),
      options_(_options),
      read_write_lock_(fct::Ref<multithreading::ReadWriteLock>::make()) {
  const auto obj =
      fct::Field<"db_", fct::Literal<"sqlite3">>(fct::Literal<"sqlite3">()) *
      fct::Field<"conn_id_", std::string>("default") *
      fct::Field<"name_", std::string>(options_.project_directory() +
                                       "database.db") *
      fct::Field<"time_formats_", std::vector<std::string>>(
          {"%Y-%m-%dT%H:%M:%s%z", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d %H:%M:%S"});

  connector_map_.emplace(std::make_pair(
      "default", fct::Ref<database::Sqlite3>::make(database::Sqlite3(obj))));

  post_tables();
}

// ----------------------------------------------------------------------------

DatabaseManager::~DatabaseManager() = default;

// ----------------------------------------------------------------------------

void DatabaseManager::copy_table(const Poco::JSON::Object& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto source_conn_id =
      JSON::get_value<std::string>(_cmd, "source_conn_id_");

  const auto source_table_name =
      JSON::get_value<std::string>(_cmd, "source_table_");

  const auto target_conn_id =
      JSON::get_value<std::string>(_cmd, "target_conn_id_");

  const auto target_table_name =
      JSON::get_value<std::string>(_cmd, "target_table_");

  if (source_conn_id == target_conn_id) {
    throw std::runtime_error(
        "Tables must be copied from different database connections!");
  }

  const auto source_conn = connector(source_conn_id);

  const auto target_conn = connector(target_conn_id);

  // Infer the table schema from the source connection and create an
  // appropriate table in the target database.
  const auto stmt =
      database::DatabaseSniffer::sniff(source_conn, target_conn->dialect(),
                                       source_table_name, target_table_name);

  target_conn->execute(stmt);

  const auto colnames = source_conn->get_colnames(source_table_name);

  const auto iterator = source_conn->select(colnames, source_table_name, "");

  auto reader = database::DatabaseReader(iterator);

  target_conn->read(target_table_name, 0, &reader);

  communication::Sender::send_string("Success!", _socket);

  post_tables();
}

// ----------------------------------------------------------------------------

void DatabaseManager::drop_table(const std::string& _name,
                                 const Poco::JSON::Object& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  connector(conn_id)->drop_table(_name);

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::describe_connection(
    const std::string& _name, Poco::Net::StreamSocket* _socket) const {
  const auto description = connector(_name)->describe();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(description, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::execute(const std::string& _name,
                              Poco::Net::StreamSocket* _socket) {
  // TODO: Remove this, splitting should be done by the database connectors
  // themselves.
  const auto splitted = database::QuerySplitter::split_queries(
      communication::Receiver::recv_string(_socket));

  const auto is_not_empty = [](const std::string& _str) -> bool {
    return _str != "";
  };

  auto queries = splitted | VIEWS::transform(io::Parser::trim) |
                 VIEWS::filter(is_not_empty);

  for (const auto query : queries) {
    logger().log(query);
    connector(_name)->execute(query);
  }

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get(const std::string& _name,
                          Poco::Net::StreamSocket* _socket) {
  const auto query = communication::Receiver::recv_string(_socket);

  auto db_iterator = connector(_name)->select(query);

  const auto colnames = db_iterator->colnames();

  std::vector<Poco::JSON::Array> columns(colnames.size());

  while (!db_iterator->end()) {
    for (auto& col : columns) {
      col.add(db_iterator->get_string());
    }
  }

  Poco::JSON::Object obj;

  for (size_t i = 0; i < columns.size(); ++i) {
    obj.set(colnames[i], columns[i]);
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_colnames(const std::string& _name,
                                   const Poco::JSON::Object& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  const auto query = (_name == "")
                         ? JSON::get_value<std::string>(_cmd, "query_")
                         : std::string("");

  const auto colnames = (query == "")
                            ? connector(conn_id)->get_colnames(_name)
                            : connector(conn_id)->select(query)->colnames();

  std::string array = "[";

  for (auto& col : colnames) {
    array += std::string("\"") + col + "\",";
  }

  if (array.size() > 1) {
    array.back() = ']';
  } else {
    array += ']';
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(array, _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_content(const std::string& _name,
                                  const Poco::JSON::Object& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto draw = JSON::get_value<Int>(_cmd, "draw_");

  const auto length = JSON::get_value<Int>(_cmd, "length_");

  const auto start = JSON::get_value<Int>(_cmd, "start_");

  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  auto obj = connector(conn_id)->get_content(_name, draw, start, length);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(JSON::stringify(obj), _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::get_nrows(const std::string& _name,
                                const Poco::JSON::Object& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  const std::int32_t nrows = connector(conn_id)->get_nrows(_name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send(sizeof(std::int32_t), &nrows, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::list_connections(Poco::Net::StreamSocket* _socket) const {
  std::string str = "[";

  multithreading::ReadLock read_lock(read_write_lock_);

  for (auto& [key, value] : connector_map_) {
    str += "\"" + key + "\",";
  }

  if (connector_map_.size() == 0) {
    str += "]";
  } else {
    str.back() = ']';
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(str, _socket);
}

// ------------------------------------------------------------------------

void DatabaseManager::list_tables(const std::string& _name,
                                  Poco::Net::StreamSocket* _socket) {
  std::string str = "[";

  const auto tables = connector(_name)->list_tables();

  for (const auto& table : tables) {
    str += "\"" + table + "\",";
  }

  if (tables.size() == 0) {
    str += "]";
  } else {
    str.back() = ']';
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(str, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::new_db(const Poco::JSON::Object& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto cmd = json::Parser<database::Command>::from_json(_cmd);

  const auto conn_id = fct::get<"conn_id_">(cmd.val_);

  const auto password = communication::Receiver::recv_string(_socket);

  multithreading::WriteLock write_lock(read_write_lock_);

  const auto it = connector_map_.find(conn_id);

  if (it != connector_map_.end()) {
    connector_map_.erase(it);
  }

  connector_map_.emplace(conn_id,
                         database::DatabaseParser::parse(cmd, password));

  write_lock.unlock();

  post_tables();

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::post_tables() {
  Poco::JSON::Object obj;

  multithreading::ReadLock read_lock(read_write_lock_);

  for (const auto& [name, conn] : connector_map_) {
    const auto tables = conn->list_tables();
    obj.set(name, jsonutils::JSON::vector_to_array_ptr(tables));
  }
}

// ----------------------------------------------------------------------------

void DatabaseManager::read_csv(const std::string& _name,
                               const Poco::JSON::Object& _cmd,
                               Poco::Net::StreamSocket* _socket) {
  auto colnames = std::optional<std::vector<std::string>>();

  if (_cmd.has("colnames_")) {
    colnames =
        JSON::array_to_vector<std::string>(JSON::get_array(_cmd, "colnames_"));
  }

  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  const auto fnames =
      JSON::array_to_vector<std::string>(JSON::get_array(_cmd, "fnames_"));

  const auto num_lines_read = JSON::get_value<size_t>(_cmd, "num_lines_read_");

  const auto quotechar = JSON::get_value<std::string>(_cmd, "quotechar_");

  const auto sep = JSON::get_value<std::string>(_cmd, "sep_");

  const auto skip = JSON::get_value<size_t>(_cmd, "skip_");

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

    connector(conn_id)->read(_name, skip, &reader);

    logger().log("Read '" + fname + "'.");
  }

  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::refresh(Poco::Net::StreamSocket* _socket) {
  post_tables();
  communication::Sender::send_string("Success!", _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_csv(const std::string& _name,
                                const Poco::JSON::Object& _cmd,
                                Poco::Net::StreamSocket* _socket) const {
  auto colnames = std::optional<std::vector<std::string>>();

  if (_cmd.has("colnames_")) {
    colnames =
        JSON::array_to_vector<std::string>(JSON::get_array(_cmd, "colnames_"));
  }

  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  const auto fnames =
      JSON::array_to_vector<std::string>(JSON::get_array(_cmd, "fnames_"));

  const auto num_lines_sniffed =
      JSON::get_value<size_t>(_cmd, "num_lines_sniffed_");

  const auto quotechar = JSON::get_value<std::string>(_cmd, "quotechar_");

  const auto sep = JSON::get_value<std::string>(_cmd, "sep_");

  const auto skip = JSON::get_value<size_t>(_cmd, "skip_");

  const auto dialect = _cmd.has("dialect_")
                           ? JSON::get_value<std::string>(_cmd, "dialect_")
                           : connector(conn_id)->dialect();

  if (_cmd.has("dialect_") && dialect != "python") {
    throw std::runtime_error(
        "If you explicitly pass a dialect, it must be 'python'!");
  }

  if (quotechar.size() != 1) {
    throw std::runtime_error(
        "The quotechar must consist of exactly one character!");
  }

  if (sep.size() != 1) {
    throw std::runtime_error(
        "The separator (sep) must consist of exactly one character!");
  }

  auto sniffer = io::CSVSniffer(colnames, dialect, fnames, num_lines_sniffed,
                                quotechar[0], sep[0], skip, _name);

  const auto create_table_statement = sniffer.sniff();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(create_table_statement, _socket);
}

// ----------------------------------------------------------------------------

void DatabaseManager::sniff_table(const std::string& _name,
                                  const Poco::JSON::Object& _cmd,
                                  Poco::Net::StreamSocket* _socket) const {
  const auto conn_id = JSON::get_value<std::string>(_cmd, "conn_id_");

  const auto roles = database::DatabaseSniffer::sniff(connector(conn_id),
                                                      "python", _name, _name);

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(roles, _socket);
}

// ----------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

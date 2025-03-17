// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef DATABASE_MYSQL_HPP_
#define DATABASE_MYSQL_HPP_

#include "database/Command.hpp"
#include "database/Connector.hpp"
#include "database/MySQLIterator.hpp"
#include "database/TableContent.hpp"

#include <mysql.h>

#include <memory>
#include <string>
#include <vector>

namespace database {

class MySQL : public Connector {
 public:
  MySQL(const typename Command::MySQLOp& _obj, const std::string& _passwd)
      : dbname_(_obj.dbname()),
        host_(_obj.host()),
        passwd_(_passwd),
        port_(_obj.port()),
        time_formats_(_obj.time_formats()),
        unix_socket_(_obj.unix_socket()),
        user_(_obj.user()) {}

  MySQL(const std::string& _dbname, const std::string& _host,
        const std::string& _passwd, const unsigned int _port,
        const std::string& _unix_socket, const std::string& _user,
        const std::vector<std::string>& _time_formats)
      : dbname_(_dbname),
        host_(_host),
        passwd_(_passwd),
        port_(_port),
        time_formats_(_time_formats),
        unix_socket_(_unix_socket),
        user_(_user) {}

  ~MySQL() = default;

 public:
  /// Returns a std::string describing the connection.
  std::string describe() const final;

  /// Returns the names of columns produced by the query.
  std::vector<std::string> get_colnames_from_query(
      const std::string& _query) const final;

  /// Returns the types of columns produced by the query.
  std::vector<io::Datatype> get_coltypes_from_query(
      const std::string& _query,
      const std::vector<std::string>& _colnames) const final;

  /// Returns the content of a table in a format that is compatible
  /// with the DataTables.js server-side processing API.
  TableContent get_content(const std::string& _tname, const std::int32_t _draw,
                           const std::int32_t _start,
                           const std::int32_t _length) final;

  /// Lists the name of the tables held in the database.
  std::vector<std::string> list_tables() final;

  /// Reads a CSV file or another data source into a table.
  void read(const std::string& _table, const size_t _skip,
            io::Reader* _reader) final;

 public:
  /// Returns the dialect of the connector.
  std::string dialect() const final { return "mysql"; }

  /// Drops a table and cleans up, if necessary.
  void drop_table(const std::string& _tname) final {
    execute("DROP TABLE `" + _tname + "`;");
  }

  /// Executes an SQL query.
  void execute(const std::string& _sql) final {
    const auto conn = make_connection();
    exec(_sql, conn);
  }

  /// Returns the names of columns of the table.
  std::vector<std::string> get_colnames_from_table(
      const std::string& _table) const final {
    return get_colnames_from_query("SELECT * FROM `" + _table + "` LIMIT 0;");
  }

  /// Returns the types of columns of the table.
  std::vector<io::Datatype> get_coltypes_from_table(
      const std::string& _table,
      const std::vector<std::string>& _colnames) const final {
    return get_coltypes_from_query("SELECT * FROM `" + _table + "` LIMIT 0;",
                                   _colnames);
  }

  /// Returns the number of rows in the table signified by _tname.
  std::int32_t get_nrows(const std::string& _tname) final {
    return select({"COUNT(*)"}, _tname, "")->get_int();
  }

  /// Returns a shared_ptr containing a MySQLIterator.
  rfl::Ref<Iterator> select(const std::vector<std::string>& _colnames,
                            const std::string& _tname,
                            const std::string& _where) final {
    return rfl::Ref<MySQLIterator>::make(make_connection(), _colnames,
                                         time_formats_, _tname, _where);
  }

  /// Returns a shared_ptr containing a MySQLIterator.
  rfl::Ref<Iterator> select(const std::string& _sql) final {
    return rfl::Ref<MySQLIterator>::make(make_connection(), _sql,
                                         time_formats_);
  }

  /// Returns the time formats used.
  const std::vector<std::string>& time_formats() const { return time_formats_; }

 private:
  /// Makes sure that the colnames of the CSV file match the colnames of the
  /// target table.
  void check_colnames(const std::vector<std::string>& _colnames,
                      io::Reader* _reader);

  /// Executes and SQL command given a connection.
  std::shared_ptr<MYSQL_RES> exec(const std::string& _sql,
                                  const std::shared_ptr<MYSQL>& _conn) const;

  /// Parses a field for the CSV reader.
  io::Datatype interpret_field_type(const enum_field_types _type) const;

  /// Prepares a INSERT INTO .. VALUES ... query
  /// to insert a large CSV file.
  std::string make_bulk_insert_query(
      const std::string& _table,
      const std::vector<std::string>& _colnames) const;

  /// Prepares a query to get the content of a table.
  std::string make_get_content_query(const std::string& _table,
                                     const std::vector<std::string>& _colnames,
                                     const std::int32_t _begin,
                                     const std::int32_t _end) const;

 private:
  /// Returns a new connection based on the connection_string_
  std::shared_ptr<MYSQL> make_connection() const {
    auto raw_ptr = mysql_init(NULL);

    const auto conn = std::shared_ptr<MYSQL>(raw_ptr, mysql_close);

    const auto res = mysql_real_connect(
        conn.get(), host_.c_str(), user_.c_str(), passwd_.c_str(),
        dbname_.c_str(), port_, unix_socket_.c_str(), CLIENT_MULTI_STATEMENTS);

    if (!res) {
      throw_error(conn);
    }

    return conn;
  }

  /// Throws an error.
  void throw_error(const std::shared_ptr<MYSQL>& _conn) const {
    const std::string msg =
        "MySQL error (" + std::to_string(mysql_errno(_conn.get())) + ") [" +
        mysql_sqlstate(_conn.get()) + "] " + mysql_error(_conn.get());

    throw std::runtime_error(msg);
  }

 private:
  /// The database to be accessed.
  const std::string dbname_;

  /// The host address.
  const std::string host_;

  /// The password used.
  const std::string passwd_;

  /// The port to be accessed.
  const unsigned int port_;

  /// Vector containing the time formats.
  const std::vector<std::string> time_formats_;

  /// The location of the unix socket.
  const std::string unix_socket_;

  /// The user name.
  const std::string user_;
};

}  // namespace database

#endif  // DATABASE_MYSQL_HPP_

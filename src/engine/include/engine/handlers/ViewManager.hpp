// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_VIEWMANAGER_HPP_
#define ENGINE_HANDLERS_VIEWMANAGER_HPP_

#include "commands/ViewCommand.hpp"
#include "engine/handlers/DataFrameManagerParams.hpp"

#include <Poco/Net/StreamSocket.h>
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>

#include <map>
#include <string>

namespace engine {
namespace handlers {

class ViewManager {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  using Command = commands::ViewCommand;

 public:
  explicit ViewManager(const DataFrameManagerParams& _params)
      : params_(_params) {}

  ~ViewManager() = default;

 public:
  /// Executes a DataFrameCommand.
  void execute_command(const Command& _command,
                       Poco::Net::StreamSocket* _socket);

 public:
  /// Sends a JSON representing the view to the client.
  void get_view_content(const typename Command::GetViewContentOp& _cmd,
                        Poco::Net::StreamSocket* _socket);

  /// Sends a JSON representing the number of rows in the view, if known.
  void get_view_nrows(const typename Command::GetViewNRowsOp& _cmd,
                      Poco::Net::StreamSocket* _socket);

  /// Writes a view to an arrow.Table.
  void view_to_arrow(const typename Command::ViewToArrowOp& _cmd,
                     Poco::Net::StreamSocket* _socket);

  /// Writes a view to CSV.
  void view_to_csv(const typename Command::ViewToCSVOp& _cmd,
                   Poco::Net::StreamSocket* _socket);

  /// Writes a view into a database.
  void view_to_db(const typename Command::ViewToDBOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Writes a view to a parquet file.
  void view_to_parquet(const typename Command::ViewToParquetOp& _cmd,
                       Poco::Net::StreamSocket* _socket);

 private:
  /// Trivial accessor
  containers::Encoding& categories() { return *params_.categories_; }

  /// Trivial accessor
  rfl::Ref<database::Connector> connector(const std::string& _name) {
    return params_.database_manager_->connector(_name);
  }

  /// Trivial accessor
  std::map<std::string, containers::DataFrame>& data_frames() {
    return *params_.data_frames_;
  }

  /// Trivial accessor
  const std::map<std::string, containers::DataFrame>& data_frames() const {
    return *params_.data_frames_;
  }

  /// Checks whether a data frame of a certain name exists
  bool df_exists(const std::string& _name) {
    const auto it = data_frames().find(_name);
    return (it == data_frames().end());
  }

  /// Trivial accessor
  containers::Encoding& join_keys_encoding() {
    return *params_.join_keys_encoding_;
  }

  /// Trivial accessor
  const communication::Logger& logger() { return *params_.logger_; }

 private:
  /// The underlying parameters.
  const DataFrameManagerParams params_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_VIEWMANAGER_HPP_

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_COLUMNMANAGER_HPP_
#define ENGINE_HANDLERS_COLUMNMANAGER_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "commands/ColumnCommand.hpp"
#include "debug/debug.hpp"
#include "engine/communication/communication.hpp"
#include "engine/config/config.hpp"
#include "engine/containers/containers.hpp"
#include "engine/handlers/DataFrameManagerParams.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"

namespace engine {
namespace handlers {

class ColumnManager {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  using Command = commands::ColumnCommand;

  using CloseDataFrameOp =
      fct::NamedTuple<fct::Field<"name_", std::string>,
                      fct::Field<"type_", fct::Literal<"DataFrame.close">>>;

  using RecvAndAddOp = fct::NamedTuple<fct::Field<"name_", std::string>,
                                       fct::Field<"role_", std::string>>;

 public:
  explicit ColumnManager(const DataFrameManagerParams& _params)
      : params_(_params) {}

  ~ColumnManager() = default;

 public:
  /// Executes a DataFrameCommand.
  void execute_command(const Command& _command,
                       Poco::Net::StreamSocket* _socket);

  /// Sends a column to the client
  void get_column(const typename Command::GetFloatColumnOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Sends a categorical columm to the client
  void get_categorical_column(const typename Command::GetStringColumnOp& _cmd,
                              Poco::Net::StreamSocket* _socket);

  /// Changes the unit of the column.
  void set_unit(const typename Command::SetFloatColumnUnitOp& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Changes the unit of the column.
  void set_unit_categorical(const typename Command::SetStringColumnUnitOp& _cmd,
                            Poco::Net::StreamSocket* _socket);

 private:
  /// Adds a new float column to an existing data frame (sent by the user, for
  /// instance as a numpy array).
  void add_float_column(const typename Command::FloatColumnOp& _cmd,
                        Poco::Net::StreamSocket* _socket);

  /// Adds a new string column to an existing data frame (sent by the user,
  /// for instance as a numpy array).
  void add_string_column(const typename Command::StringColumnOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Undertakes an aggregation on an entire column.
  void aggregate(const typename Command::AggregationOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Sends a boolean columm to the client
  void get_boolean_column(const typename Command::GetBooleanColumnOp& _cmd,
                          Poco::Net::StreamSocket* _socket);

  /// Sends a JSON representing the column to the client.
  void get_boolean_column_content(
      const typename Command::GetBooleanColumnContentOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Returns the number of rows in boolean column.
  void get_boolean_column_nrows(
      const typename Command::GetBooleanColumnNRowsOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Sends a JSON representing the column to the client.
  void get_categorical_column_content(
      const typename Command::GetStringColumnContentOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Sends a string describing the number of rows in a categorical column.
  void get_categorical_column_nrows(
      const typename Command::GetStringColumnNRowsOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Returns the unique values from a categorical column.
  void get_categorical_column_unique(
      const typename Command::GetStringColumnUniqueOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Gets the number of rows in a float column.
  void get_column_nrows(const typename Command::GetFloatColumnNRowsOp& _cmd,
                        Poco::Net::StreamSocket* _socket);

  /// Returns the unique values from a float column.
  void get_column_unique(const typename Command::GetFloatColumnUniqueOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Sends a JSON representing the column to the client.
  void get_float_column_content(
      const typename Command::GetFloatColumnContentOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Get the subroles for a float column.
  void get_subroles(const typename Command::GetFloatColumnSubrolesOp& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Get the subroles for a string column.
  void get_subroles_categorical(
      const typename Command::GetStringColumnSubrolesOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Get the unit for a float column.
  void get_unit(const typename Command::GetFloatColumnUnitOp& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Get the unit for a string column.
  void get_unit_categorical(const typename Command::GetStringColumnUnitOp& _cmd,
                            Poco::Net::StreamSocket* _socket);

  /// Changes the subroles of the column.
  void set_subroles(const typename Command::SetFloatColumnSubrolesOp& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Changes the subroles of the column.
  void set_subroles_categorical(
      const typename Command::SetStringColumnSubrolesOp& _cmd,
      Poco::Net::StreamSocket* _socket);

 private:
  /// Trivial accessor
  containers::Encoding& categories() { return *params_.categories_; }

  /// Trivial accessor
  fct::Ref<database::Connector> connector(const std::string& _name) const {
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
  const communication::Logger& logger() const { return *params_.logger_; }

 private:
  /// The underlying parameters.
  const DataFrameManagerParams params_;
};

}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_COLUMNMANAGER_HPP_

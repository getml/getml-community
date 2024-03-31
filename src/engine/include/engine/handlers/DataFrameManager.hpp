// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_
#define ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_

#include <Poco/Net/StreamSocket.h>

#include <map>
#include <memory>
#include <string>

#include "commands/DataFrameCommand.hpp"
#include "communication/communication.hpp"
#include "containers/containers.hpp"
#include "debug/debug.hpp"
#include "engine/config/config.hpp"
#include "engine/handlers/DataFrameManagerParams.hpp"
#include "engine/handlers/DatabaseManager.hpp"
#include "rfl/Field.hpp"
#include "rfl/NamedTuple.hpp"
#include "rfl/Ref.hpp"

namespace engine {
namespace handlers {

class DataFrameManager {
 public:
  static constexpr const char* FLOAT_COLUMN =
      containers::Column<bool>::FLOAT_COLUMN;
  static constexpr const char* STRING_COLUMN =
      containers::Column<bool>::STRING_COLUMN;

  using Command = commands::DataFrameCommand;

  struct CloseDataFrameOp {
    using Tag = rfl::Literal<"DataFrame.close">;
    rfl::Field<"name_", std::string> name;
  };

  struct RecvAndAddOp {
    rfl::Field<"name_", std::string> name;
    rfl::Field<"role_", std::string> role;
  };

 public:
  explicit DataFrameManager(const DataFrameManagerParams& _params)
      : params_(_params) {}

  ~DataFrameManager() = default;

 public:
  /// Executes a DataFrameCommand.
  void execute_command(const Command& _command,
                       Poco::Net::StreamSocket* _socket);

  /// Receives a FloatColumn and adds it to the DataFrame.
  void recv_and_add_float_column(
      const RecvAndAddOp& _cmd, containers::DataFrame* _df,
      multithreading::WeakWriteLock* _weak_write_lock,
      Poco::Net::StreamSocket* _socket) const;

  /// Adds a string column to a data frame.
  void recv_and_add_string_column(
      const RecvAndAddOp& _cmd, containers::DataFrame* _df,
      multithreading::WeakWriteLock* _weak_write_lock,
      Poco::Net::StreamSocket* _socket);

  /// Adds a string column to a data frame.
  void recv_and_add_string_column(
      const RecvAndAddOp& _cmd,
      const rfl::Ref<containers::Encoding>& _local_categories,
      const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
      containers::DataFrame* _df, Poco::Net::StreamSocket* _socket) const;

 public:
  /// Creates a new data frame and adds it to the map of data frames.
  void add_data_frame(const std::string& _name,
                      Poco::Net::StreamSocket* _socket);

  /// Adds a new float column to an existing data frame (parsed by the column
  /// operators).
  void add_float_column(const typename Command::AddFloatColumnOp& _cmd,
                        Poco::Net::StreamSocket* _socket);

  /// Adds a new float column to an existing data frame (sent by the user, for
  /// instance as a numpy array).
  void add_float_column(const typename Command::FloatColumnOp& _cmd,
                        Poco::Net::StreamSocket* _socket);

  /// Adds a string column to an existing data frame (parsed by
  /// the column operators).
  void add_string_column(const typename Command::AddStringColumnOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Adds a new string column to an existing data frame (sent by the user,
  /// for instance as a numpy array).
  void add_string_column(const typename Command::StringColumnOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Appends data to an existing data frame.
  void append_to_data_frame(const typename Command::AppendToDataFrameOp& _cmd,
                            Poco::Net::StreamSocket* _socket);

  /// Calculates the plots for a categorical column.
  void calc_categorical_column_plots(
      const typename Command::CalcCategoricalColumnPlotOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Calculates the plots for a column.
  void calc_column_plots(const typename Command::CalcColumnPlotOp& _cmd,
                         Poco::Net::StreamSocket* _socket);

  /// Concatenates a list of data frames.
  void concat(const typename Command::ConcatDataFramesOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Writes the data frame to CSV.
  void df_to_csv(
      const std::string& _fname, const size_t _batch_size,
      const std::string& _quotechar, const std::string& _sep,
      const containers::DataFrame& _df,
      const rfl::Ref<containers::Encoding>& _categories,
      const rfl::Ref<containers::Encoding>& _join_keys_encoding) const;

  /// Writes a data frame to a database.
  void df_to_db(
      const std::string& _conn_id, const std::string& _table_name,
      const containers::DataFrame& _df,
      const rfl::Ref<containers::Encoding>& _categories,
      const rfl::Ref<containers::Encoding>& _join_keys_encoding) const;

  /// Freezes the data frame (making it immutable).
  void freeze(const typename Command::FreezeDataFrameOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from an Arrow DataFrame.
  void from_arrow(const typename Command::AddDfFromArrowOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from a set of CSV files.
  void from_csv(const typename Command::AddDfFromCSVOp& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from a table in the database.
  void from_db(const typename Command::AddDfFromDBOp& _cmd,
               Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from a JSON string.
  void from_json(const typename Command::AddDfFromJSONOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Creates a new DataFrame from a parquet file.
  void from_parquet(const typename Command::AddDfFromParquetOp& _cmd,
                    Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from a database query.
  void from_query(const typename Command::AddDfFromQueryOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Creates a new data frame from a view.
  void from_view(const typename Command::AddDfFromViewOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Sends a data frame back to the client, column-by-column.
  void get_data_frame(const typename Command::GetDataFrameOp& _cmd,
                      Poco::Net::StreamSocket* _socket);

  /// Expresses the data frame in HTML format, for a Jupyter notebook.
  void get_data_frame_html(const typename Command::GetDataFrameHTMLOp& _cmd,
                           Poco::Net::StreamSocket* _socket);

  /// Expresses the data frame as a string.
  void get_data_frame_string(const typename Command::GetDataFrameStringOp& _cmd,
                             Poco::Net::StreamSocket* _socket);

  /// Sends the content of a data frame in a format that is compatible with
  /// DataTables.js server-side processing.
  void get_data_frame_content(
      const typename Command::GetDataFrameContentOp& _cmd,
      Poco::Net::StreamSocket* _socket);

  /// Get the size of a data frame
  void get_nbytes(const typename Command::GetDataFrameNBytesOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

  /// Get the number of rows in a data frame
  void get_nrows(const typename Command::GetDataFrameNRowsOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Returns a string describing the last time a data frame has been changed.
  void last_change(const typename Command::LastChangeOp& _cmd,
                   Poco::Net::StreamSocket* _socket);

  template <typename T, typename IterType>
  std::string make_column_string(const Int _draw, const size_t _nrows,
                                 IterType _begin, IterType _end) const;

  /// Refreshes a data frame.
  void refresh(const typename Command::RefreshDataFrameOp& _cmd,
               Poco::Net::StreamSocket* _socket);

  /// Removes a column from a DataFrame.
  void remove_column(const typename Command::RemoveColumnOp& _cmd,
                     Poco::Net::StreamSocket* _socket);

  /// Sends summary statistics back to the client.
  void summarize(const typename Command::SummarizeDataFrameOp& _cmd,
                 Poco::Net::StreamSocket* _socket);

  /// Writes the dataframe to Arrow.
  void to_arrow(const typename Command::ToArrowOp& _cmd,
                Poco::Net::StreamSocket* _socket);

  /// Writes the dataframe to CSV.
  void to_csv(const typename Command::ToCSVOp& _cmd,
              Poco::Net::StreamSocket* _socket);

  /// Writes the dataframe into the data base.
  void to_db(const typename Command::ToDBOp& _cmd,
             Poco::Net::StreamSocket* _socket);

  /// Writes the dataframe into a parquet file.
  void to_parquet(const typename Command::ToParquetOp& _cmd,
                  Poco::Net::StreamSocket* _socket);

 private:
  /// Takes care of the process of actually adding the column.
  void add_float_column_to_df(
      const std::string& _role, const containers::Column<Float>& _col,
      containers::DataFrame* _df,
      multithreading::WeakWriteLock* _weak_write_lock) const;

  /// Adds an integer column to the data frame.
  void add_int_column_to_df(const std::string& _name, const std::string& _role,
                            const std::string& _unit,
                            const containers::Column<strings::String>& _col,
                            containers::DataFrame* _df,
                            multithreading::WeakWriteLock* _weak_write_lock,
                            Poco::Net::StreamSocket* _socket);

  /// Adds an integer column to the data frame.
  void add_int_column_to_df(
      const std::string& _name, const std::string& _role,
      const std::string& _unit, const containers::Column<strings::String>& _col,
      const rfl::Ref<containers::Encoding>& _local_categories,
      const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
      containers::DataFrame* _df) const;

  /// Adds a string column to the data frame.
  void add_string_column_to_df(
      const std::string& _name, const std::string& _role,
      const std::string& _unit, const containers::Column<strings::String>& _col,
      containers::DataFrame* _df,
      multithreading::WeakWriteLock* _weak_write_lock) const;

  /// Receives the actual data contained in a DataFrame
  void receive_data(
      const rfl::Ref<containers::Encoding>& _local_categories,
      const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
      containers::DataFrame* _df, Poco::Net::StreamSocket* _socket) const;

 private:
  /// Trivial accessor
  containers::Encoding& categories() { return *params_.categories_; }

  /// Trivial accessor
  rfl::Ref<database::Connector> connector(const std::string& _name) const {
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

  // ------------------------------------------------------------------------

 private:
  /// The underlying parameters.
  const DataFrameManagerParams params_;
};

// ------------------------------------------------------------------------
// ------------------------------------------------------------------------

template <typename T, typename IterType>
std::string DataFrameManager::make_column_string(const Int _draw,
                                                 const size_t _nrows,
                                                 IterType _begin,
                                                 IterType _end) const {
  const auto base = rfl::make_field<"draw">(_draw) *
                    rfl::make_field<"recordsTotal">(_nrows) *
                    rfl::make_field<"recordsFiltered">(_nrows);

  auto data = std::vector<std::vector<std::string>>();

  if (_nrows == 0) {
    return rfl::json::write(base * rfl::make_field<"data">(data));
  }

  for (auto it = _begin; it != _end; ++it) {
    auto row = std::vector<std::string>();

    if constexpr (std::is_same<T, strings::String>()) {
      row.push_back(it->str());
    } else if constexpr (std::is_same<T, std::string>()) {
      row.push_back(*it);
    } else {
      row.push_back(io::Parser::to_string(*it));
    }

    data.push_back(row);
  }

  return rfl::json::write(base * rfl::make_field<"data">(data));
}

// ------------------------------------------------------------------------
}  // namespace handlers
}  // namespace engine

#endif  // ENGINE_HANDLERS_DATAFRAMEMANAGER_HPP_

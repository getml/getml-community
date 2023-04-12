// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/DataFrameManager.hpp"
#include "fct/always_false.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::execute_command(const Command& _command,
                                       Poco::Net::StreamSocket* _socket) {
  const auto handle = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, Command::AddFloatColumnOp>()) {
      add_float_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::AddStringColumnOp>()) {
      add_string_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::AppendToDataFrameOp>()) {
      append_to_data_frame(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::CalcCategoricalColumnPlotOp>()) {
      calc_categorical_column_plots(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::CalcColumnPlotOp>()) {
      calc_column_plots(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ConcatDataFramesOp>()) {
      concat(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::FreezeDataFrameOp>()) {
      freeze(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameOp>()) {
      get_data_frame(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameHTMLOp>()) {
      get_data_frame_html(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameStringOp>()) {
      get_data_frame_string(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameContentOp>()) {
      get_data_frame_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetFloatColumnContentOp>()) {
      get_float_column_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameNBytesOp>()) {
      get_nbytes(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetDataFrameNRowsOp>()) {
      get_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::LastChangeOp>()) {
      last_change(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::RefreshDataFrameOp>()) {
      refresh(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::RemoveColumnOp>()) {
      remove_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::SummarizeDataFrameOp>()) {
      summarize(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ToArrowOp>()) {
      to_arrow(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ToCSVOp>()) {
      to_csv(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ToDBOp>()) {
      to_db(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ToParquetOp>()) {
      to_parquet(_cmd, _socket);
    } else {
      static_assert(fct::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  fct::visit(handle, _command.val_);
}

}  // namespace handlers
}  // namespace engine

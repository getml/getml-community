// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "json/json.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::get_data_frame(
    const typename Command::GetDataFrameOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  multithreading::ReadLock read_lock(params_.read_write_lock_);

  using CmdType = fct::TaggedUnion<
      "type_", typename commands::DataFrameCommand::GetFloatColumnOp,
      typename commands::DataFrameCommand::GetStringColumnOp, CloseDataFrameOp>;

  while (true) {
    const auto json_str = communication::Receiver::recv_string(_socket);

    const auto cmd = json::from_json<CmdType>(json_str);

    const auto handle = [this, _socket](const auto& _cmd) -> bool {
      using Type = std::decay_t<decltype(_cmd)>;
      if constexpr (std::is_same<Type, typename commands::DataFrameCommand::
                                           GetStringColumnOp>()) {
        ColumnManager(params_).get_categorical_column(_cmd, _socket);
        return false;
      } else if constexpr (std::is_same<Type,
                                        typename commands::DataFrameCommand::
                                            GetFloatColumnOp>()) {
        ColumnManager(params_).get_column(_cmd, _socket);
        return false;
      } else if constexpr (std::is_same<Type, CloseDataFrameOp>()) {
        communication::Sender::send_string("Success!", _socket);
        return true;
      }
    };

    const bool finished = fct::visit(handle, cmd);

    if (finished) {
      break;
    }
  }
}

}  // namespace handlers
}  // namespace engine
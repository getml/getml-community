// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <rfl/Ref.hpp>
#include <rfl/as.hpp>
#include <rfl/json/read.hpp>
#include <rfl/visit.hpp>

#include "engine/handlers/DataFrameManager.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::receive_data(
    const rfl::Ref<containers::Encoding>& _local_categories,
    const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df, Poco::Net::StreamSocket* _socket) const {
  using CmdType = rfl::TaggedUnion<
      "type_", typename commands::DataFrameCommand::FloatColumnOp,
      typename commands::DataFrameCommand::StringColumnOp, CloseDataFrameOp>;

  while (true) {
    const auto json_str = communication::Receiver::recv_string(_socket);

    const auto cmd = rfl::json::read<CmdType>(json_str).value();

    const auto handle = [this, &_local_categories, &_local_join_keys_encoding,
                         _df, _socket](const auto& _cmd) -> bool {
      using Type = std::decay_t<decltype(_cmd)>;
      if constexpr (std::is_same<
                        Type,
                        typename commands::DataFrameCommand::FloatColumnOp>()) {
        recv_and_add_float_column(rfl::as<RecvAndAddOp>(_cmd), _df, nullptr,
                                  _socket);
        communication::Sender::send_string("Success!", _socket);
        return false;
      } else if constexpr (std::is_same<Type,
                                        typename commands::DataFrameCommand::
                                            StringColumnOp>()) {
        recv_and_add_string_column(rfl::as<RecvAndAddOp>(_cmd),
                                   _local_categories, _local_join_keys_encoding,
                                   _df, _socket);
        communication::Sender::send_string("Success!", _socket);
        return false;
      } else if constexpr (std::is_same<Type, CloseDataFrameOp>()) {
        communication::Sender::send_string("Success!", _socket);
        return true;
      }
    };

    const bool finished = rfl::visit(handle, cmd);

    if (finished) {
      break;
    }
  }
}

}  // namespace handlers
}  // namespace engine

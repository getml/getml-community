// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <Poco/TemporaryFile.h>

#include "commands/DataFrameFromJSON.hpp"
#include "containers/Roles.hpp"
#include "engine/handlers/AggOpParser.hpp"
#include "engine/handlers/ArrowHandler.hpp"
#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "engine/handlers/ViewParser.hpp"
#include "metrics/metrics.hpp"
#include <rfl/always_false.hpp>

namespace engine {
namespace handlers {

void ColumnManager::execute_command(const Command& _command,
                                    Poco::Net::StreamSocket* _socket) {
  const auto handle = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, Command::FloatColumnOp>()) {
      add_float_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::StringColumnOp>()) {
      add_string_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::AggregationOp>()) {
      aggregate(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetBooleanColumnOp>()) {
      get_boolean_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetBooleanColumnContentOp>()) {
      get_boolean_column_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetBooleanColumnNRowsOp>()) {
      get_boolean_column_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetStringColumnOp>()) {
      get_categorical_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetStringColumnContentOp>()) {
      get_categorical_column_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetStringColumnNRowsOp>()) {
      get_categorical_column_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetStringColumnUniqueOp>()) {
      get_categorical_column_unique(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetFloatColumnOp>()) {
      get_column(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetFloatColumnNRowsOp>()) {
      get_column_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetFloatColumnUniqueOp>()) {
      get_column_unique(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetFloatColumnContentOp>()) {
      get_float_column_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetFloatColumnSubrolesOp>()) {
      get_subroles(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::GetStringColumnSubrolesOp>()) {
      get_subroles_categorical(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetFloatColumnUnitOp>()) {
      get_unit(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetStringColumnUnitOp>()) {
      get_unit_categorical(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::SetFloatColumnSubrolesOp>()) {
      set_subroles(_cmd, _socket);
    } else if constexpr (std::is_same<Type,
                                      Command::SetStringColumnSubrolesOp>()) {
      set_subroles_categorical(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::SetFloatColumnUnitOp>()) {
      set_unit(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::SetStringColumnUnitOp>()) {
      set_unit_categorical(_cmd, _socket);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  rfl::visit(handle, _command.val_);
}

}  // namespace handlers
}  // namespace engine

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "commands/DataFrameFromJSON.hpp"
#include "containers/Roles.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "rfl/json.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::from_json(const typename Command::AddDfFromJSONOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto json_str = communication::Receiver::recv_string(_socket);

  const auto append = _cmd.append();

  const auto& name = _cmd.schema().name();

  const auto time_formats = _cmd.time_formats();

  const auto schema = containers::Schema(_cmd.schema());

  const auto obj = rfl::json::read<commands::DataFrameFromJSON>(json_str);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = std::make_shared<containers::Encoding>(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = std::make_shared<containers::Encoding>(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  auto df = containers::DataFrame(name, local_categories,
                                  local_join_keys_encoding, pool);

  df.from_json(obj, time_formats, schema);

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  params_.join_keys_encoding_->append(*local_join_keys_encoding);

  df.set_categories(params_.categories_.ptr());

  df.set_join_keys_encoding(params_.join_keys_encoding_.ptr());

  if (!append || data_frames().find(name) == data_frames().end()) {
    data_frames()[name] = df;
  } else {
    data_frames()[name].append(df);
  }

  data_frames()[name].create_indices();

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

}  // namespace handlers
}  // namespace engine

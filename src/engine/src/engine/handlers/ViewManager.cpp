// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/ViewManager.hpp"

#include "communication/Sender.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/ViewParser.hpp"

#include <rfl/always_false.hpp>

namespace engine {
namespace handlers {

ViewManager::ViewManager(const DataFrameManagerParams& _params)
    : params_(_params) {}

void ViewManager::execute_command(const Command& _command,
                                  Poco::Net::StreamSocket* _socket) {
  const auto handle = [this, _socket](const auto& _cmd) {
    using Type = std::decay_t<decltype(_cmd)>;

    if constexpr (std::is_same<Type, Command::GetViewContentOp>()) {
      get_view_content(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::GetViewNRowsOp>()) {
      get_view_nrows(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ViewToArrowOp>()) {
      view_to_arrow(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ViewToCSVOp>()) {
      view_to_csv(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ViewToDBOp>()) {
      view_to_db(_cmd, _socket);
    } else if constexpr (std::is_same<Type, Command::ViewToParquetOp>()) {
      view_to_parquet(_cmd, _socket);
    } else {
      static_assert(rfl::always_false_v<Type>, "Not all cases were covered.");
    }
  };

  rfl::visit(handle, _command.val_);
}

// ------------------------------------------------------------------------

void ViewManager::get_view_content(
    const typename Command::GetViewContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto draw = _cmd.draw();

  const auto length = _cmd.length();

  const auto start = _cmd.start();

  const auto& cols = _cmd.cols();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto content =
      ViewParser(params_.categories_, params_.join_keys_encoding_,
                 params_.data_frames_, params_.options_)
          .get_content(draw, start, length, false, cols);

  read_lock.unlock();

  communication::Sender::send_string(rfl::json::write(content), _socket);
}

// ------------------------------------------------------------------------

void ViewManager::get_view_nrows(const typename Command::GetViewNRowsOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto cols = _cmd.cols();

  const auto force = _cmd.force();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto content =
      ViewParser(params_.categories_, params_.join_keys_encoding_,
                 params_.data_frames_, params_.options_)
          .get_content(1, 0, 0, force, cols);

  read_lock.unlock();

  communication::Sender::send_string(rfl::json::write(content), _socket);
}

// ------------------------------------------------------------------------

void ViewManager::view_to_arrow(const typename Command::ViewToArrowOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& view = _cmd.view();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto table = ViewParser(local_categories, local_join_keys_encoding,
                                params_.data_frames_, params_.options_)
                         .to_table(view);

  read_lock.unlock();

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_table(table, _socket);
}

// ------------------------------------------------------------------------

void ViewManager::view_to_csv(const typename Command::ViewToCSVOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& fname = _cmd.fname();

  const auto batch_size = _cmd.batch_size();

  const auto quotechar = _cmd.quotechar();

  const auto sep = _cmd.sep();

  const auto& view = _cmd.view();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto df = ViewParser(local_categories, local_join_keys_encoding,
                             params_.data_frames_, params_.options_)
                      .parse(view);

  DataFrameManager(params_).df_to_csv(fname, batch_size, quotechar, sep, df,
                                      params_.categories_,
                                      params_.join_keys_encoding_);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ViewManager::view_to_db(const typename Command::ViewToDBOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto& conn_id = _cmd.conn_id();

  const auto& table_name = _cmd.table_name();

  const auto& view = _cmd.view();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto df = ViewParser(local_categories, local_join_keys_encoding,
                             params_.data_frames_, params_.options_)
                      .parse(view);

  DataFrameManager(params_).df_to_db(conn_id, table_name, df, local_categories,
                                     local_join_keys_encoding);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void ViewManager::view_to_parquet(const typename Command::ViewToParquetOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto& fname = _cmd.fname();

  const auto& compression = _cmd.compression();

  const auto& view = _cmd.view();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto table = ViewParser(local_categories, local_join_keys_encoding,
                                params_.data_frames_, params_.options_)
                         .to_table(view);

  read_lock.unlock();

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .to_parquet(table, fname, compression);

  communication::Sender::send_string("Success!", _socket);
}
}  // namespace handlers
}  // namespace engine

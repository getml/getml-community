// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include <Poco/TemporaryFile.h>

#include "commands/DataFrameFromJSON.hpp"
#include "engine/containers/Roles.hpp"
#include "engine/handlers/AggOpParser.hpp"
#include "engine/handlers/ArrowHandler.hpp"
#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/DataFrameManager.hpp"
#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "engine/handlers/ViewParser.hpp"
#include "json/json.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::aggregate(const typename Command::AggregationOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto& aggregation = _cmd.get<"aggregation_">();

  auto response = containers::Column<Float>(nullptr, 1);

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  response[0] = AggOpParser(params_.categories_, params_.join_keys_encoding_,
                            params_.data_frames_)
                    .aggregate(aggregation);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_column(response, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::execute_command(const Command& _command,
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
      []<bool _flag = false>() {
        static_assert(_flag, "Not all cases were covered.");
      }
      ();
    }
  };

  fct::visit(handle, _command.val_);
}

// ------------------------------------------------------------------------

void DataFrameManager::freeze(const typename Command::FreezeDataFrameOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(name, &data_frames());

  df.freeze();

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::from_arrow(
    const typename Command::AddDfFromArrowOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto schema = containers::Schema(_cmd);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = fct::Ref<containers::Encoding>::make(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  const auto arrow_handler = handlers::ArrowHandler(
      local_categories, local_join_keys_encoding, params_.options_);

  auto df = arrow_handler.table_to_df(arrow_handler.recv_table(_socket), name,
                                      schema);

  // Now we upgrade the weak write lock to a strong write lock to commit
  // the changes.
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

// ------------------------------------------------------------------------

void DataFrameManager::from_csv(const typename Command::AddDfFromCSVOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto& colnames = _cmd.get<"colnames_">();

  const auto& fnames = _cmd.get<"fnames_">();

  const auto num_lines_read = _cmd.get<"num_lines_read_">();

  const auto quotechar = _cmd.get<"quotechar_">();

  const auto sep = _cmd.get<"sep_">();

  const auto skip = _cmd.get<"skip_">();

  const auto& time_formats = _cmd.get<"time_formats_">();

  const auto schema = containers::Schema(_cmd);

  // We need the weak write lock for the categories and join keys encoding.
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = std::make_shared<containers::Encoding>(
      pool, params_.join_keys_encoding_.ptr());

  auto df = containers::DataFrame(name, local_categories,
                                  local_join_keys_encoding, pool);

  df.from_csv(colnames, fnames, quotechar, sep, num_lines_read, skip,
              time_formats, schema);

  // Now we upgrade the weak write lock to a strong write lock to commit
  // the changes.
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

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::from_db(const typename Command::AddDfFromDBOp& _cmd,
                               Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto& conn_id = _cmd.get<"conn_id_">();

  const auto& table_name = _cmd.get<"table_name_">();

  const auto schema = containers::Schema(_cmd);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = std::make_shared<containers::Encoding>(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = std::make_shared<containers::Encoding>(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  auto df = containers::DataFrame(name, local_categories,
                                  local_join_keys_encoding, pool);

  df.from_db(connector(conn_id), table_name, schema);

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

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::from_json(const typename Command::AddDfFromJSONOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto json_str = communication::Receiver::recv_string(_socket);

  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto time_formats = _cmd.get<"time_formats_">();

  const auto schema = containers::Schema(_cmd);

  const auto obj = json::from_json<commands::DataFrameFromJSON>(json_str);

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

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::from_parquet(
    const typename Command::AddDfFromParquetOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto& fname = _cmd.get<"fname_">();

  const auto schema = containers::Schema(_cmd);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  const auto arrow_handler = handlers::ArrowHandler(
      local_categories, local_join_keys_encoding, params_.options_);

  auto df = arrow_handler.table_to_df(arrow_handler.read_parquet(fname), name,
                                      schema);

  // Now we upgrade the weak write lock to a strong write lock to commit
  // the changes.
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

// ------------------------------------------------------------------------

void DataFrameManager::from_query(
    const typename Command::AddDfFromQueryOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto& conn_id = _cmd.get<"conn_id_">();

  const auto& query = _cmd.get<"query_">();

  const auto schema = containers::Schema(_cmd);

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = std::make_shared<containers::Encoding>(
      pool, params_.join_keys_encoding_.ptr());

  auto df = containers::DataFrame(name, local_categories,
                                  local_join_keys_encoding, pool);

  df.from_query(connector(conn_id), query, schema);

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

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::from_view(const typename Command::AddDfFromViewOp& _cmd,
                                 Poco::Net::StreamSocket* _socket) {
  const auto append = _cmd.get<"append_">();

  const auto& name = _cmd.get<"name_">();

  const auto view = _cmd.get<"view_">();

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  auto local_categories =
      fct::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  auto local_join_keys_encoding = fct::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());

  auto df = ViewParser(local_categories, local_join_keys_encoding,
                       params_.data_frames_, params_.options_)
                .parse(view)
                .clone(name);

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

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column(
    const typename Command::GetBooleanColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      BoolOpParser(params_.categories_, params_.join_keys_encoding_,
                   params_.data_frames_)
          .parse(json_col);

  if (column_view.is_infinite()) {
    throw std::runtime_error(
        "The length of the column view is infinite! You can look at "
        "the column view, "
        "but you cannot retrieve it.");
  }

  const auto array = column_view.to_array(0, std::nullopt, false);

  read_lock.unlock();

  const auto field = arrow::field("column", arrow::boolean());

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_array(array, field, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column_content(
    const typename Command::GetBooleanColumnContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto draw = _cmd.get<"draw_">();

  const auto length = _cmd.get<"length_">();

  const auto start = _cmd.get<"start_">();

  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      BoolOpParser(params_.categories_, params_.join_keys_encoding_,
                   params_.data_frames_)
          .parse(json_col);

  const auto data_ptr = column_view.to_vector(start, length, false);

  assert_true(data_ptr);

  const auto nrows = std::holds_alternative<size_t>(column_view.nrows())
                         ? std::get<size_t>(column_view.nrows())
                         : length;

  const auto col_str =
      make_column_string<bool>(draw, nrows, data_ptr->begin(), data_ptr->end());

  communication::Sender::send_string(col_str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_boolean_column_nrows(
    const typename Command::GetBooleanColumnNRowsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      BoolOpParser(params_.categories_, params_.join_keys_encoding_,
                   params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(column_view.nrows_to_str(), _socket);
}
// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column(
    const typename Command::GetStringColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  if (column_view.is_infinite()) {
    throw std::runtime_error(
        "The length of the column view is infinite! You can look at "
        "the column view, "
        "but you cannot retrieve it.");
  }

  const auto array = column_view.to_array(0, std::nullopt, false);

  read_lock.unlock();

  const auto field = arrow::field("column", arrow::utf8());

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_array(array, field, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column_content(
    const typename Command::GetStringColumnContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto draw = _cmd.get<"draw_">();

  const auto length = _cmd.get<"length_">();

  const auto start = _cmd.get<"start_">();

  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  const auto data_ptr = column_view.to_vector(start, length, false);

  assert_true(data_ptr);

  const auto nrows = std::holds_alternative<size_t>(column_view.nrows())
                         ? std::get<size_t>(column_view.nrows())
                         : length;

  const auto col_str = make_column_string<strings::String>(
      draw, nrows, data_ptr->begin(), data_ptr->end());

  communication::Sender::send_string(col_str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column_nrows(
    const typename Command::GetStringColumnNRowsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(column_view.nrows_to_str(), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_categorical_column_unique(
    const typename Command::GetStringColumnUniqueOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  const auto array = column_view.unique();

  read_lock.unlock();

  const auto field = arrow::field("column", arrow::utf8());

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_array(array, field, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_column(
    const typename Command::GetFloatColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  if (column_view.is_infinite()) {
    throw std::runtime_error(
        "The length of the column view is infinite! You can look at "
        "the column view, "
        "but you cannot retrieve it.");
  }

  const auto array = column_view.to_array(0, std::nullopt, false);

  read_lock.unlock();

  const auto field =
      column_view.unit().find("time stamp") != std::string::npos
          ? arrow::field("column", arrow::timestamp(arrow::TimeUnit::NANO))
          : arrow::field("column", arrow::float64());

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_array(array, field, _socket);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_column_unique(
    const typename Command::GetFloatColumnUniqueOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  const auto array = column_view.unique();

  read_lock.unlock();

  const auto field =
      column_view.unit().find("time stamp") != std::string::npos
          ? arrow::field("column", arrow::timestamp(arrow::TimeUnit::NANO))
          : arrow::field("column", arrow::float64());

  handlers::ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                         params_.options_)
      .send_array(array, field, _socket);

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_content(
    const typename Command::GetDataFrameContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto draw = _cmd.get<"draw_">();

  const auto length = _cmd.get<"length_">();

  const auto start = _cmd.get<"start_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto content = df.get_content(draw, start, length);

  read_lock.unlock();

  communication::Sender::send_string(json::to_json(content), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_column_nrows(
    const typename Command::GetFloatColumnNRowsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(column_view.nrows_to_str(), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_html(
    const typename Command::GetDataFrameHTMLOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto max_rows = _cmd.get<"max_rows_">();

  const auto border = _cmd.get<"border_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto str = df.get_html(max_rows, border);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_string(
    const typename Command::GetDataFrameStringOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto str = df.get_string(20);

  read_lock.unlock();

  communication::Sender::send_string(str, _socket);
}

// ------------------------------------------------------------------------

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
        get_categorical_column(_cmd, _socket);
        return false;
      } else if constexpr (std::is_same<Type,
                                        typename commands::DataFrameCommand::
                                            GetFloatColumnOp>()) {
        get_column(_cmd, _socket);
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

// ------------------------------------------------------------------------

void DataFrameManager::get_float_column_content(
    const typename Command::GetFloatColumnContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto draw = _cmd.get<"draw_">();

  const auto length = _cmd.get<"length_">();

  const auto start = _cmd.get<"start_">();

  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  const auto col = column_view.to_column(start, length, false);

  const auto nrows = std::holds_alternative<size_t>(column_view.nrows())
                         ? std::get<size_t>(column_view.nrows())
                         : length;

  const auto col_str =
      make_column_string<Float>(draw, nrows, col.begin(), col.end());

  communication::Sender::send_string(col_str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nbytes(
    const typename Command::GetDataFrameNBytesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(name, &data_frames());

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(std::to_string(df.nbytes()), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nrows(
    const typename Command::GetDataFrameNRowsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(name, &data_frames());

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(std::to_string(df.nrows()), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_subroles(
    const typename Command::GetFloatColumnSubrolesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_categorical_column(column_view.subroles(),
                                                 _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_subroles_categorical(
    const typename Command::GetStringColumnSubrolesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_categorical_column(column_view.subroles(),
                                                 _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_unit(
    const typename Command::GetFloatColumnUnitOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      FloatOpParser(params_.categories_, params_.join_keys_encoding_,
                    params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(column_view.unit(), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_unit_categorical(
    const typename Command::GetStringColumnUnitOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& json_col = _cmd.get<"col_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto column_view =
      StringOpParser(params_.categories_, params_.join_keys_encoding_,
                     params_.data_frames_)
          .parse(json_col);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(column_view.unit(), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::last_change(const typename Command::LastChangeOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto df = utils::Getter::get(name, data_frames());

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(df.last_change(), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::receive_data(
    const fct::Ref<containers::Encoding>& _local_categories,
    const fct::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df, Poco::Net::StreamSocket* _socket) const {
  using CmdType = fct::TaggedUnion<
      "type_", typename commands::DataFrameCommand::FloatColumnOp,
      typename commands::DataFrameCommand::StringColumnOp, CloseDataFrameOp>;

  while (true) {
    const auto json_str = communication::Receiver::recv_string(_socket);

    const auto cmd = json::from_json<CmdType>(json_str);

    const auto handle = [this, &_local_categories, &_local_join_keys_encoding,
                         _df, _socket](const auto& _cmd) -> bool {
      using Type = std::decay_t<decltype(_cmd)>;
      if constexpr (std::is_same<
                        Type,
                        typename commands::DataFrameCommand::FloatColumnOp>()) {
        recv_and_add_float_column(_cmd, _df, nullptr, _socket);
        communication::Sender::send_string("Success!", _socket);
        return false;
      } else if constexpr (std::is_same<Type,
                                        typename commands::DataFrameCommand::
                                            StringColumnOp>()) {
        recv_and_add_string_column(_cmd, _local_categories,
                                   _local_join_keys_encoding, _df, _socket);
        communication::Sender::send_string("Success!", _socket);
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

// ------------------------------------------------------------------------

void DataFrameManager::recv_and_add_float_column(
    const RecvAndAddOp& _cmd, containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket) const {
  const auto role = _cmd.get<"role_">();

  const auto name = _cmd.get<"name_">();

  auto col = ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                          params_.options_)
                 .recv_column<Float>(_df->pool(), name, _socket);

  add_float_column_to_df(role, col, _df, _weak_write_lock);
}

// ------------------------------------------------------------------------

void DataFrameManager::recv_and_add_string_column(
    const RecvAndAddOp& _cmd, containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket) {
  assert_true(_weak_write_lock);

  const auto role = _cmd.get<"role_">();

  const auto name = _cmd.get<"name_">();

  const auto str_col =
      ArrowHandler(params_.categories_, params_.join_keys_encoding_,
                   params_.options_)
          .recv_column<strings::String>(_df->pool(), name, _socket);

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING ||
      role == containers::DataFrame::ROLE_TEXT) {
    add_string_column_to_df(name, role, "", str_col, _df, _weak_write_lock);
  } else {
    add_int_column_to_df(name, role, "", str_col, _df, _weak_write_lock,
                         _socket);
  }
}

// ------------------------------------------------------------------------

void DataFrameManager::recv_and_add_string_column(
    const RecvAndAddOp& _cmd,
    const fct::Ref<containers::Encoding>& _local_categories,
    const fct::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df, Poco::Net::StreamSocket* _socket) const {
  const auto role = _cmd.get<"role_">();

  const auto name = _cmd.get<"name_">();

  const auto str_col =
      ArrowHandler(_local_categories, _local_join_keys_encoding,
                   params_.options_)
          .recv_column<strings::String>(_df->pool(), name, _socket);

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING ||
      role == containers::DataFrame::ROLE_TEXT) {
    add_string_column_to_df(name, role, "", str_col, _df, nullptr);
  } else {
    add_int_column_to_df(name, role, "", str_col, _local_categories,
                         _local_join_keys_encoding, _df);
  }
}

// ------------------------------------------------------------------------

void DataFrameManager::refresh(const typename Command::RefreshDataFrameOp& _cmd,
                               Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto df = utils::Getter::get(name, data_frames());

  const auto roles = containers::Roles::from_schema(df.to_schema(false));

  read_lock.unlock();

  communication::Sender::send_string(json::to_json(roles), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::remove_column(
    const typename Command::RemoveColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto df_name = _cmd.get<"df_name_">();

  const auto name = _cmd.get<"name_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  const bool success = df.remove_column(name);

  if (!success) {
    throw std::runtime_error("Could not remove column. Column named '" + name +
                             "' not found.");
  }

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::set_subroles(
    const typename Command::SetFloatColumnSubrolesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& role = _cmd.get<"role_">();

  const auto& df_name = _cmd.get<"df_name_">();

  const auto& subroles = _cmd.get<"subroles_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  auto column = df.float_column(name, role);

  column.set_subroles(subroles);

  df.add_float_column(column, role);

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::set_subroles_categorical(
    const typename Command::SetStringColumnSubrolesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& role = _cmd.get<"role_">();

  const auto& df_name = _cmd.get<"df_name_">();

  const auto& subroles = _cmd.get<"subroles_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING) {
    auto column = df.unused_string(name);
    column.set_subroles(subroles);
    df.add_string_column(column, role);
  } else if (role == containers::DataFrame::ROLE_TEXT) {
    auto column = df.text(name);
    column.set_subroles(subroles);
    df.add_string_column(column, role);
  } else {
    auto column = df.int_column(name, role);
    column.set_subroles(subroles);
    df.add_int_column(column, role);
  }

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit(
    const typename Command::SetFloatColumnUnitOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& role = _cmd.get<"role_">();

  const auto& df_name = _cmd.get<"df_name_">();

  const auto& unit = _cmd.get<"unit_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  auto column = df.float_column(name, role);

  column.set_unit(unit);

  df.add_float_column(column, role);

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::set_unit_categorical(
    const typename Command::SetStringColumnUnitOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& role = _cmd.get<"role_">();

  const auto& df_name = _cmd.get<"df_name_">();

  const auto& unit = _cmd.get<"unit_">();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING) {
    auto column = df.unused_string(name);
    column.set_unit(unit);
    df.add_string_column(column, role);
  } else if (role == containers::DataFrame::ROLE_TEXT) {
    auto column = df.text(name);
    column.set_unit(unit);
    df.add_string_column(column, role);
  } else {
    auto column = df.int_column(name, role);
    column.set_unit(unit);
    df.add_int_column(column, role);
  }

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::summarize(
    const typename Command::SummarizeDataFrameOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto summary = df.to_monitor();

  read_lock.unlock();

  communication::Sender::send_string(json::to_json(summary), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::to_arrow(const typename Command::ToArrowOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, data_frames());

  const auto arrow_handler = handlers::ArrowHandler(
      params_.categories_, params_.join_keys_encoding_, params_.options_);

  const auto table = arrow_handler.df_to_table(df);

  read_lock.unlock();

  arrow_handler.send_table(table, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::to_csv(const typename Command::ToCSVOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& fname = _cmd.get<"fname_">();

  const auto batch_size = _cmd.get<"batch_size_">();

  const auto quotechar = _cmd.get<"quotechar_">();

  const auto sep = _cmd.get<"sep_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, data_frames());

  df_to_csv(fname, batch_size, quotechar, sep, df, params_.categories_,
            params_.join_keys_encoding_);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::to_db(const typename Command::ToDBOp& _cmd,
                             Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto conn_id = _cmd.get<"conn_id_">();

  const auto table_name = _cmd.get<"table_name_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, data_frames());

  df_to_db(conn_id, table_name, df, params_.categories_,
           params_.join_keys_encoding_);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::to_parquet(const typename Command::ToParquetOp& _cmd,
                                  Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.get<"name_">();

  const auto& fname = _cmd.get<"fname_">();

  const auto& compression = _cmd.get<"compression_">();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, data_frames());

  const auto arrow_handler = handlers::ArrowHandler(
      params_.categories_, params_.join_keys_encoding_, params_.options_);

  const auto table = arrow_handler.df_to_table(df);

  read_lock.unlock();

  arrow_handler.to_parquet(table, fname, compression);

  communication::Sender::send_string("Success!", _socket);
}

}  // namespace handlers
}  // namespace engine

// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "engine/handlers/DataFrameManager.hpp"

#include <Poco/TemporaryFile.h>

#include "commands/DataFrameFromJSON.hpp"
#include "containers/Roles.hpp"
#include "engine/handlers/AggOpParser.hpp"
#include "engine/handlers/ArrowHandler.hpp"
#include "engine/handlers/BoolOpParser.hpp"
#include "engine/handlers/ColumnManager.hpp"
#include "engine/handlers/FloatOpParser.hpp"
#include "engine/handlers/StringOpParser.hpp"
#include "engine/handlers/ViewParser.hpp"
#include "json/json.hpp"
#include "metrics/metrics.hpp"

namespace engine {
namespace handlers {

void DataFrameManager::add_data_frame(const std::string& _name,
                                      Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = rfl::Ref<containers::Encoding>::make(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  // TODO
  auto df = containers::DataFrame(_name, local_categories.ptr(),
                                  local_join_keys_encoding.ptr(), pool);

  communication::Sender::send_string("Success!", _socket);

  receive_data(local_categories, local_join_keys_encoding, &df, _socket);

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  params_.join_keys_encoding_->append(*local_join_keys_encoding);

  df.set_categories(params_.categories_.ptr());

  df.set_join_keys_encoding(params_.join_keys_encoding_.ptr());

  data_frames()[_name] = df;

  data_frames()[_name].create_indices();
}

// ------------------------------------------------------------------------

void DataFrameManager::add_float_column(
    const typename Command::AddFloatColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto& role = _cmd.role();

  const auto& name = _cmd.name();

  const auto& unit = _cmd.unit();

  const auto& json_col = _cmd.col();

  const auto& df_name = _cmd.df_name();

  const auto parser = FloatOpParser(
      params_.categories_, params_.join_keys_encoding_, params_.data_frames_);

  const auto column_view = parser.parse(json_col);

  const auto make_col = [this, column_view, parser, name, unit,
                         _socket](const std::optional<size_t> _nrows) {
    auto col = column_view.to_column(0, _nrows, false);

    col.set_name(name);

    col.set_unit(unit);

    parser.check(col, params_.logger_, _socket);

    return col;
  };

  auto [df, exists] = utils::Getter::get_if_exists(df_name, &data_frames());

  if (exists) {
    const auto col = make_col(df->nrows());

    add_float_column_to_df(role, col, df, &weak_write_lock);

  } else {
    if (column_view.is_infinite()) {
      throw std::runtime_error(
          "Column length could not be inferred! This is because "
          "you have "
          "tried to add an infinite length ColumnView to a data "
          "frame "
          "that does not contain any columns yet.");
    }

    const auto col = make_col(std::nullopt);

    const auto pool = params_.options_.make_pool();

    auto new_df =
        containers::DataFrame(df_name, params_.categories_.ptr(),
                              params_.join_keys_encoding_.ptr(), pool);

    add_float_column_to_df(role, col, &new_df, &weak_write_lock);

    data_frames()[df_name] = new_df;

    data_frames()[df_name].create_indices();
  }

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::add_float_column_to_df(
    const std::string& _role, const containers::Column<Float>& _col,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock) const {
  if (_weak_write_lock) _weak_write_lock->upgrade();

  _df->add_float_column(_col, _role);
}

// ------------------------------------------------------------------------

void DataFrameManager::add_int_column_to_df(
    const std::string& _name, const std::string& _role,
    const std::string& _unit, const containers::Column<strings::String>& _col,
    containers::DataFrame* _df, multithreading::WeakWriteLock* _weak_write_lock,
    Poco::Net::StreamSocket* _socket) {
  const auto pool = params_.options_.make_pool();

  const auto local_categories =
      std::make_shared<containers::Encoding>(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = std::make_shared<containers::Encoding>(
      pool, params_.join_keys_encoding_.ptr());

  auto col = containers::Column<Int>(_df->pool(), _col.nrows());

  auto encoding = local_join_keys_encoding;

  if (_role == containers::DataFrame::ROLE_CATEGORICAL) {
    encoding = local_categories;
  }

  for (size_t i = 0; i < _col.nrows(); ++i) {
    col[i] = (*encoding)[_col[i]];
  }

  col.set_name(_name);

  col.set_unit(_unit);

  assert_true(_weak_write_lock);

  _weak_write_lock->upgrade();

  _df->add_int_column(col, _role);

  if (_role == containers::DataFrame::ROLE_CATEGORICAL) {
    params_.categories_->append(*local_categories);
  } else if (_role == containers::DataFrame::ROLE_JOIN_KEY) {
    params_.join_keys_encoding_->append(*local_join_keys_encoding);
  } else {
    assert_true(false);
  }
}

// ------------------------------------------------------------------------

void DataFrameManager::add_int_column_to_df(
    const std::string& _name, const std::string& _role,
    const std::string& _unit, const containers::Column<strings::String>& _col,
    const rfl::Ref<containers::Encoding>& _local_categories,
    const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
    containers::DataFrame* _df) const {
  auto col = containers::Column<Int>(_df->pool(), _col.nrows());

  auto encoding = _local_join_keys_encoding;

  if (_role == containers::DataFrame::ROLE_CATEGORICAL) {
    encoding = _local_categories;
  }

  for (size_t i = 0; i < _col.nrows(); ++i) {
    col[i] = (*encoding)[_col[i]];
  }

  col.set_name(_name);

  col.set_unit(_unit);

  _df->add_int_column(col, _role);
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column(
    const typename Command::AddStringColumnOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto& role = _cmd.role();

  const auto& name = _cmd.name();

  const auto& unit = _cmd.unit();

  const auto& json_col = _cmd.col();

  const auto& df_name = _cmd.df_name();

  const auto parser = StringOpParser(
      params_.categories_, params_.join_keys_encoding_, params_.data_frames_);

  const auto pool = params_.options_.make_pool();

  auto new_df = containers::DataFrame(df_name, params_.categories_.ptr(),
                                      params_.join_keys_encoding_.ptr(), pool);

  auto [df, exists] = utils::Getter::get_if_exists(df_name, &data_frames());

  if (!exists) {
    df = &new_df;
  }

  const auto column_view = parser.parse(json_col);

  if (!exists && column_view.is_infinite()) {
    throw std::runtime_error(
        "Column length could not be inferred! This is because you have "
        "tried to add an infinite length ColumnView to a data frame "
        "that does not contain any columns yet.");
  }

  const auto col = exists ? column_view.to_column(0, df->nrows(), false)
                          : column_view.to_column(0, std::nullopt, false);

  parser.check(col, name, params_.logger_, _socket);

  if (role == containers::DataFrame::ROLE_UNUSED ||
      role == containers::DataFrame::ROLE_UNUSED_STRING ||
      role == containers::DataFrame::ROLE_TEXT) {
    add_string_column_to_df(name, role, unit, col, df, &weak_write_lock);
  } else {
    add_int_column_to_df(name, role, unit, col, df, &weak_write_lock, _socket);
  }

  if (!exists) {
    data_frames()[df_name] = new_df;

    data_frames()[df_name].create_indices();
  }

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::add_string_column_to_df(
    const std::string& _name, const std::string& _role,
    const std::string& _unit, const containers::Column<strings::String>& _col,
    containers::DataFrame* _df,
    multithreading::WeakWriteLock* _weak_write_lock) const {
  auto col = _col;

  col.set_name(_name);

  col.set_unit(_unit);

  if (_weak_write_lock) {
    _weak_write_lock->upgrade();
  }

  _df->add_string_column(col, _role);
}

// ------------------------------------------------------------------------

void DataFrameManager::append_to_data_frame(
    const typename Command::AppendToDataFrameOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = rfl::Ref<containers::Encoding>::make(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  // TODO
  auto df = containers::DataFrame(name, local_categories.ptr(),
                                  local_join_keys_encoding.ptr(), pool);

  receive_data(local_categories, local_join_keys_encoding, &df, _socket);

  weak_write_lock.upgrade();

  auto [old_df, exists] = utils::Getter::get_if_exists(name, &data_frames());

  if (exists) {
    old_df->append(df);
  } else {
    data_frames()[name] = df;
  }

  data_frames()[name].create_indices();

  params_.categories_->append(*local_categories);

  params_.join_keys_encoding_->append(*local_join_keys_encoding);
}

// ------------------------------------------------------------------------

void DataFrameManager::calc_categorical_column_plots(
    const typename Command::CalcCategoricalColumnPlotOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto& df_name = _cmd.df_name();

  const auto& role = _cmd.role();

  const auto num_bins = _cmd.num_bins();

  const auto& target_name = _cmd.target_name();

  const auto& target_role = _cmd.target_role();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(df_name, data_frames());

  auto vec = std::vector<strings::String>();

  if (role == containers::DataFrame::ROLE_CATEGORICAL) {
    const auto col = df.int_column(name, role);

    vec.resize(col.nrows());

    for (size_t i = 0; i < col.nrows(); ++i) {
      vec[i] = categories()[col[i]];
    }
  } else if (role == containers::DataFrame::ROLE_JOIN_KEY) {
    const auto col = df.int_column(name, role);

    vec.resize(col.nrows());

    for (size_t i = 0; i < col.nrows(); ++i) {
      vec[i] = join_keys_encoding()[col[i]];
    }
  } else if (role == containers::DataFrame::ROLE_TEXT) {
    const auto col = df.text(name);

    vec.resize(col.nrows());

    for (size_t i = 0; i < col.nrows(); ++i) {
      vec[i] = col[i];
    }
  } else if (role == containers::DataFrame::ROLE_UNUSED ||
             role == containers::DataFrame::ROLE_UNUSED_STRING) {
    const auto col = df.unused_string(name);

    vec.resize(col.nrows());

    for (size_t i = 0; i < col.nrows(); ++i) {
      vec[i] = col[i];
    }
  } else {
    throw std::runtime_error("Role '" + role + "' not known!");
  }

  std::vector<Float> targets;

  if (target_name != "") {
    const auto target_col = df.float_column(target_name, target_role);

    targets = std::vector<Float>(target_col.begin(), target_col.end());
  }

  read_lock.unlock();

  std::string json_str;

  if (targets.size() == vec.size()) {
    json_str = json::to_json(metrics::Summarizer::calc_categorical_column_plot(
        num_bins, vec, targets));
  } else {
    json_str = json::to_json(
        metrics::Summarizer::calc_categorical_column_plot(num_bins, vec));
  }

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json_str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::calc_column_plots(
    const typename Command::CalcColumnPlotOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto& df_name = _cmd.df_name();

  const auto& role = _cmd.role();

  const auto num_bins = _cmd.num_bins();

  const auto& target_name = _cmd.target_name();

  const auto& target_role = _cmd.target_role();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(df_name, data_frames());

  const auto col = df.float_column(name, role);

  std::vector<const Float*> targets;

  auto target_col = containers::Column<Float>(df.pool());

  if (target_name != "") {
    target_col = df.float_column(target_name, target_role);

    targets.push_back(target_col.data());
  }

  const containers::NumericalFeatures features = {
      helpers::Feature<Float>(col.to_vector_ptr())};

  const auto obj = metrics::Summarizer::calculate_feature_plots(
      features, col.nrows(), 1, num_bins, targets);

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(json::to_json(obj), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::concat(const typename Command::ConcatDataFramesOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto data_frame_objs = _cmd.data_frames();

  if (data_frame_objs.size() == 0) {
    throw std::runtime_error(
        "You should provide at least one data frame or view to "
        "concatenate!");
  }

  multithreading::WeakWriteLock weak_write_lock(params_.read_write_lock_);

  const auto pool = params_.options_.make_pool();

  const auto local_categories = rfl::Ref<containers::Encoding>::make(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
      pool, params_.join_keys_encoding_.ptr());  // TODO

  auto view_parser = ViewParser(local_categories, local_join_keys_encoding,
                                params_.data_frames_, params_.options_);

  const auto extract_df =
      [&view_parser](const auto& _cmd) -> containers::DataFrame {
    return view_parser.parse(_cmd);
  };

  auto range = data_frame_objs | VIEWS::transform(extract_df);

  auto df = range[0].clone(name);

  for (size_t i = 1; i < RANGES::size(range); ++i) {
    df.append(range[i]);
  }

  df.create_indices();

  df.check_plausibility();

  weak_write_lock.upgrade();

  params_.categories_->append(*local_categories);

  params_.join_keys_encoding_->append(*local_join_keys_encoding);

  data_frames()[name] = df;

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::df_to_csv(
    const std::string& _fname, const size_t _batch_size,
    const std::string& _quotechar, const std::string& _sep,
    const containers::DataFrame& _df,
    const rfl::Ref<containers::Encoding>& _categories,
    const rfl::Ref<containers::Encoding>& _join_keys_encoding) const {
  // We are using the bell character (\a) as the quotechar. It is least
  // likely to appear in any field.
  auto reader = containers::DataFrameReader(
      _df, _categories.ptr(), _join_keys_encoding.ptr(), '\a', '|');

  const auto colnames = reader.colnames();

  size_t fnum = 0;

  while (!reader.eof()) {
    auto fnum_str = std::to_string(++fnum);

    if (fnum_str.size() < 5) {
      fnum_str = std::string(5 - fnum_str.size(), '0') + fnum_str;
    }

    const auto current_fname =
        _batch_size == 0 ? _fname + ".csv" : _fname + "-" + fnum_str + ".csv";

    auto writer =
        io::CSVWriter(current_fname, _batch_size, colnames, _quotechar, _sep);

    writer.write(&reader);
  }
}

// ------------------------------------------------------------------------

void DataFrameManager::df_to_db(
    const std::string& _conn_id, const std::string& _table_name,
    const containers::DataFrame& _df,
    const rfl::Ref<containers::Encoding>& _categories,
    const rfl::Ref<containers::Encoding>& _join_keys_encoding) const {
  // We are using the bell character (\a) as the quotechar. It is least
  // likely to appear in any field.
  auto reader = containers::DataFrameReader(
      _df, _categories.ptr(), _join_keys_encoding.ptr(), '\a', '|');

  const auto conn = connector(_conn_id);

  const auto statement = io::StatementMaker::make_statement(
      _table_name, conn->dialect(), reader.colnames(), reader.coltypes());

  logger().log(statement);

  conn->execute(statement);

  conn->read(_table_name, 0, &reader);

  params_.database_manager_->post_tables();
}

// ------------------------------------------------------------------------

void DataFrameManager::freeze(const typename Command::FreezeDataFrameOp& _cmd,
                              Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

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

  const auto local_categories = rfl::Ref<containers::Encoding>::make(
      pool, params_.categories_.ptr());  // TODO

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
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

  weak_write_lock.unlock();

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

  weak_write_lock.unlock();

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
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  const auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
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

  weak_write_lock.unlock();

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
      rfl::Ref<containers::Encoding>::make(pool, params_.categories_.ptr());

  auto local_join_keys_encoding = rfl::Ref<containers::Encoding>::make(
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

  weak_write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_content(
    const typename Command::GetDataFrameContentOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto draw = _cmd.draw();

  const auto length = _cmd.length();

  const auto start = _cmd.start();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto content = df.get_content(draw, start, length);

  read_lock.unlock();

  communication::Sender::send_string(json::to_json(content), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_data_frame_html(
    const typename Command::GetDataFrameHTMLOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  const auto max_rows = _cmd.max_rows();

  const auto border = _cmd.border();

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
  const auto& name = _cmd.name();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto str = df.get_string(20);

  read_lock.unlock();

  communication::Sender::send_string(str, _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nbytes(
    const typename Command::GetDataFrameNBytesOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto nbytes = utils::Getter::get(name, &data_frames()).nbytes();

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(std::to_string(nbytes), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::get_nrows(
    const typename Command::GetDataFrameNRowsOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto nrows = utils::Getter::get(name, &data_frames()).nrows();

  read_lock.unlock();

  communication::Sender::send_string("Found!", _socket);

  communication::Sender::send_string(std::to_string(nrows), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::last_change(const typename Command::LastChangeOp& _cmd,
                                   Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto last_change =
      utils::Getter::get(name, data_frames()).last_change();

  read_lock.unlock();

  communication::Sender::send_string("Success!", _socket);

  communication::Sender::send_string(last_change, _socket);
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
    const rfl::Ref<containers::Encoding>& _local_categories,
    const rfl::Ref<containers::Encoding>& _local_join_keys_encoding,
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
  const auto& name = _cmd.name();

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
  const auto df_name = _cmd.df_name();

  const auto name = _cmd.name();

  multithreading::WriteLock write_lock(params_.read_write_lock_);

  auto& df = utils::Getter::get(df_name, &data_frames());

  const bool success = df.remove_column(name);

  if (!success) {
    throw std::runtime_error("Could not remove column. Column named '" + name +
                             "' not found.");
  }

  write_lock.unlock();

  communication::Sender::send_string("Success!", _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::summarize(
    const typename Command::SummarizeDataFrameOp& _cmd,
    Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

  multithreading::ReadLock read_lock(params_.read_write_lock_);

  const auto& df = utils::Getter::get(name, &data_frames());

  const auto summary = df.to_monitor();

  read_lock.unlock();

  communication::Sender::send_string(json::to_json(summary), _socket);
}

// ------------------------------------------------------------------------

void DataFrameManager::to_arrow(const typename Command::ToArrowOp& _cmd,
                                Poco::Net::StreamSocket* _socket) {
  const auto& name = _cmd.name();

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
  const auto& name = _cmd.name();

  const auto& fname = _cmd.fname();

  const auto batch_size = _cmd.batch_size();

  const auto quotechar = _cmd.quotechar();

  const auto sep = _cmd.sep();

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
  const auto& name = _cmd.name();

  const auto conn_id = _cmd.conn_id();

  const auto table_name = _cmd.table_name();

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
  const auto& name = _cmd.name();

  const auto& fname = _cmd.fname();

  const auto& compression = _cmd.compression();

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

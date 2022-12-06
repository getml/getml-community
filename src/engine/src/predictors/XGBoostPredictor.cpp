// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/XGBoostPredictor.hpp"

#include <optional>
#include <stdexcept>
#include <variant>

#include "predictors/XGBoostIteratorSparse.hpp"

namespace predictors {

void XGBoostPredictor::add_target(const DMatrixPtr &_d_matrix,
                                  const FloatFeature &_y) const {
  std::vector<float> y_float(_y.size());

  std::transform(_y.begin(), _y.end(), y_float.begin(),
                 [](const Float _val) { return static_cast<float>(_val); });

  if (XGDMatrixSetFloatInfo(*_d_matrix, "label", y_float.data(),
                            y_float.size()) != 0) {
    throw std::runtime_error("Setting XGBoost labels failed!");
  }
}

// -----------------------------------------------------------------------------

typename XGBoostPredictor::BoosterPtr XGBoostPredictor::allocate_booster(
    const DMatrixHandle _dmats[], bst_ulong _len) const {
  BoosterHandle *booster = new BoosterHandle;

  if (XGBoosterCreate(_dmats, _len, booster) != 0) {
    delete booster;

    throw std::runtime_error("Could not create XGBoost handle!");
  }

  return BoosterPtr(booster, &XGBoostPredictor::delete_booster);
}

// -----------------------------------------------------------------------------

typename XGBoostPredictor::DMatrixPtr
XGBoostPredictor::convert_to_in_memory_dmatrix(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical) const {
  if (_X_categorical.size() > 0) {
    return convert_to_in_memory_dmatrix_sparse(_X_categorical, _X_numerical);
  }
  return convert_to_in_memory_dmatrix_dense(_X_numerical);
}

// -----------------------------------------------------------------------------

XGBoostMatrix XGBoostPredictor::convert_to_memory_mapped_dmatrix(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y) const {
  if (_X_categorical.size() > 0) {
    return convert_to_memory_mapped_dmatrix_sparse(_X_categorical, _X_numerical,
                                                   _y);
  }
  return convert_to_memory_mapped_dmatrix_dense(_X_numerical, _y);
}

// -----------------------------------------------------------------------------

XGBoostMatrix XGBoostPredictor::convert_to_memory_mapped_dmatrix_dense(
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y) const {
  if (_X_numerical.size() == 0) {
    throw std::runtime_error("You must provide at least one column of data!");
  }

  assert_true(_X_numerical.at(0).is_memory_mapped());
  assert_true(_X_numerical.at(0).pool());

  const auto pool =
      std::make_shared<memmap::Pool>(_X_numerical.at(0).pool()->temp_dir());

  auto iter = std::make_unique<XGBoostIteratorDense>(_X_numerical, _y, pool);

  DMatrixHandle *handle = new DMatrixHandle;

  char config[] = "{\"missing\": NaN, \"cache_prefix\": \"cache\"}";

  if (XGDMatrixCreateFromCallback(
          iter.get(), iter->proxy(), XGBoostIteratorDense__reset,
          XGBoostIteratorDense__next, config, handle) != 0) {
    delete handle;

    throw std::runtime_error(
        std::string("Creating XGBoost DMatrix from Matrix failed: ") +
        XGBGetLastError());
  }

  auto d_matrix = DMatrixPtr(handle, &XGBoostIteratorDense::delete_dmatrix);

  return XGBoostMatrix{.d_matrix_ = std::move(d_matrix),
                       .iter_ = std::move(iter)};
}

// -----------------------------------------------------------------------------

XGBoostMatrix XGBoostPredictor::convert_to_memory_mapped_dmatrix_sparse(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y) const {
  if (_X_categorical.size() == 0) {
    throw std::runtime_error("You must provide at least one column of data!");
  }

  auto iter = std::make_unique<XGBoostIteratorSparse>(_X_categorical,
                                                      _X_numerical, _y, impl_);

  DMatrixHandle *handle = new DMatrixHandle;

  char config[] = "{\"missing\": NaN, \"cache_prefix\": \"cache\"}";

  if (XGDMatrixCreateFromCallback(
          iter.get(), iter->proxy(), XGBoostIteratorSparse__reset,
          XGBoostIteratorSparse__next, config, handle) != 0) {
    delete handle;

    throw std::runtime_error(
        std::string("Creating sparse XGBoost DMatrix from Matrix failed: ") +
        XGBGetLastError());
  }

  auto d_matrix = DMatrixPtr(handle, &XGBoostIteratorSparse::delete_dmatrix);

  return XGBoostMatrix{.d_matrix_ = std::move(d_matrix),
                       .iter_ = std::move(iter)};
}

// -----------------------------------------------------------------------------

typename XGBoostPredictor::DMatrixPtr
XGBoostPredictor::convert_to_in_memory_dmatrix_dense(
    const std::vector<FloatFeature> &_X_numerical) const {
  if (_X_numerical.size() == 0) {
    throw std::runtime_error("You must provide at least one column of data!");
  }

  std::vector<float> mat_float(_X_numerical.size() * _X_numerical[0].size());

  for (size_t j = 0; j < _X_numerical.size(); ++j) {
    if (_X_numerical[j].size() != _X_numerical[0].size()) {
      throw std::runtime_error("All columns must have the same length!");
    }

    for (size_t i = 0; i < _X_numerical[j].size(); ++i) {
      mat_float[i * _X_numerical.size() + j] =
          static_cast<float>(_X_numerical[j][i]);
    }
  }

  DMatrixHandle *d_matrix = new DMatrixHandle;

  if (XGDMatrixCreateFromMat(mat_float.data(), _X_numerical[0].size(),
                             _X_numerical.size(), -1, d_matrix) != 0) {
    delete d_matrix;

    throw std::runtime_error(
        std::string("Creating XGBoost DMatrix from Matrix failed: ") +
        XGBGetLastError() +
        " Do your "
        "features contain NAN or infinite values?");
  }

  return DMatrixPtr(d_matrix, &XGBoostIteratorDense::delete_dmatrix);
}

// -----------------------------------------------------------------------------

typename XGBoostPredictor::DMatrixPtr
XGBoostPredictor::convert_to_in_memory_dmatrix_sparse(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical) const {
  if (impl().n_encodings() != _X_categorical.size()) {
    const auto msg = "Expected " + std::to_string(impl().n_encodings()) +
                     " categorical columns, got " +
                     std::to_string(_X_categorical.size()) + ".";
    assert_msg(false, msg);
    throw std::runtime_error(msg);
  }

  const auto csr_mat = impl().make_csr<float, unsigned int, size_t>(
      _X_categorical, _X_numerical);

  DMatrixHandle *d_matrix = new DMatrixHandle;

  if (XGDMatrixCreateFromCSREx(csr_mat.indptr(), csr_mat.indices(),
                               csr_mat.data(), csr_mat.nrows() + 1,
                               csr_mat.size(), csr_mat.ncols(),
                               d_matrix) != 0) {
    delete d_matrix;

    throw std::runtime_error(
        std::string("Creating XGBoost DMatrix from CSRMatrix failed: ") +
        XGBGetLastError() +
        " Do your features contain NAN or infinite values?");
  }

  return DMatrixPtr(d_matrix, &XGBoostIteratorDense::delete_dmatrix);
}

// -----------------------------------------------------------------------------

Float XGBoostPredictor::evaluate_iter(const DMatrixPtr &_valid_set,
                                      const BoosterPtr &_handle,
                                      const int _n_iter) const {
  const char *eval_names[1] = {"getml_validation"};

  const char *out_result = NULL;

  if (XGBoosterEvalOneIter(*_handle, _n_iter, _valid_set.get(), eval_names, 1,
                           &out_result)) {
    throw std::runtime_error("XGBoost: Evaluating tree or linear model " +
                             std::to_string(_n_iter + 1) +
                             " failed: " + XGBGetLastError());
  }

  const auto out_result_str = std::string(out_result);

  const auto pos = out_result_str.rfind(":");

  assert_true(pos != std::string::npos);

  const auto value_str = out_result_str.substr(pos + 1);

  return static_cast<Float>(std::stod(value_str));
}

// -----------------------------------------------------------------------------

std::vector<Float> XGBoostPredictor::feature_importances(
    const size_t _num_features) const {
  auto handle = allocate_booster(NULL, 0);

  if (XGBoosterLoadModelFromBuffer(*handle, model(), len()) != 0) {
    throw std::runtime_error(std::string("Could not reload booster: ") +
                             XGBGetLastError());
  }

  bst_ulong out_len = 0;

  const char **out_dump_array = nullptr;

  if (XGBoosterDumpModel(*handle, "", 1, &out_len, &out_dump_array) != 0) {
    throw std::runtime_error(std::string("Generating XGBoost dump failed: ") +
                             XGBGetLastError());
  }

  std::vector<Float> all_feature_importances(impl().ncols_csr());

  for (bst_ulong i = 0; i < out_len; ++i) {
    parse_dump(out_dump_array[i], &all_feature_importances);
  }

  std::vector<Float> feature_importances(_num_features);

  impl().compress_importances(all_feature_importances, &feature_importances);

  Float sum_importances = std::accumulate(feature_importances.begin(),
                                          feature_importances.end(), 0.0);

  for (auto &val : feature_importances) {
    val = val / sum_importances;

    if (std::isnan(val)) {
      val = 0.0;
    }
  }

  return feature_importances;
}

// -----------------------------------------------------------------------------

Poco::JSON::Object::Ptr XGBoostPredictor::fingerprint() const {
  auto obj = Poco::JSON::Object::Ptr(new Poco::JSON::Object());

  obj->set("cmd_", cmd_);
  obj->set("dependencies_", JSON::vector_to_array_ptr(dependencies_));
  obj->set("impl_", impl().to_json_obj());

  return obj;
}

// -----------------------------------------------------------------------------

std::string XGBoostPredictor::fit(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical, const FloatFeature &_y,
    const std::optional<std::vector<IntFeature>> &_X_categorical_valid,
    const std::optional<std::vector<FloatFeature>> &_X_numerical_valid,
    const std::optional<FloatFeature> &_y_valid) {
  assert_true((_X_categorical_valid && true) == (_X_numerical_valid && true));

  assert_true((_X_categorical_valid && true) == (_y_valid && true));

  impl().check_plausibility(_X_categorical, _X_numerical, _y);

  const auto train_set = make_matrix(_X_categorical, _X_numerical, _y);

  const auto valid_set =

      _y_valid ? std::make_optional<XGBoostMatrix>(
                     make_matrix(_X_categorical_valid.value(),
                                 _X_numerical_valid.value(), _y_valid.value()))
               : std::optional<XGBoostMatrix>();

  auto handle = allocate_booster(train_set.get(), 1);

  set_hyperparameters(handle, _y.is_memory_mapped());

  fit_handle(_logger, train_set, valid_set, handle);

  const char *out_dptr;

  bst_ulong len = 0;

  if (XGBoosterGetModelRaw(*handle, &len, &out_dptr) != 0) {
    throw std::runtime_error("Storing of booster failed!");
  }

  model_ = std::vector<char>(out_dptr, out_dptr + len);

  std::stringstream msg;

  if (hyperparams_.booster_ == "gblinear") {
    msg << std::endl
        << "XGBoost: Trained " << hyperparams_.n_iter_ << " linear models.";
  } else {
    msg << std::endl
        << "XGBoost: Trained " << hyperparams_.n_iter_ << " trees.";
  }

  return msg.str();
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::fit_handle(
    const std::shared_ptr<const logging::AbstractLogger> _logger,
    const XGBoostMatrix &_train_set,
    const std::optional<XGBoostMatrix> &_valid_set,
    const BoosterPtr &_handle) const {
  const auto log = [this, _logger](const int _i, const int _n_iter) {
    if (!_logger) {
      return;
    }

    const auto progress = ((_i + 1) * 100) / _n_iter;

    const auto progress_str = "Progress: " + std::to_string(progress) + "%.";

    if (hyperparams_.booster_ == "gblinear") {
      _logger->log("XGBoost: Trained linear model " + std::to_string(_i + 1) +
                   ". " + progress_str);
    } else {
      _logger->log("XGBoost: Trained tree " + std::to_string(_i + 1) + ". " +
                   progress_str);
    }
  };

  size_t n_no_improvement = 0;

  Float best_value = std::numeric_limits<Float>::max();

  const auto evaluate = [this, &_valid_set, &n_no_improvement, &best_value,
                         &_handle](const int _i) -> bool {
    if (!_valid_set) {
      return false;
    }

    const auto value = evaluate_iter(_valid_set->d_matrix_, _handle, _i);

    if (value < best_value) {
      n_no_improvement = 0;
      best_value = value;
      return false;
    }

    ++n_no_improvement;

    if (n_no_improvement < hyperparams_.early_stopping_rounds_) [[likely]] {
        return false;
      }

    return true;
  };

  const auto n_iter = static_cast<int>(hyperparams_.n_iter_);

  for (int i = 0; i < n_iter; ++i) {
    if (XGBoosterUpdateOneIter(*_handle, i, *_train_set.get())) {
      throw std::runtime_error("XGBoost: Fitting tree or linear model " +
                               std::to_string(i + 1) +
                               " failed: " + XGBGetLastError());
    }

    if (evaluate(i)) [[unlikely]] {
        log(i, i);
        break;
      }

    log(i, n_iter);
  }
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::load(const std::string &_fname) {
  auto handle = allocate_booster(NULL, 0);

  if (XGBoosterLoadModel(*handle, _fname.c_str()) != 0) {
    throw std::runtime_error(std::string("Could not load XGBoostPredictor: ") +
                             XGBGetLastError());
  }

  const char *out_dptr;

  bst_ulong len = 0;

  if (XGBoosterGetModelRaw(*handle, &len, &out_dptr) != 0) {
    throw std::runtime_error("Storing of booster failed!");
  }

  model_ = std::vector<char>(out_dptr, out_dptr + len);
}

// -----------------------------------------------------------------------------

XGBoostMatrix XGBoostPredictor::make_matrix(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y) const {
  if (_X_categorical.size() == 0 && _X_numerical.size() == 0) {
    throw std::runtime_error("You must provide at least some features!");
  }

  const bool is_memory_mapped = _X_numerical.size() > 0
                                    ? _X_numerical.at(0).is_memory_mapped()
                                    : _X_categorical.at(0).is_memory_mapped();

  if (is_memory_mapped && hyperparams_.external_memory_) {
    return convert_to_memory_mapped_dmatrix(_X_categorical, _X_numerical, _y);
  }

  auto d_matrix = convert_to_in_memory_dmatrix(_X_categorical, _X_numerical);

  if (_y) {
    add_target(d_matrix, *_y);
  }

  return XGBoostMatrix{.d_matrix_ = std::move(d_matrix)};
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::parse_dump(
    const std::string &_dump,
    std::vector<Float> *_all_feature_importances) const {
  std::vector<std::string> lines;

  {
    std::stringstream stream(_dump);
    std::string line;
    while (std::getline(stream, line)) {
      lines.push_back(line);
    }
  }

  if (hyperparams_.booster_ == "gblinear") {
    assert_true(lines.size() >= _all_feature_importances->size() + 3);

    for (size_t i = 0; i < _all_feature_importances->size(); ++i) {
      (*_all_feature_importances)[i] =
          std::abs(std::atof(lines[i + 3].c_str()));
    }
  } else {
    // Parse individual lines, extracting the gain
    // A typical node might look like this:
    // 4:[f3<42.5] yes=9,no=10,missing=9,gain=8119.99414,cover=144
    // And a leaf looks like this:
    // 9:leaf=3.354321,cover=80

    for (auto &line : lines) {
      std::size_t begin = line.find("[f") + 2;

      std::size_t end = line.find("<");

      if (end != std::string::npos) {
        int fnum = std::stoi(line.substr(begin, end - begin));

        assert_true(fnum >= 0);

        assert_true(fnum < static_cast<int>(_all_feature_importances->size()));

        begin = line.find("gain=") + 5;

        assert_true(begin - 5 != std::string::npos);

        end = line.find(",", begin);

        assert_true(end != std::string::npos);

        Float gain = std::stod(line.substr(begin, end - begin));

        (*_all_feature_importances)[fnum] += gain;
      }
    }
  }
}

// -----------------------------------------------------------------------------

FloatFeature XGBoostPredictor::predict(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical) const {
  impl().check_plausibility(_X_categorical, _X_numerical);

  if (!is_fitted()) {
    throw std::runtime_error("XGBoostPredictor has not been fitted!");
  }

  const auto matrix = make_matrix(_X_categorical, _X_numerical, std::nullopt);

  auto handle = allocate_booster(matrix.get(), 1);

  if (XGBoosterLoadModelFromBuffer(*handle, model(), len()) != 0) {
    throw std::runtime_error(std::string("Could not reload booster: ") +
                             XGBGetLastError());
  }

  assert_true(_X_numerical.size() > 0 || _X_categorical.size() > 0);

  const auto size = _X_numerical.size() > 0 ? _X_numerical.at(0).size()
                                            : _X_categorical.at(0).size();

  auto yhat = FloatFeature(std::make_shared<std::vector<Float>>(size));

  bst_ulong nrows = 0;

  const float *yhat_float = nullptr;

  if (XGBoosterPredict(*handle, *matrix.get(), 0, 0, 0, &nrows, &yhat_float) !=
      0) {
    throw std::runtime_error(
        std::string("Generating XGBoost predictions failed!") +
        XGBGetLastError());
  }

  assert_msg(static_cast<size_t>(nrows) == yhat.size(),
             "nrows: " + std::to_string(nrows) +
                 ", yhat.size(): " + std::to_string(yhat.size()));

  std::transform(yhat_float, yhat_float + nrows, yhat.begin(),
                 [](const float val) { return static_cast<Float>(val); });

  return yhat;
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::save(const std::string &_fname) const {
  if (len() == 0) {
    throw std::runtime_error("XGBoostPredictor has not been fitted!");
  }

  auto handle = allocate_booster(NULL, 0);

  if (XGBoosterLoadModelFromBuffer(*handle, model(), len()) != 0) {
    std::runtime_error(std::string("Could not reload booster: ") +
                       XGBGetLastError());
  }

  if (XGBoosterSaveModel(*handle, _fname.c_str()) != 0) {
    throw std::runtime_error(std::string("Could not save XGBoostPredictor: ") +
                             XGBGetLastError());
  }
}

// -----------------------------------------------------------------------------

void XGBoostPredictor::set_hyperparameters(const BoosterPtr &_handle,
                                           const bool _is_memory_mapped) const {
  XGBoosterSetParam(*_handle, "alpha",
                    std::to_string(hyperparams_.alpha_).c_str());

  XGBoosterSetParam(*_handle, "booster", hyperparams_.booster_.c_str());

  XGBoosterSetParam(*_handle, "colsample_bytree",
                    std::to_string(hyperparams_.colsample_bytree_).c_str());

  XGBoosterSetParam(*_handle, "colsample_bylevel",
                    std::to_string(hyperparams_.colsample_bylevel_).c_str());

  XGBoosterSetParam(*_handle, "eta", std::to_string(hyperparams_.eta_).c_str());

  XGBoosterSetParam(*_handle, "gamma",
                    std::to_string(hyperparams_.gamma_).c_str());

  XGBoosterSetParam(*_handle, "lambda",
                    std::to_string(hyperparams_.lambda_).c_str());

  XGBoosterSetParam(*_handle, "max_delta_step",
                    std::to_string(hyperparams_.max_delta_step_).c_str());

  XGBoosterSetParam(*_handle, "max_depth",
                    std::to_string(hyperparams_.max_depth_).c_str());

  XGBoosterSetParam(*_handle, "min_child_weight",
                    std::to_string(hyperparams_.min_child_weights_).c_str());

  XGBoosterSetParam(*_handle, "num_parallel_tree",
                    std::to_string(hyperparams_.num_parallel_tree_).c_str());

  XGBoosterSetParam(*_handle, "normalize_type",
                    hyperparams_.normalize_type_.c_str());

  const auto nthread = impl().get_num_threads(hyperparams_.nthread_);

  XGBoosterSetParam(*_handle, "nthread", std::to_string(nthread).c_str());

  // XGBoost has deprecated reg::linear, but we will continue to support it.
  if (hyperparams_.objective_ == "reg:linear") {
    XGBoosterSetParam(*_handle, "objective", "reg:squarederror");
  } else {
    XGBoosterSetParam(*_handle, "objective", hyperparams_.objective_.c_str());
  }

  if (hyperparams_.one_drop_) {
    XGBoosterSetParam(*_handle, "one_drop", "1");
  } else {
    XGBoosterSetParam(*_handle, "one_drop", "0");
  }

  XGBoosterSetParam(*_handle, "rate_drop",
                    std::to_string(hyperparams_.rate_drop_).c_str());

  XGBoosterSetParam(*_handle, "sample_type", hyperparams_.sample_type_.c_str());

  if (hyperparams_.silent_) {
    XGBoosterSetParam(*_handle, "silent", "1");
  } else {
    XGBoosterSetParam(*_handle, "silent", "0");
  }

  XGBoosterSetParam(*_handle, "skip_drop",
                    std::to_string(hyperparams_.skip_drop_).c_str());

  XGBoosterSetParam(*_handle, "subsample",
                    std::to_string(hyperparams_.subsample_).c_str());

  // This is recommended by the XGBoost documentation.
  if (_is_memory_mapped && hyperparams_.external_memory_) {
    XGBoosterSetParam(*_handle, "tree_method", "approx");
  }
}

// -----------------------------------------------------------------------------
}  // namespace predictors

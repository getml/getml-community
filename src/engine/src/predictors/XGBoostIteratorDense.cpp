#include "predictors/XGBoostIteratorDense.hpp"

// ----------------------------------------------------------------------------

#include <unistd.h>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

namespace predictors {

XGBoostIteratorDense::XGBoostIteratorDense(
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y,
    const std::shared_ptr<memmap::Pool> &_pool)
    : batch_size_(calc_batch_size()),
      cur_it_(0),
      features_(init_features(_X_numerical, _pool)),
      nrows_(init_nrows(_X_numerical)),
      num_batches_(calc_num_batches(batch_size_, nrows_)),
      num_features_(_X_numerical.size()),
      proxy_(init_proxy()),
      targets_(init_targets(_y, _pool)) {
  assert_true(!targets_ || targets_->size() == nrows_);
  assert_true(num_batches_ * batch_size_ >= nrows_);
}

// ----------------------------------------------------------------------------

XGBoostIteratorDense::~XGBoostIteratorDense() = default;

// ----------------------------------------------------------------------------

size_t XGBoostIteratorDense::calc_batch_size() {
  const auto page_size = static_cast<size_t>(getpagesize());
  return page_size / sizeof(float);
}

// ----------------------------------------------------------------------------

size_t XGBoostIteratorDense::calc_num_batches(const size_t _batch_size,
                                              const size_t _nrows) {
  return _nrows % _batch_size == 0 ? _nrows / _batch_size
                                   : _nrows / _batch_size + 1;
}

// ----------------------------------------------------------------------------

size_t XGBoostIteratorDense::init_nrows(
    const std::vector<FloatFeature> &_X_numerical) {
  assert_true(_X_numerical.size() != 0);
  return _X_numerical.at(0).size();
}

// ----------------------------------------------------------------------------

std::shared_ptr<memmap::Vector<float>> XGBoostIteratorDense::init_features(
    const std::vector<FloatFeature> &_X_numerical,
    const std::shared_ptr<memmap::Pool> &_pool) {
  assert_true(_X_numerical.size() != 0);
  assert_true(_X_numerical.at(0).is_memory_mapped());
  assert_true(_pool);

  const auto features = std::make_shared<memmap::Vector<float>>(
      _pool, _X_numerical.size() * _X_numerical.at(0).size());

  for (size_t j = 0; j < _X_numerical.size(); ++j) {
    if (_X_numerical.at(j).size() != _X_numerical.at(0).size()) {
      throw std::invalid_argument("All columns must have the same length!");
    }
    for (size_t i = 0; i < _X_numerical[j].size(); ++i) {
      (*features)[i * _X_numerical.size() + j] =
          static_cast<float>(_X_numerical[j][i]);
    }
  }

  return features;
}

// ----------------------------------------------------------------------------

typename XGBoostIteratorDense::DMatrixPtr XGBoostIteratorDense::init_proxy() {
  DMatrixHandle *handle = new DMatrixHandle;
  if (XGProxyDMatrixCreate(handle) != XGBOOST_SUCCESS) {
    delete handle;
    throw std::runtime_error("Creating XGBoost proxy matrix failed!");
  }
  return DMatrixPtr(handle, &XGBoostIteratorDense::delete_dmatrix);
}

// ----------------------------------------------------------------------------

std::shared_ptr<memmap::Vector<float>> XGBoostIteratorDense::init_targets(
    const std::optional<FloatFeature> &_y,
    const std::shared_ptr<memmap::Pool> &_pool) {
  if (!_y) {
    return nullptr;
  }

  assert_true(_pool);

  const auto targets =
      std::make_shared<memmap::Vector<float>>(_pool, _y->size());

  std::transform(_y->begin(), _y->end(), targets->begin(),
                 [](const Float _val) { return static_cast<float>(_val); });

  return targets;
}

// ----------------------------------------------------------------------------

int XGBoostIteratorDense::next() {
  if (cur_it_ == num_batches_) {
    cur_it_ = 0;
    return END_IS_REACHED;
  }

  const char *array = update_array();

  auto result = XGProxyDMatrixSetDataDense(proxy(), array);

  if (result != XGBOOST_SUCCESS) {
    throw std::runtime_error(
        "Could not set up the proxy matrix: XGProxyDMatrixSetDataDense "
        "failed!");
  }

  if (targets_) {
    result = XGDMatrixSetDenseInfo(proxy(), "label", current_target_batch(),
                                   current_batch_size(), XGBOOST_TYPE_FLOAT);

    if (result != XGBOOST_SUCCESS) {
      throw std::runtime_error(
          "Could not set up the proxy matrix: XGDMatrixSetDenseInfo "
          "failed!");
    }
  }

  ++cur_it_;

  return CONTINUE;
}

// ----------------------------------------------------------------------------

char *XGBoostIteratorDense::update_array() {
  const char endianness = helpers::Endianness::is_little_endian() ? '<' : '>';

  const auto typestr = endianness + std::string("f4");

  const char format[] =
      "{\"data\": [%lu, false], \"shape\":[%lu, %lu], \"typestr\": "
      "\"%s\", \"version\": 3}";

  memset(array_, '\0', sizeof(array_));

  sprintf(array_, format, (size_t)(current_feature_batch()),
          current_batch_size(), num_features_, typestr.c_str());

  return array_;
}

// ----------------------------------------------------------------------------
}  // namespace predictors

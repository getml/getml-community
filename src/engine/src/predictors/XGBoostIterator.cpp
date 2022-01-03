#include "predictors/XGBoostIterator.hpp"

// ----------------------------------------------------------------------------

#include <unistd.h>

// ----------------------------------------------------------------------------

#include "debug/debug.hpp"

// ----------------------------------------------------------------------------

namespace predictors {

XGBoostIterator::XGBoostIterator(
    const std::shared_ptr<memmap::Vector<float>> &_features,
    const std::shared_ptr<memmap::Vector<float>> &_targets, const size_t _nrows)
    : batch_size_(calc_batch_size()),
      cur_it_(0),
      features_(_features),
      nrows_(_nrows),
      num_batches_(calc_num_batches(batch_size_, _nrows)),
      num_features_(calc_num_features(_features, _nrows)),
      proxy_(init_proxy()),
      targets_(_targets) {
  assert_true(!_targets || _targets->size() == nrows_);
  assert_true(num_batches_ * batch_size_ >= nrows_);
}

// ----------------------------------------------------------------------------

XGBoostIterator::~XGBoostIterator() = default;

// ----------------------------------------------------------------------------

size_t XGBoostIterator::calc_batch_size() {
  const auto page_size = static_cast<size_t>(getpagesize());
  return page_size / sizeof(float);
}

// ----------------------------------------------------------------------------

size_t XGBoostIterator::calc_num_batches(const size_t _batch_size,
                                         const size_t _nrows) {
  return _nrows % _batch_size == 0 ? _nrows / _batch_size
                                   : _nrows / _batch_size + 1;
}

// ----------------------------------------------------------------------------

size_t XGBoostIterator::calc_num_features(
    const std::shared_ptr<memmap::Vector<float>> &_features,
    const size_t _nrows) {
  assert_true(_features);
  assert_true(_features->size() % _nrows == 0);
  return _features->size() / _nrows;
}

// ----------------------------------------------------------------------------

typename XGBoostIterator::DMatrixPtr XGBoostIterator::init_proxy() {
  DMatrixHandle *handle = new DMatrixHandle;
  if (XGProxyDMatrixCreate(handle) != XGBOOST_SUCCESS) {
    delete handle;
    throw std::runtime_error("Creating XGBoost proxy matrix failed!");
  }
  return DMatrixPtr(handle, &XGBoostIterator::delete_dmatrix);
}

// ----------------------------------------------------------------------------

int XGBoostIterator::next() {
  if (cur_it_ == num_batches_) {
    cur_it_ = 0;
    return END_IS_REACHED;
  }

  set_array();

  auto result = XGProxyDMatrixSetDataDense(proxy(), array_);

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

void XGBoostIterator::set_array() {
  char array[] =
      "{\"data\": [%lu, false], \"shape\":[%lu, %lu], \"typestr\": "
      "\"<f4\", \"version\": 3}";

  memset(array_, '\0', sizeof(array_));

  sprintf(array_, array, (size_t)(current_feature_batch()),
          current_batch_size(), num_features_);
}

// ----------------------------------------------------------------------------
}  // namespace predictors

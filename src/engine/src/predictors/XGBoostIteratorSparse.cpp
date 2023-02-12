// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "predictors/XGBoostIteratorSparse.hpp"

#include <cstdint>

#include "debug/debug.hpp"
#include "helpers/Endianness.hpp"

namespace predictors {

XGBoostIteratorSparse::XGBoostIteratorSparse(
    const std::vector<IntFeature> &_X_categorical,
    const std::vector<FloatFeature> &_X_numerical,
    const std::optional<FloatFeature> &_y,
    const fct::Ref<const PredictorImpl> &_impl)
    : batch_size_(calc_batch_size()),
      cur_it_(0),
      impl_(_impl),
      nrows_(init_nrows(_X_categorical)),
      num_batches_(calc_num_batches(batch_size_, nrows_)),
      proxy_(init_proxy()),
      X_categorical_(_X_categorical),
      X_numerical_(_X_numerical),
      y_(_y) {
  assert_true(num_batches_ * batch_size_ >= nrows_);
}

// ----------------------------------------------------------------------------

XGBoostIteratorSparse::~XGBoostIteratorSparse() = default;

// ----------------------------------------------------------------------------

size_t XGBoostIteratorSparse::calc_batch_size() {
  const auto page_size = static_cast<size_t>(getpagesize());
  return page_size / sizeof(float);
}

// ----------------------------------------------------------------------------

size_t XGBoostIteratorSparse::calc_num_batches(const size_t _batch_size,
                                               const size_t _nrows) {
  return _nrows % _batch_size == 0 ? _nrows / _batch_size
                                   : _nrows / _batch_size + 1;
}

// ----------------------------------------------------------------------------

size_t XGBoostIteratorSparse::calc_num_features(
    const std::shared_ptr<memmap::Vector<float>> &_features,
    const size_t _nrows) {
  assert_true(_features);
  assert_true(_features->size() % _nrows == 0);
  return _features->size() / _nrows;
}

// ----------------------------------------------------------------------------

size_t XGBoostIteratorSparse::init_nrows(
    const std::vector<IntFeature> &_X_categorical) {
  assert_true(_X_categorical.size() != 0);
  return _X_categorical.at(0).size();
}

// ----------------------------------------------------------------------------

typename XGBoostIteratorSparse::DMatrixPtr XGBoostIteratorSparse::init_proxy() {
  DMatrixHandle *handle = new DMatrixHandle;
  if (XGProxyDMatrixCreate(handle) != XGBOOST_SUCCESS) {
    delete handle;
    throw std::runtime_error("Creating XGBoost proxy matrix failed!");
  }
  return DMatrixPtr(handle, &XGBoostIteratorSparse::delete_dmatrix);
}

// ----------------------------------------------------------------------------

std::unique_ptr<const typename XGBoostIteratorSparse::CSRMatrixType>
XGBoostIteratorSparse::make_proxy_csr() const {
  const auto begin = cur_it_ * batch_size_;

  const auto end = begin + current_batch_size();

  const auto get_subrange = [begin, end](const auto &_col) {
    assert_true(end <= _col.size());
    return fct::Range(_col.data() + begin, _col.data() + end);
  };

  const auto ranges_categorical = fct::collect::vector<fct::Range<const Int *>>(
      X_categorical_ | VIEWS::transform(get_subrange));

  const auto ranges_numerical = fct::collect::vector<fct::Range<const Float *>>(
      X_numerical_ | VIEWS::transform(get_subrange));

  using DataType = CSRMatrixType::DataType;
  using IndicesType = CSRMatrixType::IndicesType;
  using IndptrType = CSRMatrixType::IndptrType;

  return std::make_unique<const CSRMatrixType>(
      impl().make_csr<DataType, IndicesType, IndptrType>(ranges_categorical,
                                                         ranges_numerical));
}

// ----------------------------------------------------------------------------

std::vector<float> XGBoostIteratorSparse::make_current_target_batch() const {
  const auto to_float = [](const Float _val) -> float {
    return static_cast<float>(_val);
  };

  const auto begin = cur_it_ * batch_size_;

  assert_true(y_);

  auto batch = std::vector<float>(current_batch_size());

  std::transform(y_->data() + begin, y_->data() + begin + batch.size(),
                 batch.begin(), to_float);

  return batch;
}

// ----------------------------------------------------------------------------

std::tuple<const char *, const char *, const char *>
XGBoostIteratorSparse::make_array_interfaces(const CSRMatrixType &_proxy_csr) {
  const char endianness = helpers::Endianness::is_little_endian() ? '<' : '>';

  update_array(_proxy_csr.data(), _proxy_csr.size(),
               endianness + std::string("f4"), array_data_);

  update_array(_proxy_csr.indices(), _proxy_csr.size(),
               endianness + std::string("u4"), array_indices_);

  update_array(_proxy_csr.indptr(), _proxy_csr.nrows() + 1,
               endianness + std::string("u8"), array_indptr_);

  return std::make_tuple(array_data_, array_indices_, array_indptr_);
}

// ----------------------------------------------------------------------------

int XGBoostIteratorSparse::next() {
  if (cur_it_ == num_batches_) {
    cur_it_ = 0;
    return END_IS_REACHED;
  }

  proxy_csr_ = make_proxy_csr();

  assert_true(proxy_csr_);

  const auto ncol = static_cast<bst_ulong>(proxy_csr_->ncols());

  const auto [data, indices, indptr] = make_array_interfaces(*proxy_csr_);

  auto result = XGProxyDMatrixSetDataCSR(proxy(), indptr, indices, data, ncol);

  if (result != XGBOOST_SUCCESS) {
    throw std::runtime_error(
        std::string(
            "Could not set up the proxy matrix. XGBProxyDMatrixSetDataCSR "
            "failed: ") +
        XGBGetLastError());
  }

  if (y_) {
    const auto current_target_batch = make_current_target_batch();

    result =
        XGDMatrixSetDenseInfo(proxy(), "label", current_target_batch.data(),
                              current_batch_size(), XGBOOST_TYPE_FLOAT);

    if (result != XGBOOST_SUCCESS) {
      throw std::runtime_error(
          std::string(
              "Could not set up the proxy matrix. XGDMatrixSetSparseInfo "
              "failed: ") +
          XGBGetLastError());
    }
  }

  ++cur_it_;

  return CONTINUE;
}

// ----------------------------------------------------------------------------
}  // namespace predictors

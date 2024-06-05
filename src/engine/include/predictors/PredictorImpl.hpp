// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef PREDICTORS_PREDICTORIMPL_HPP_
#define PREDICTORS_PREDICTORIMPL_HPP_

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "debug/debug.hpp"
#include "helpers/Saver.hpp"
#include "memmap/memmap.hpp"
#include "predictors/CSRMatrix.hpp"
#include "predictors/Encoding.hpp"
#include "predictors/FloatFeature.hpp"
#include "predictors/IntFeature.hpp"
#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

namespace predictors {

/// Impl class for a predictor.
class PredictorImpl {
 public:
  using f_autofeatures =
      rfl::Field<"autofeatures_", std::vector<std::vector<size_t>>>;

  using f_categorical =
      rfl::Field<"categorical_colnames_", std::vector<std::string>>;

  using f_encoding = rfl::Field<"encodings_", std::vector<Encoding>>;

  using f_numerical =
      rfl::Field<"numerical_colnames_", std::vector<std::string>>;

  using ReflectionType =
      rfl::NamedTuple<f_autofeatures, f_categorical, f_encoding, f_numerical>;

 public:
  PredictorImpl(const std::vector<size_t>& _num_autofeatures,
                const std::vector<std::string>& _categorical_colnames,
                const std::vector<std::string>& _numerical_colnames);

  explicit PredictorImpl(const ReflectionType& _nt);

  ~PredictorImpl();

 public:
  /// Compresses importances calculated for a CSR Matrix to aggregated
  /// importances for each categorical column,
  void compress_importances(const std::vector<Float>& _all_feature_importances,
                            std::vector<Float>* _feature_importances) const;

  /// Makes sure that input columns passed by the user are plausible.
  size_t check_plausibility(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Makes sure that input columns passed by the user are plausible.
  void check_plausibility(const std::vector<IntFeature>& _X_categorical,
                          const std::vector<FloatFeature>& _X_numerical,
                          const FloatFeature& _y) const;

  /// Fits the encodings.
  void fit_encodings(const std::vector<IntFeature>& _X_categorical);

  /// Infers the appropriate number of threads to use.
  Int get_num_threads(const Int _num_threads) const;

  /// Generates a CSRMatrix from the categorical and numerical columns.
  template <typename DataType, typename IndicesType, typename IndptrType>
  CSRMatrix<DataType, IndicesType, IndptrType> make_csr(
      const std::vector<IntFeature>& _X_categorical,
      const std::vector<FloatFeature>& _X_numerical) const;

  /// Generates a CSRMatrix from the categorical and numerical ranges.
  template <typename DataType, typename IndicesType, typename IndptrType>
  CSRMatrix<DataType, IndicesType, IndptrType> make_csr(
      const std::vector<fct::Range<const Int*>>& _X_categorical,
      const std::vector<fct::Range<const Float*>>& _X_numerical) const;

  /// Select the columns that have made the cut during the feature selection.
  void select_features(const size_t _n_selected,
                       const std::vector<size_t>& _index);

  /// Saves the predictor impl as a JSON.
  void save(const std::string& _fname,
            const typename helpers::Saver::Format _format) const;

  /// Transforms the columns using the encodings.
  std::vector<IntFeature> transform_encodings(
      const std::vector<IntFeature>& _X_categorical) const;

 public:
  /// Trivial (const) getter.
  const std::vector<std::vector<size_t>>& autofeatures() const {
    return autofeatures_;
  }

  /// Trivial (const) getter.
  const std::vector<std::string>& categorical_colnames() const {
    return categorical_colnames_;
  }

  /// Trivial (const) getter.
  const std::vector<std::string>& numerical_colnames() const {
    return numerical_colnames_;
  }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const {
    return f_autofeatures(autofeatures_) *
           f_categorical(categorical_colnames_) * f_encoding(encodings_) *
           f_numerical(numerical_colnames_);
  }

  /// Number of encodings available.
  size_t n_encodings() const { return encodings_.size(); }

  /// Trivial (const) getter.
  Int n_unique(const size_t _i) const {
    assert_true(_i < encodings_.size());
    return encodings_[_i].n_unique();
  }

  /// The number of columns in CSR Matrix resulting from this Impl.
  size_t ncols_csr() const {
    size_t ncols = num_autofeatures() + numerical_colnames_.size();

    for (const auto& enc : encodings_) {
      ncols += enc.n_unique();
    }

    return ncols;
  }

  /// Trivial (const) getter.
  const size_t num_autofeatures() const {
    size_t n = 0;
    for (const auto& vec : autofeatures_) {
      n += vec.size();
    }
    return n;
  }

  /// Trivial (const) getter.
  const size_t num_manual_features() const {
    return categorical_colnames_.size() + numerical_colnames_.size();
  }

 private:
  /// Select columns that have made the cut during the feature selection.
  template <class T>
  std::vector<T> select_cols(const size_t _n_selected,
                             const std::vector<size_t>& _index,
                             const size_t _ix_begin,
                             const std::vector<T>& _colnames) const;

 private:
  /// The index of the autofeatures used.
  std::vector<std::vector<size_t>> autofeatures_;

  /// Names of the categorical columns taken from the population table as
  /// features.
  std::vector<std::string> categorical_colnames_;

  /// Encodings used for the categorical columns.
  std::vector<Encoding> encodings_;

  /// Names of the numerical columns taken from the population table as
  /// features.
  std::vector<std::string> numerical_colnames_;
};

}  // namespace predictors

// ----------------------------------------------------------------------------
// ----------------------------------------------------------------------------

namespace predictors {

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType> PredictorImpl::make_csr(
    const std::vector<IntFeature>& _X_categorical,
    const std::vector<FloatFeature>& _X_numerical) const {
  auto csr_mat = CSRMatrix<DataType, IndicesType, IndptrType>();

  for (const auto& col : _X_numerical) {
    csr_mat.add(col);
  }

  for (size_t i = 0; i < _X_categorical.size(); ++i) {
    csr_mat.add(_X_categorical[i], n_unique(i));
  }

  return csr_mat;
}

// ----------------------------------------------------------------------------

template <typename DataType, typename IndicesType, typename IndptrType>
CSRMatrix<DataType, IndicesType, IndptrType> PredictorImpl::make_csr(
    const std::vector<fct::Range<const Int*>>& _X_categorical,
    const std::vector<fct::Range<const Float*>>& _X_numerical) const {
  auto csr_mat = CSRMatrix<DataType, IndicesType, IndptrType>();

  for (const auto& col : _X_numerical) {
    csr_mat.add(col);
  }

  for (size_t i = 0; i < _X_categorical.size(); ++i) {
    csr_mat.add(_X_categorical[i], n_unique(i));
  }

  return csr_mat;
}

// ----------------------------------------------------------------------------

template <class T>
std::vector<T> PredictorImpl::select_cols(const size_t _n_selected,
                                          const std::vector<size_t>& _index,
                                          const size_t _ix_begin,
                                          const std::vector<T>& _cols) const {
  std::vector<T> selected;

  const auto begin = _index.begin();

  const auto end = _index.begin() + _n_selected;

  for (size_t i = 0; i < _cols.size(); ++i) {
    const auto is_included = [_ix_begin, i](size_t ix) {
      return ix == _ix_begin + i;
    };

    const auto it = std::find_if(begin, end, is_included);

    if (it != end) {
      selected.push_back(_cols[i]);
    }
  }

  return selected;
}

// ----------------------------------------------------------------------------
}  // namespace predictors

#endif  // PREDICTORS_PREDICTORIMPL_HPP_

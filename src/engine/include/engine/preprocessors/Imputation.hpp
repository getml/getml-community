// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_IMPUTATION_HPP_
#define ENGINE_PREPROCESSORS_IMPUTATION_HPP_

#include <Poco/JSON/Object.h>

#include <memory>
#include <utility>
#include <vector>

#include "engine/commands/Preprocessor.hpp"
#include "engine/containers/containers.hpp"
#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/TransformParams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/Ref.hpp"
#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class Imputation : public Preprocessor {
 private:
  typedef std::map<helpers::ColumnDescription, std::pair<Float, bool>>
      ImputationMapType;

 public:
  using ImputationOp = typename commands::Preprocessor::ImputationOp;

  using f_column_descriptions =
      fct::Field<"column_descriptions_",
                 std::vector<helpers::ColumnDescription>>;

  using f_means = fct::Field<"means_", std::vector<Float>>;

  using f_needs_dummies = fct::Field<"needs_dummies_", std::vector<bool>>;

  using NamedTupleType =
      fct::NamedTuple<f_column_descriptions, f_means, f_needs_dummies>;

 public:
  Imputation(const ImputationOp& _op,
             const std::vector<Poco::JSON::Object::Ptr>& _dependencies)
      : add_dummies_(_op.get<"add_dummies_">()), dependencies_(_dependencies) {}

  ~Imputation() = default;

 private:
  Imputation() {}

 public:
  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const final;

  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const FitParams& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const TransformParams& _params) const final;

 public:
  /// Creates a deep copy.
  std::shared_ptr<Preprocessor> clone(
      const std::optional<std::vector<Poco::JSON::Object::Ptr>>& _dependencies =
          std::nullopt) const final {
    const auto c = std::make_shared<Imputation>(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const {
    return f_column_descriptions(column_descriptions()) * f_means(means()) *
           f_needs_dummies(needs_dummies());
  }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final {
    return {};
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::IMPUTATION; }

 private:
  /// Adds a dummy column that assumes the value of 1, if and only if the
  /// original column is nan.
  void add_dummy(const containers::Column<Float>& _original_col,
                 containers::DataFrame* _df) const;

  /// Extracts an imputed column and adds it to the data frame.
  void extract_and_add(const std::string& _marker, const size_t _table,
                       const containers::Column<Float>& _original_col,
                       containers::DataFrame* _df);

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const std::string& _marker,
                                         const size_t _table);

  /// Replaces the original column with an imputed one. Returns a boolean
  /// indicating whether any value had to be imputed.
  bool impute(const containers::Column<Float>& _original_col,
              const Float _imputation_value, containers::DataFrame* _df) const;

  /// Retrieves all pairs in cols_ matching _marker and _table.
  std::vector<std::pair<Float, bool>> retrieve_pairs(const std::string& _marker,
                                                     const size_t _table) const;

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::DataFrame& _df,
                                     const std::string& _marker,
                                     const size_t _table) const;

 private:
  /// Trivial accessor
  ImputationMapType& cols() {
    assert_true(cols_);
    return *cols_;
  }

  /// Trivial accessor
  const ImputationMapType& cols() const {
    assert_true(cols_);
    return *cols_;
  }

  /// Retrieves the column description from the map.
  std::vector<helpers::ColumnDescription> column_descriptions() const {
    if (!cols_) {
      throw std::runtime_error(
          "The Imputation preprocessor has not been fitted.");
    }
    return fct::collect::vector<helpers::ColumnDescription>(*cols_ |
                                                            VIEWS::keys);
  }

  /// Retrieve the column description of all columns in cols_.
  std::vector<std::shared_ptr<helpers::ColumnDescription>> get_all_cols()
      const {
    std::vector<std::shared_ptr<helpers::ColumnDescription>> all_cols;
    for (const auto& [key, _] : cols()) {
      all_cols.push_back(std::make_shared<helpers::ColumnDescription>(key));
    }
    return all_cols;
  }

  /// Generates the colname for the newly created column.
  std::string make_dummy_name(const std::string& _colname) const {
    return helpers::Macros::dummy_begin() + _colname +
           helpers::Macros::dummy_end();
  }

  /// Generates the colname for the newly created column.
  std::string make_name(const std::string& _colname,
                        const Float _replacement) const {
    return helpers::Macros::imputation_begin() + _colname +
           helpers::Macros::imputation_replacement() +
           std::to_string(_replacement) + helpers::Macros::imputation_end();
  }

  /// Retrieves the means from the map.
  std::vector<Float> means() const {
    if (!cols_) {
      throw std::runtime_error(
          "The Imputation preprocessor has not been fitted.");
    }
    return fct::collect::vector<Float>(*cols_ | VIEWS::values | VIEWS::keys);
  }

  /// Retrieves the means from the map.
  std::vector<bool> needs_dummies() const {
    if (!cols_) {
      throw std::runtime_error(
          "The Imputation preprocessor has not been fitted.");
    }
    return fct::collect::vector<bool>(*cols_ | VIEWS::values | VIEWS::values);
  }

 private:
  /// Whether to create dummy columns.
  bool add_dummies_;

  /// Map of all columns to which the imputation transformation applies.
  /// Maps to the mean value and whether we need to build a dummy column.
  std::shared_ptr<ImputationMapType> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<Poco::JSON::Object::Ptr> dependencies_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_IMPUTATION_HPP_


// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_IMPUTATION_HPP_
#define ENGINE_PREPROCESSORS_IMPUTATION_HPP_

#include <memory>
#include <rfl/Field.hpp>
#include <rfl/Literal.hpp>
#include <rfl/NamedTuple.hpp>
#include <rfl/Ref.hpp>
#include <utility>
#include <vector>

#include "commands/Fingerprint.hpp"
#include "commands/Preprocessor.hpp"
#include "engine/Float.hpp"
#include "engine/preprocessors/Params.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "fct/to.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/Macros.hpp"
#include "helpers/StringIterator.hpp"

namespace engine {
namespace preprocessors {

class Imputation : public Preprocessor {
 private:
  typedef std::map<helpers::ColumnDescription, std::pair<Float, bool>>
      ImputationMapType;

  using MarkerType = typename helpers::ColumnDescription::MarkerType;

 public:
  using ImputationOp = typename commands::Preprocessor::ImputationOp;

  struct SaveLoad {
    rfl::Field<"column_descriptions_", std::vector<helpers::ColumnDescription>>
        column_descriptions;
    rfl::Field<"means_", std::vector<Float>> means;
    rfl::Field<"needs_dummies_", std::vector<bool>> needs_dummies;
  };

  using ReflectionType = SaveLoad;

 public:
  Imputation(const ImputationOp& _op,
             const std::vector<commands::Fingerprint>& _dependencies)
      : add_dummies_(_op.add_dummies()),
        cols_(rfl::Ref<ImputationMapType>::make()),
        dependencies_(_dependencies) {}

  ~Imputation() = default;

 private:
  Imputation() : cols_(rfl::Ref<ImputationMapType>::make()) {}

 public:
  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const Params& _params) final;

  /// Loads the predictor
  void load(const std::string& _fname) final;

  /// Stores the preprocessor.
  void save(const std::string& _fname,
            const typename helpers::Saver::Format& _format) const final;

  /// Transforms the data frames by adding the desired time series
  /// transformations.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  transform(const Params& _params) const final;

 public:
  /// Creates a deep copy.
  rfl::Ref<Preprocessor> clone(
      const std::optional<std::vector<commands::Fingerprint>>& _dependencies =
          std::nullopt) const final {
    const auto c = rfl::Ref<Imputation>::make(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  commands::Fingerprint fingerprint() const final {
    using FingerprintType =
        typename commands::Fingerprint::ImputationFingerprint;
    return commands::Fingerprint(FingerprintType{
        .dependencies = dependencies_,
        .op =
            commands::Preprocessor::ImputationOp{.add_dummies = add_dummies_}});
  }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const {
    return ReflectionType{.column_descriptions = column_descriptions(),
                          .means = means(),
                          .needs_dummies = needs_dummies()};
  }

  /// The preprocessor does not generate any SQL scripts.
  std::vector<std::string> to_sql(
      const helpers::StringIterator&,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&)
      const final {
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
  void extract_and_add(const MarkerType _marker, const size_t _table,
                       const containers::Column<Float>& _original_col,
                       containers::DataFrame* _df);

  /// Fits and transforms an individual data frame.
  containers::DataFrame fit_transform_df(const containers::DataFrame& _df,
                                         const MarkerType _marker,
                                         const size_t _table);

  /// Replaces the original column with an imputed one. Returns a boolean
  /// indicating whether any value had to be imputed.
  bool impute(const containers::Column<Float>& _original_col,
              const Float _imputation_value, containers::DataFrame* _df) const;

  /// Retrieves all pairs in cols_ matching _marker and _table.
  std::vector<std::pair<Float, bool>> retrieve_pairs(const MarkerType _marker,
                                                     const size_t _table) const;

  /// Transforms a single data frame.
  containers::DataFrame transform_df(const containers::DataFrame& _df,
                                     const MarkerType _marker,
                                     const size_t _table) const;

 private:
  /// Trivial accessor
  ImputationMapType& cols() { return *cols_; }

  /// Trivial accessor
  const ImputationMapType& cols() const { return *cols_; }

  /// Retrieves the column description from the map.
  std::vector<helpers::ColumnDescription> column_descriptions() const {
    return *cols_ | std::views::keys | fct::ranges::to<std::vector>();
  }

  /// Retrieve the column description of all columns in cols_.
  std::vector<rfl::Ref<helpers::ColumnDescription>> get_all_cols() const {
    std::vector<rfl::Ref<helpers::ColumnDescription>> all_cols;
    for (const auto& [key, _] : cols()) {
      all_cols.push_back(rfl::Ref<helpers::ColumnDescription>::make(key));
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
    return *cols_ | std::views::values | std::views::keys |
           fct::ranges::to<std::vector>();
  }

  /// Retrieves the means from the map.
  std::vector<bool> needs_dummies() const {
    return *cols_ | std::views::values | std::views::values |
           fct::ranges::to<std::vector>();
  }

 private:
  /// Whether to create dummy columns.
  bool add_dummies_;

  /// Map of all columns to which the imputation transformation applies.
  /// Maps to the mean value and whether we need to build a dummy column.
  rfl::Ref<ImputationMapType> cols_;

  /// The dependencies inserted into the the preprocessor.
  std::vector<commands::Fingerprint> dependencies_;
};

}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_IMPUTATION_HPP_

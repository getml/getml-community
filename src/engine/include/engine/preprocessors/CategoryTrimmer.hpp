// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef ENGINE_PREPROCESSORS_CATEGORYTRIMMER_HPP_
#define ENGINE_PREPROCESSORS_CATEGORYTRIMMER_HPP_

#include <Poco/JSON/Object.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "engine/JSON.hpp"
#include "engine/commands/Preprocessor.hpp"
#include "engine/containers/containers.hpp"
#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/TransformParams.hpp"
#include "fct/Field.hpp"
#include "fct/Literal.hpp"
#include "fct/NamedTuple.hpp"
#include "helpers/ColumnDescription.hpp"
#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

namespace engine {
namespace preprocessors {

class CategoryTrimmer : public Preprocessor {
 public:
  using CategoryPair =
      std::pair<helpers::ColumnDescription, fct::Ref<const std::set<Int>>>;

  using CategoryTrimmerOp = typename commands::Preprocessor::CategoryTrimmerOp;

  using f_peripheral_sets =
      fct::Field<"peripheral_sets_", std::vector<std::vector<CategoryPair>>>;

  using f_population_sets =
      fct::Field<"population_sets_", std::vector<CategoryPair>>;

  using NamedTupleType = fct::NamedTuple<f_peripheral_sets, f_population_sets>;

  static constexpr const char* TRIMMED = "(trimmed)";

 public:
  CategoryTrimmer(const CategoryTrimmerOp& _op,
                  const std::vector<Poco::JSON::Object::Ptr>& _dependencies)
      : dependencies_(_dependencies),
        max_num_categories_(_op.get<"max_num_categories_">()),
        min_freq_(_op.get<"min_freq_">()) {}

  ~CategoryTrimmer() = default;

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

  /// Generates the mapping tables.
  std::vector<std::string> to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator) const final;

 public:
  /// Creates a deep copy.
  std::shared_ptr<Preprocessor> clone(
      const std::optional<std::vector<Poco::JSON::Object::Ptr>>& _dependencies =
          std::nullopt) const final {
    const auto c = std::make_shared<CategoryTrimmer>(*this);
    if (_dependencies) {
      c->dependencies_ = *_dependencies;
    }
    return c;
  }

  /// Returns the type of the preprocessor.
  std::string type() const final { return Preprocessor::CATEGORY_TRIMMER; }

  /// Trivial (const) accessor.
  size_t max_num_categories() const { return max_num_categories_; }

  /// Trivial (const) accessor.
  size_t min_freq() const { return min_freq_; }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const {
    return f_peripheral_sets(peripheral_sets_) *
           f_population_sets(population_sets_);
  }

 private:
  /// Expresses the transformation of a particular column in SQL code.
  std::string column_to_sql(
      const helpers::StringIterator& _categories,
      const std::shared_ptr<const transpilation::SQLDialectGenerator>&
          _sql_dialect_generator,
      const CategoryPair& _pair) const;

  /// Fits on a single data frame.
  std::vector<CategoryPair> fit_df(const containers::DataFrame& _df,
                                   const std::string& _marker) const;

  /// Generates a cateogory set for a column.
  fct::Ref<const std::set<Int>> make_category_set(
      const containers::Column<Int>& _col) const;

  /// Generates the counts for each category, sorted in descending order.
  std::vector<std::pair<Int, size_t>> make_counts(
      const containers::Column<Int>& _col) const;

  /// Generates a map mapping each category to its count.
  std::map<Int, size_t> make_map(const containers::Column<Int>& _col) const;

  /// Transforms a single data frame using the appropriate set.
  containers::DataFrame transform_df(
      const std::vector<CategoryPair>& _sets,
      const std::shared_ptr<memmap::Pool>& _pool,
      const fct::Ref<const containers::Encoding>& _categories,
      const containers::DataFrame& _df) const;

 private:
  /// The dependencies inserted into the the preprocessor.
  std::vector<Poco::JSON::Object::Ptr> dependencies_;

  /// The maximum number of categories.
  size_t max_num_categories_;

  /// The minimum number required for a category to be included.
  size_t min_freq_;

  /// The sets for the population tables.
  std::vector<std::vector<CategoryPair>> peripheral_sets_;

  /// The sets for the population table.
  std::vector<CategoryPair> population_sets_;
};

// ----------------------------------------------------
}  // namespace preprocessors
}  // namespace engine

#endif  // ENGINE_PREPROCESSORS_CATEGORYTRIMMER_HPP_

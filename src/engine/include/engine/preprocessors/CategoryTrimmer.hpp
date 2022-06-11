#ifndef ENGINE_PREPROCESSORS_CATEGORYTRIMMER_HPP_
#define ENGINE_PREPROCESSORS_CATEGORYTRIMMER_HPP_

// ----------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// ----------------------------------------------------------------------------

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

// ----------------------------------------------------------------------------

#include "helpers/ColumnDescription.hpp"
#include "helpers/helpers.hpp"
#include "strings/strings.hpp"

// ----------------------------------------------------------------------------

#include "engine/JSON.hpp"
#include "engine/containers/containers.hpp"

// ----------------------------------------------------------------------------

#include "engine/preprocessors/FitParams.hpp"
#include "engine/preprocessors/Preprocessor.hpp"
#include "engine/preprocessors/TransformParams.hpp"

// ----------------------------------------------------------------------------

namespace engine {
namespace preprocessors {

class CategoryTrimmer : public Preprocessor {
  using CategoryPair =
      std::pair<helpers::ColumnDescription, fct::Ref<const std::set<Int>>>;

  static constexpr const char* TRIMMED = "(trimmed)";

 public:
  CategoryTrimmer() : max_num_categories_(999), min_freq_(30) {}

  CategoryTrimmer(const Poco::JSON::Object& _obj,
                  const std::vector<Poco::JSON::Object::Ptr>& _dependencies)
      : dependencies_(_dependencies) {
    *this = from_json_obj(_obj);
  }

  ~CategoryTrimmer() = default;

 public:
  /// Returns the fingerprint of the preprocessor (necessary to build
  /// the dependency graphs).
  Poco::JSON::Object::Ptr fingerprint() const final;

  /// Identifies which features should be extracted from which time stamps.
  std::pair<containers::DataFrame, std::vector<containers::DataFrame>>
  fit_transform(const FitParams& _params) final;

  /// Expresses the Seasonal preprocessor as a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const final;

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

 private:
  /// Transform a JSON object to a category pair.
  CategoryPair category_pair_from_obj(
      const Poco::JSON::Object::Ptr& _ptr) const;

  /// Transform a category pair to a JSON object.
  Poco::JSON::Object::Ptr category_pair_to_obj(const CategoryPair& _pair) const;

  /// Fits on a single data frame.
  std::vector<CategoryPair> fit_df(const containers::DataFrame& _df,
                                   const std::string& _marker) const;

  /// Parses a JSON object.
  CategoryTrimmer from_json_obj(const Poco::JSON::Object& _obj) const;

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

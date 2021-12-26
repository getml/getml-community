#ifndef HELPERS_VOCABULARYCONTAINER_HPP_
#define HELPERS_VOCABULARYCONTAINER_HPP_

// -------------------------------------------------------------------------

#include <Poco/JSON/Object.h>

// -------------------------------------------------------------------------

#include <memory>
#include <vector>

// -------------------------------------------------------------------------

#include "stl/stl.hpp"
#include "strings/strings.hpp"

// -------------------------------------------------------------------------

#include "helpers/DataFrame.hpp"
#include "helpers/StringIterator.hpp"

// -------------------------------------------------------------------------

namespace helpers {
// -------------------------------------------------------------------------

class VocabularyContainer {
 public:
  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

 public:
  VocabularyContainer(size_t _min_df, size_t _max_size,
                      const DataFrame& _population,
                      const std::vector<DataFrame>& _peripheral);

  VocabularyContainer(const VocabForDf& _population,
                      const std::vector<VocabForDf>& _peripheral);

  explicit VocabularyContainer(const Poco::JSON::Object& _obj);

  ~VocabularyContainer();

  /// Transforms the VocabularyContainer into a JSON object.
  Poco::JSON::Object::Ptr to_json_obj() const;

 public:
  /// Trivial (const) accessor
  const std::vector<VocabForDf>& peripheral() const { return peripheral_; }

  /// Represents the peripheral vocabulary as a vector of iterators.
  const std::vector<std::vector<StringIterator>> peripheral_iterators() const {
    const auto range = peripheral_ | VIEWS::transform(to_iterators);
    return stl::collect::vector<std::vector<StringIterator>>(range);
  }

  /// Trivial (const) accessor
  const VocabForDf& population() const { return population_; }

  /// Represents the population vocabulary as a vector of iterators.
  const std::vector<StringIterator> population_iterators() const {
    return to_iterators(population_);
  }

 private:
  /// Turns a vocab for df into a set of string iterators
  static std::vector<StringIterator> to_iterators(const VocabForDf& _vocab) {
    using VecType = VocabForDf::value_type;
    const auto get_value = [](const VecType& _vec,
                              const size_t _i) -> strings::String {
      return (*_vec)[_i];
    };
    const auto make_iterator = [get_value](const VecType& _vec) {
      assert_true(_vec);
      return StringIterator(std::bind(get_value, _vec, std::placeholders::_1),
                            _vec->size());
    };
    const auto range = _vocab | VIEWS::transform(make_iterator);
    return stl::collect::vector<StringIterator>(range);
  }

 private:
  /// The vocabulary for the peripheral tables.
  std::vector<VocabForDf> peripheral_;

  /// The vocabulary for the population table.
  VocabForDf population_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_VOCABULARYCONTAINER_HPP_


// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef HELPERS_VOCABULARYCONTAINER_HPP_
#define HELPERS_VOCABULARYCONTAINER_HPP_

#include <memory>
#include <vector>

#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "fct/collect.hpp"
#include "helpers/DataFrame.hpp"
#include "helpers/StringIterator.hpp"
#include "strings/strings.hpp"

namespace helpers {

class VocabularyContainer {
 public:
  typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
      VocabForDf;

  /// The vocabulary for the peripheral tables.
  using f_peripheral = fct::Field<"peripheral_", std::vector<VocabForDf>>;

  /// The vocabulary for the population table.
  using f_population = fct::Field<"population_", VocabForDf>;

  using NamedTupleType = fct::NamedTuple<f_peripheral, f_population>;

 public:
  VocabularyContainer(size_t _min_df, size_t _max_size,
                      const DataFrame& _population,
                      const std::vector<DataFrame>& _peripheral);

  VocabularyContainer(const VocabForDf& _population,
                      const std::vector<VocabForDf>& _peripheral);

  explicit VocabularyContainer(const NamedTupleType& _val);

  ~VocabularyContainer() = default;

 public:
  /// Trivial (const) accessor
  const NamedTupleType& named_tuple() const { return val_; }

  /// Trivial (const) accessor
  const std::vector<VocabForDf>& peripheral() const {
    return val_.get<f_peripheral>();
  }

  /// Represents the peripheral vocabulary as a vector of iterators.
  const std::vector<std::vector<StringIterator>> peripheral_iterators() const {
    const auto range = peripheral() | VIEWS::transform(to_iterators);
    return fct::collect::vector<std::vector<StringIterator>>(range);
  }

  /// Trivial (const) accessor
  const VocabForDf& population() const { return val_.get<f_population>(); }

  /// Represents the population vocabulary as a vector of iterators.
  const std::vector<StringIterator> population_iterators() const {
    return to_iterators(population());
  }

 private:
  /// Generates a new container from the input values.
  static VocabularyContainer make_container(
      size_t _min_df, size_t _max_size, const DataFrame& _population,
      const std::vector<DataFrame>& _peripheral);

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
    return fct::collect::vector<StringIterator>(range);
  }

 private:
  /// The underlying data
  NamedTupleType val_;
};

}  // namespace helpers

#endif  // HELPERS_VOCABULARYCONTAINER_HPP_


// Copyright 2024 Code17 GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_
#define FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_

#include "fastprop/algorithm/FastProp.hpp"
#include "transpilation/SQLDialectGenerator.hpp"

#include <rfl/Field.hpp>
#include <rfl/NamedTuple.hpp>

#include <memory>
#include <vector>

namespace fastprop {
namespace subfeatures {

class FastPropContainer {
 public:
  using Subcontainers = std::vector<std::shared_ptr<const FastPropContainer>>;

  using ReflectionType = rfl::NamedTuple<
      rfl::Field<"fast_prop_", std::shared_ptr<const algorithm::FastProp>>,
      rfl::Field<"subcontainers_", std::shared_ptr<const Subcontainers>>>;

 public:
  FastPropContainer(
      const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
      const std::shared_ptr<const Subcontainers>& _subcontainers);

  explicit FastPropContainer(const ReflectionType& _val);

  ~FastPropContainer() = default;

 public:
  /// Trivial (const) accessor.
  const algorithm::FastProp& fast_prop() const {
    assert_true(fast_prop_);
    return *fast_prop_;
  }

  /// Whether there is a fast_prop contained in the container.
  bool has_fast_prop() const { return (fast_prop_ && true); }

  /// Necessary for the automated parsing to work.
  ReflectionType reflection() const {
    return rfl::make_field<"fast_prop_">(fast_prop_) *
           rfl::make_field<"subcontainers_">(subcontainers_);
  }

  /// Returns the number of peripheral tables
  size_t size() const {
    assert_true(subcontainers_);
    return subcontainers_->size();
  }

  /// Trivial accessor
  std::shared_ptr<const FastPropContainer> subcontainers(size_t _i) const {
    assert_true(subcontainers_);
    assert_true(_i < subcontainers_->size());
    return subcontainers_->at(_i);
  }

  /// Expresses the features to in SQL code.
  void to_sql(const helpers::StringIterator& _categories,
              const helpers::VocabularyTree& _vocabulary,
              const std::shared_ptr<const transpilation::SQLDialectGenerator>&
                  _sql_dialect_generator,
              const std::string& _feature_prefix, const bool _subfeatures,
              std::vector<std::string>* _sql) const;

 private:
  /// The algorithm used to generate new features.
  const std::shared_ptr<const algorithm::FastProp> fast_prop_;

  /// The FastPropContainer used for any subtrees.
  const std::shared_ptr<const Subcontainers> subcontainers_;
};

}  // namespace subfeatures
}  // namespace fastprop

#endif  // FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_

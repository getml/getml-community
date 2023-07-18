// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#ifndef FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_
#define FASTPROP_SUBFEATURES_FASTPROPCONTAINER_HPP_

#include <memory>
#include <vector>

#include "fastprop/algorithm/algorithm.hpp"
#include "fct/Field.hpp"
#include "fct/NamedTuple.hpp"
#include "transpilation/SQLDialectGenerator.hpp"

namespace fastprop {
namespace subfeatures {

class FastPropContainer {
 public:
  using Subcontainers = std::vector<std::shared_ptr<const FastPropContainer>>;

  using NamedTupleType = fct::NamedTuple<
      fct::Field<"fast_prop_", std::shared_ptr<const algorithm::FastProp>>,
      fct::Field<"subcontainers_", std::shared_ptr<const Subcontainers>>>;

 public:
  FastPropContainer(
      const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
      const std::shared_ptr<const Subcontainers>& _subcontainers);

  FastPropContainer(const NamedTupleType& _val)
      : fast_prop_(_val.get<"fast_prop_">()),
        subcontainers_(_val.get<"subcontainers_">()) {}

  ~FastPropContainer();

 public:
  /// Trivial (const) accessor.
  const algorithm::FastProp& fast_prop() const {
    assert_true(fast_prop_);
    return *fast_prop_;
  }

  /// Whether there is a fast_prop contained in the container.
  bool has_fast_prop() const { return (fast_prop_ && true); }

  /// Necessary for the automated parsing to work.
  NamedTupleType named_tuple() const {
    return fct::make_field<"fast_prop_">(fast_prop_) *
           fct::make_field<"subcontainers_">(subcontainers_);
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


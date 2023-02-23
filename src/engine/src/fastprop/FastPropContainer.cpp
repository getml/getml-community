// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "fastprop/subfeatures/FastPropContainer.hpp"

namespace fastprop {
namespace subfeatures {
// ----------------------------------------------------------------------------

FastPropContainer::FastPropContainer(
    const std::shared_ptr<const algorithm::FastProp>& _fast_prop,
    const std::shared_ptr<const Subcontainers>& _subcontainers)
    : fast_prop_(_fast_prop), subcontainers_(_subcontainers) {
  assert_true(subcontainers_);
}

// ----------------------------------------------------------------------------

FastPropContainer::~FastPropContainer() = default;

// ----------------------------------------------------------------------------

void FastPropContainer::to_sql(
    const helpers::StringIterator& _categories,
    const helpers::VocabularyTree& _vocabulary,
    const std::shared_ptr<const transpilation::SQLDialectGenerator>&
        _sql_dialect_generator,
    const std::string& _prefix, const bool _subfeatures,
    std::vector<std::string>* _sql) const {
  if (has_fast_prop()) {
    const auto features =
        fast_prop().to_sql(_categories, _vocabulary, _sql_dialect_generator,
                           _prefix, 0, _subfeatures);

    _sql->insert(_sql->end(), features.begin(), features.end());

    if (_subfeatures) {
      const auto to_feature_name =
          [&_prefix](const size_t _feature_num) -> std::string {
        return "feature_" + _prefix + std::to_string(_feature_num + 1);
      };

      const auto iota = fct::iota<size_t>(0, fast_prop().num_features());

      const auto autofeatures = fct::collect::vector<std::string>(
          iota | VIEWS::transform(to_feature_name));

      const auto main_table =
          transpilation::SQLGenerator::make_staging_table_name(
              fast_prop().placeholder().name());

      assert_true(_sql_dialect_generator);

      _sql->push_back(_sql_dialect_generator->make_feature_table(
                          main_table, autofeatures, {}, {}, {},
                          "_" + _prefix + "PROPOSITIONALIZATION") +
                      "\n");
    }
  }

  if (_subfeatures) {
    for (size_t i = 0; i < size(); ++i) {
      if (subcontainers(i)) {
        subcontainers(i)->to_sql(
            _categories, _vocabulary, _sql_dialect_generator,
            _prefix + std::to_string(i + 1) + "_", _subfeatures, _sql);
      }
    }
  }
}

// ----------------------------------------------------------------------------
}  // namespace subfeatures
}  // namespace fastprop

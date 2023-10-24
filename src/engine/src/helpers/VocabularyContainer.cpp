// Copyright 2022 The SQLNet Company GmbH
//
// This file is licensed under the Elastic License 2.0 (ELv2).
// Refer to the LICENSE.txt file in the root of the repository
// for details.
//

#include "helpers/VocabularyContainer.hpp"

namespace helpers {

// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer(
    size_t _min_df, size_t _max_size, const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral)
    : VocabularyContainer(
          make_container(_min_df, _max_size, _population, _peripheral)) {}

// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer(
    const VocabForDf& _population, const std::vector<VocabForDf>& _peripheral)
    : val_(f_peripheral(_peripheral) * f_population(_population)) {}

// ----------------------------------------------------------------------------

VocabularyContainer::VocabularyContainer(const NamedTupleType& _val)
    : val_(_val) {}

// ----------------------------------------------------------------------------

VocabularyContainer VocabularyContainer::make_container(
    size_t _min_df, size_t _max_size, const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral) {
  const auto extract_from_col =
      [_min_df, _max_size](const Column<strings::String>& col) {
        return textmining::Vocabulary::generate(
            _min_df, _max_size, fct::Range(col.begin(), col.end()));
      };

  const auto extract_from_df =
      [extract_from_col](const DataFrame& df) -> VocabForDf {
    auto range = df.text_ | VIEWS::transform(extract_from_col);
    return VocabForDf(range.begin(), range.end());
  };

  const auto peripheral_dfs =
      fct::collect::vector(_peripheral | VIEWS::transform(extract_from_df));

  const auto population_dfs = extract_from_df(_population);

  const auto val = f_peripheral(peripheral_dfs) * f_population(population_dfs);

  return VocabularyContainer(val);
}

}  // namespace helpers

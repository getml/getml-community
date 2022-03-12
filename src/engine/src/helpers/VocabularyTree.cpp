#include "helpers/VocabularyTree.hpp"

// ----------------------------------------------------------------------------

#include "fct/fct.hpp"

// ----------------------------------------------------------------------------

#include "helpers/Macros.hpp"

// ----------------------------------------------------------------------------

namespace helpers {
// ----------------------------------------------------------------------------

VocabularyTree::VocabularyTree(
    const VocabForDf& _population, const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::vector<Schema>& _peripheral_schema)
    : peripheral_(parse_peripheral(_population, _peripheral, _placeholder,
                                   _peripheral_names, _peripheral_schema)),
      population_(_population),
      subtrees_(parse_subtrees(_population, _peripheral, _placeholder,
                               _peripheral_names, _peripheral_schema)) {
  assert_true(peripheral_.size() == subtrees_.size());
}

// ----------------------------------------------------------------------------

VocabularyTree::~VocabularyTree() = default;

// ----------------------------------------------------------------------------

typename VocabularyTree::VocabForDf VocabularyTree::find_peripheral(
    const std::vector<VocabForDf>& _peripheral, const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names) {
  const auto it = std::find(_peripheral_names.begin(), _peripheral_names.end(),
                            _placeholder.name_);

  if (it == _peripheral_names.end()) {
    throw std::runtime_error("Peripheral table named '" + _placeholder.name_ +
                             "' not found!");
  }

  const auto ix = std::distance(_peripheral_names.begin(), it);

  assert_true(ix >= 0);

  assert_true(static_cast<size_t>(ix) < _peripheral.size());

  return _peripheral.at(ix);
}

// ----------------------------------------------------------------------------

std::vector<typename VocabularyTree::VocabForDf>
VocabularyTree::find_text_fields(
    const std::vector<VocabForDf>& _peripheral, const Placeholder& _placeholder,
    const std::vector<Schema>& _peripheral_schema) {
  assert_msg(_peripheral_schema.size() == _peripheral.size(),
             "_peripheral_schema.size(): " +
                 std::to_string(_peripheral_schema.size()) +
                 ", _peripheral.size(): " + std::to_string(_peripheral.size()));

  const auto is_relevant_text_field =
      [&_placeholder, &_peripheral_schema](const size_t _i) -> bool {
    const auto table_name = _placeholder.name() + Macros::staging_table_num();

    const auto begin = _peripheral_schema.at(_i).name().find(table_name);

    if (begin == std::string::npos) {
      return false;
    }

    return _peripheral_schema.at(_i).name().find(Macros::text_field(),
                                                 begin + table_name.size()) !=
           std::string::npos;
  };

  const auto get_vocab = [&_peripheral](const size_t _i) -> VocabForDf {
    return _peripheral.at(_i);
  };

  const auto iota = fct::iota<size_t>(0, _peripheral_schema.size());

  const auto range = iota | VIEWS::filter(is_relevant_text_field) |
                     VIEWS::transform(get_vocab);

  return fct::collect::vector<VocabForDf>(range);
}

// ----------------------------------------------------------------------------

std::vector<typename VocabularyTree::VocabForDf>
VocabularyTree::parse_peripheral(
    const VocabForDf& _population, const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::vector<Schema>& _peripheral_schema) {
  const auto extract_peripheral = std::bind(
      find_peripheral, _peripheral, std::placeholders::_1, _peripheral_names);

  const auto peripheral = fct::collect::vector<VocabForDf>(
      _placeholder.joined_tables_ | VIEWS::transform(extract_peripheral));

  const auto text_fields =
      find_text_fields(_peripheral, _placeholder, _peripheral_schema);

  return fct::join::vector<VocabForDf>({peripheral, text_fields});
}

// ----------------------------------------------------------------------------

std::vector<std::optional<VocabularyTree>> VocabularyTree::parse_subtrees(
    const VocabForDf& _population, const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const std::vector<Schema>& _peripheral_schema) {
  const auto extract_peripheral = std::bind(
      find_peripheral, _peripheral, std::placeholders::_1, _peripheral_names);

  const auto make_subtree =
      [&_peripheral, &_peripheral_names, extract_peripheral,
       &_peripheral_schema](
          const Placeholder& _p) -> std::optional<VocabularyTree> {
    if (_p.joined_tables_.size() == 0) {
      return std::nullopt;
    }

    const auto new_population = extract_peripheral(_p);

    if (new_population.size() == 0 && _p.joined_tables_.size() == 0) {
      return std::nullopt;
    }

    return VocabularyTree(new_population, _peripheral, _p, _peripheral_names,
                          _peripheral_schema);
  };

  const auto vocab_for_text_fields =
      find_text_fields(_peripheral, _placeholder, _peripheral_schema);

  auto subtrees_for_placeholder =
      fct::collect::vector<std::optional<VocabularyTree>>(
          _placeholder.joined_tables_ | VIEWS::transform(make_subtree));

  // Text fields never have subtrees.
  for (size_t i = 0; i < vocab_for_text_fields.size(); ++i) {
    subtrees_for_placeholder.push_back(std::nullopt);
  }

  return subtrees_for_placeholder;
}

// ----------------------------------------------------------------------------
}  // namespace helpers

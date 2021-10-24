#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

VocabularyTree::VocabularyTree(
    const VocabForDf& _population,
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
    : peripheral_( parse_peripheral(
          _population, _peripheral, _placeholder, _peripheral_names ) ),
      population_( _population ),
      subtrees_( parse_subtrees(
          _population, _peripheral, _placeholder, _peripheral_names ) )
{
    assert_true( peripheral_.size() == subtrees_.size() );
}

// ----------------------------------------------------------------------------

VocabularyTree::~VocabularyTree() = default;

// ----------------------------------------------------------------------------

typename VocabularyTree::VocabForDf VocabularyTree::find_peripheral(
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
{
    const auto it = std::find(
        _peripheral_names.begin(),
        _peripheral_names.end(),
        _placeholder.name_ );

    if ( it == _peripheral_names.end() )
        {
            throw std::invalid_argument(
                "Peripheral table named '" + _placeholder.name_ +
                "' not found!" );
        }

    const auto ix = std::distance( _peripheral_names.begin(), it );

    assert_true( ix >= 0 );

    assert_true( static_cast<size_t>( ix ) < _peripheral.size() );

    return _peripheral.at( ix );
}

// ----------------------------------------------------------------------------

std::vector<typename VocabularyTree::VocabForDf>
VocabularyTree::parse_peripheral(
    const VocabForDf& _population,
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
{
    const auto extract_peripheral = std::bind(
        find_peripheral,
        _peripheral,
        std::placeholders::_1,
        _peripheral_names );

    const auto range =
        _placeholder.joined_tables_ | VIEWS::transform( extract_peripheral );

    return stl::collect::vector<VocabForDf>( range );
}

// ----------------------------------------------------------------------------

std::vector<std::optional<VocabularyTree>> VocabularyTree::parse_subtrees(
    const VocabForDf& _population,
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names )
{
    const auto extract_peripheral = std::bind(
        find_peripheral,
        _peripheral,
        std::placeholders::_1,
        _peripheral_names );

    const auto make_subtree =
        [&_peripheral, &_peripheral_names, extract_peripheral](
            const Placeholder& _p ) -> std::optional<VocabularyTree> {
        if ( _p.joined_tables_.size() == 0 )
            {
                return std::nullopt;
            }

        const auto new_population = extract_peripheral( _p );

        if ( new_population.size() == 0 && _p.joined_tables_.size() == 0 )
            {
                return std::nullopt;
            }

        return VocabularyTree(
            new_population, _peripheral, _p, _peripheral_names );
    };

    const auto range =
        _placeholder.joined_tables_ | VIEWS::transform( make_subtree );

    return stl::collect::vector<std::optional<VocabularyTree>>( range );
}

// ----------------------------------------------------------------------------
}  // namespace helpers

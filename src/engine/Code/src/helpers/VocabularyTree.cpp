#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

VocabularyTree::VocabularyTree(
    const VocabForDf& _population,
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const bool _split_text_fields )
    : peripheral_( parse_peripheral(
          _population,
          _peripheral,
          _placeholder,
          _peripheral_names,
          _split_text_fields ) ),
      population_( _split_text_fields ? _population : VocabForDf() ),
      subtrees_( parse_subtrees(
          _population,
          _peripheral,
          _placeholder,
          _peripheral_names,
          _split_text_fields ) )
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
    const std::vector<std::string>& _peripheral_names,
    const bool _split_text_fields )
{
    if ( _split_text_fields )
        {
            auto result =
                std::vector<VocabForDf>( _placeholder.joined_tables_.size() );

            for ( const auto& p : _population )
                {
                    result.push_back( { p } );
                }

            return result;
        }

    const auto extract_peripheral = std::bind(
        find_peripheral,
        _peripheral,
        std::placeholders::_1,
        _peripheral_names );

    const auto range = _placeholder.joined_tables_ |
                       std::ranges::views::transform( extract_peripheral );

    return stl::make::vector<VocabForDf>( range );
}

// ----------------------------------------------------------------------------

std::vector<std::optional<VocabularyTree>> VocabularyTree::parse_subtrees(
    const VocabForDf& _population,
    const std::vector<VocabForDf>& _peripheral,
    const Placeholder& _placeholder,
    const std::vector<std::string>& _peripheral_names,
    const bool _split_text_fields )
{
    const auto extract_peripheral = std::bind(
        find_peripheral,
        _peripheral,
        std::placeholders::_1,
        _peripheral_names );

    const auto make_subtree =
        [&_peripheral,
         &_peripheral_names,
         extract_peripheral,
         _split_text_fields](
            const Placeholder& _p ) -> std::optional<VocabularyTree> {
        if ( !_split_text_fields && _p.joined_tables_.size() == 0 )
            {
                return std::nullopt;
            }

        const auto new_population = extract_peripheral( _p );

        if ( _split_text_fields && new_population.size() == 0 )
            {
                return std::nullopt;
            }

        return VocabularyTree(
            new_population,
            _peripheral,
            _p,
            _peripheral_names,
            _split_text_fields );
    };

    const auto range = _placeholder.joined_tables_ |
                       std::ranges::views::transform( make_subtree );

    auto subtrees = stl::make::vector<std::optional<VocabularyTree>>( range );

    if ( !_split_text_fields )
        {
            return subtrees;
        }

    for ( size_t i = 0; i < _population.size(); ++i )
        {
            subtrees.push_back( std::nullopt );
        }

    return subtrees;
}

// ----------------------------------------------------------------------------
}  // namespace helpers

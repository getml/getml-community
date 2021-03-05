#include "helpers/helpers.hpp"

namespace helpers
{
// ----------------------------------------------------------------------------

WordIndexContainer::WordIndexContainer(
    const DataFrame& _population,
    const std::vector<DataFrame>& _peripheral,
    const VocabularyContainer& _vocabulary_container )
{
    assert_true(
        _vocabulary_container.peripheral().size() == _peripheral.size() );

    for ( size_t i = 0; i < _peripheral.size(); ++i )
        {
            peripheral_.push_back( make_word_indices(
                _vocabulary_container.peripheral().at( i ),
                _peripheral.at( i ) ) );
        }

    population_ =
        make_word_indices( _vocabulary_container.population(), _population );
}

// ----------------------------------------------------------------------------

WordIndexContainer::WordIndexContainer(
    const WordIndices& _population,
    const std::vector<WordIndices>& _peripheral )
    : peripheral_( _peripheral ), population_( _population )
{
}

// ----------------------------------------------------------------------------

WordIndexContainer::~WordIndexContainer() = default;

// ----------------------------------------------------------------------------

VocabularyContainer WordIndexContainer::vocabulary() const
{
    const auto get_vocab =
        []( const std::shared_ptr<const textmining::WordIndex>& _word_index ) {
            assert_true( _word_index );
            return _word_index->vocabulary_ptr();
        };

    const auto extract_vocab_for_df =
        [get_vocab]( const WordIndices& _word_indices ) {
            auto range = _word_indices | std::views::transform( get_vocab );
            return stl::make::vector<
                std::shared_ptr<const std::vector<strings::String>>>( range );
        };

    const auto population = extract_vocab_for_df( population_ );

    auto range = peripheral_ | std::views::transform( extract_vocab_for_df );

    const auto peripheral = stl::make::vector<VocabForDf>( range );

    return VocabularyContainer( population, peripheral );
}

// ----------------------------------------------------------------------------

typename WordIndexContainer::WordIndices WordIndexContainer::make_word_indices(
    const VocabForDf& _vocabulary, const DataFrame& _df ) const
{
    assert_true( _df.text_.size() == _vocabulary.size() );

    const auto make_index = [&_df, &_vocabulary]( const size_t ix ) {
        const auto& col = _df.text_.at( ix );
        const auto& voc = _vocabulary.at( ix );
        return std::make_shared<const textmining::WordIndex>(
            stl::Range( col.begin(), col.end() ), voc );
    };

    const auto iota =
        std::views::iota( static_cast<size_t>( 0 ), _df.text_.size() );

    auto range = iota | std::views::transform( make_index );

    return stl::make::vector<std::shared_ptr<const textmining::WordIndex>>(
        range );
}

// ----------------------------------------------------------------------------
}  // namespace helpers

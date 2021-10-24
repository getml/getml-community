#include "textmining/textmining.hpp"

namespace textmining
{
// ----------------------------------------------------------------------------

WordIndex::WordIndex(
    const stl::Range<const strings::String*>& _range,
    const std::shared_ptr<const std::vector<strings::String>>& _vocabulary )
    : nrows_( static_cast<size_t>(
          std::distance( _range.begin(), _range.end() ) ) ),
      vocabulary_( _vocabulary )
{
    std::tie( indptr_, words_ ) = make_indptr_and_words( _range );
}

// ----------------------------------------------------------------------------

WordIndex::~WordIndex() = default;

// ----------------------------------------------------------------------------

std::pair<std::vector<size_t>, std::vector<Int>>
WordIndex::make_indptr_and_words(
    const stl::Range<const strings::String*>& _range ) const
{
    // ----------------------------------------------------------------

    const auto voc_range = stl::Range<const strings::String*>(
        vocabulary().data(), vocabulary().data() + vocabulary().size() );

    const auto voc_map = Vocabulary::to_map( voc_range );

    // ----------------------------------------------------------------

    const auto to_number = [&voc_map]( const strings::String& word ) -> Int {
        const auto it = voc_map.find( word );
        if ( it == voc_map.end() )
            {
                return -1;
            }
        return it->second;
    };

    // ----------------------------------------------------------------

    const auto in_vocabulary = []( const Int val ) -> bool { return val >= 0; };

    // ----------------------------------------------------------------

    auto indptr = std::vector<size_t>( { 0 } );

    auto words = std::vector<Int>( 0 );

    for ( const auto& textfield : _range )
        {
            const auto processed = Vocabulary::process_text_field( textfield );

            auto range = processed | VIEWS::transform( to_number ) |
                         VIEWS::filter( in_vocabulary );

            size_t num_words = 0;

            for ( const auto& word_ix : range )
                {
                    words.push_back( word_ix );
                    ++num_words;
                }

            std::sort(
                words.begin() + indptr.back(),
                words.begin() + indptr.back() + num_words );

            indptr.push_back( indptr.back() + num_words );
        }

    return std::make_pair( indptr, words );
}

// ----------------------------------------------------------------------------
}  // namespace textmining

#include "textmining/textmining.hpp"

namespace textmining
{
// ----------------------------------------------------------------------------

std::vector<std::pair<strings::String, size_t>> Vocabulary::count_df(
    const stl::Range<const strings::String*> _range )
{
    const auto process =
        []( const strings::String& _text_field ) -> std::set<std::string> {
        return Vocabulary::process_text_field( _text_field );
    };

    const auto range = _range | std::views::transform( process );

    const auto df_map = make_map( range );

    using Pair = std::pair<strings::String, size_t>;

    auto df_vec = std::vector<Pair>( df_map.begin(), df_map.end() );

    const auto by_count = []( const Pair& p1, const Pair& p2 ) -> bool {
        return p1.second > p2.second;
    };

    std::sort( df_vec.begin(), df_vec.end(), by_count );

    return df_vec;
}

// ----------------------------------------------------------------------------

std::shared_ptr<const std::vector<strings::String>> Vocabulary::generate(
    const size_t _min_df,
    const size_t _max_size,
    const stl::Range<const strings::String*> _range )
{
    using Pair = std::pair<strings::String, size_t>;

    const auto df_count = count_df( _range );

    const auto count_greater_than_min_df = [_min_df]( const Pair& p ) -> bool {
        return p.second >= _min_df;
    };

    const auto get_first = []( const Pair& p ) -> strings::String {
        return p.first;
    };

    const auto range =
        df_count | std::views::filter( count_greater_than_min_df ) |
        std::views::transform( get_first ) | std::views::take( _max_size );

    auto vocab = std::make_shared<std::vector<strings::String>>(
        stl::collect::vector<strings::String>( range ) );

    std::sort( vocab->begin(), vocab->end() );

    return vocab;
}

// ----------------------------------------------------------------------------

std::set<std::string> Vocabulary::process_text_field(
    const strings::String& _text_field )
{
    const auto make_unique =
        []( const std::vector<std::string>& _tokens ) -> std::set<std::string> {
        return std::set<std::string>( _tokens.begin(), _tokens.end() );
    };

    return make_unique( split_text_field( _text_field ) );
}

// ----------------------------------------------------------------------------

std::vector<std::string> Vocabulary::split_text_field(
    const strings::String& _text_field )
{
    const auto is_non_empty = []( const std::string& str ) -> bool {
        return str.size() > 0;
    };

    const auto remove_empty =
        [is_non_empty](
            const std::vector<std::string>& vec ) -> std::vector<std::string> {
        const auto range = vec | std::views::filter( is_non_empty );
        return stl::collect::vector<std::string>( range );
    };

    const auto splitted = StringSplitter::split( _text_field.to_lower().str() );

    return remove_empty( splitted );
}

// ----------------------------------------------------------------------------

std::map<strings::String, Int> Vocabulary::to_map(
    const stl::Range<const strings::String*> _range )
{
    Int value = 0;

    auto m = std::map<strings::String, Int>();

    for ( const auto& key : _range )
        {
            m[key] = value++;
        }

    return m;
}

// ----------------------------------------------------------------------------
}  // namespace textmining

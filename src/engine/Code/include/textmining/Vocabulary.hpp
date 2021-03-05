#ifndef TEXTMINING_VOCABULARY_HPP_
#define TEXTMINING_VOCABULARY_HPP_

namespace textmining
{
// -------------------------------------------------------------------------

class Vocabulary
{
   public:
    /// Generates the vocabulary based on a column.
    static std::shared_ptr<const std::vector<strings::String>> generate(
        const size_t _min_df,
        const size_t _max_size,
        const stl::Range<const strings::String*> _range );

    /// Processes a single text field to extract a set of unique words.
    static std::set<std::string> process_text_field(
        const strings::String& _text_field );

    /// Splits a single text field to extract a vector of words.
    static std::vector<std::string> split_text_field(
        const strings::String& _text_field );

    /// Generates an unordered_map for the vocabulary.
    static std::map<strings::String, Int> to_map(
        const stl::Range<const strings::String*> _range );

   private:
    /// Counts the document frequency for each individual word.
    static std::vector<std::pair<strings::String, size_t>> count_df(
        const stl::Range<const strings::String*> _range );

    /// Counts the document frequency for each individual word.
    template <class RangeType>
    static std::map<strings::String, size_t> make_map( const RangeType& _range )
    {
        std::map<strings::String, size_t> df_map;

        for ( const auto& unique_tokens : _range )
            {
                for ( const auto& token_str : unique_tokens )
                    {
                        const auto token = strings::String( token_str );

                        const auto it = df_map.find( token );

                        if ( it == df_map.end() )
                            {
                                df_map[token] = 1;
                            }
                        else
                            {
                                it->second++;
                            }
                    }
            }

        return df_map;
    }
};

// -------------------------------------------------------------------------
}  // namespace textmining

#endif  // TEXTMINING_VOCABULARY_HPP_

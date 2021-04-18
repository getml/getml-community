#ifndef HELPERS_TEXTFIELDSPLITTER_HPP_
#define HELPERS_TEXTFIELDSPLITTER_HPP_

// ----------------------------------------------------------------------------

namespace helpers
{
// ----------------------------------------------------------------------------

class TextFieldSplitter
{
   public:
    /// Modifies the vocabulary container to reverse the effect of the text
    /// field splitting.
    static VocabularyContainer reverse(
        const VocabularyContainer& _vocab,
        const Placeholder& _population_schema,
        const std::vector<Placeholder>& _peripheral_schema );

    /// Splits up all text fields into individual words and puts them in a
    /// separate data frame.
    static std::pair<DataFrame, std::vector<DataFrame>> split_text_fields(
        const DataFrame& _population_df,
        const std::vector<DataFrame>& _peripheral_dfs,
        const std::shared_ptr<const logging::AbstractLogger>& _logger );

   private:
    /// Adds a rowid to the data frame.
    static DataFrame add_rowid( const DataFrame& _df );

    /// Counts the total number of text fields.
    static size_t count_text_fields(
        const DataFrame& _population_df,
        const std::vector<DataFrame>& _peripheral_dfs );

    /// Generates a new peripheral data frame.
    static DataFrame make_new_df(
        const std::string& _df_name, const Column<strings::String>& _col );

    /// Returns the same data frame, but with the text fields removed.
    static DataFrame remove_text_fields( const DataFrame& _df );

    /// Splits the text field in an individual column.
    static std::pair<Column<Int>, Column<strings::String>>
    split_text_fields_on_col( const Column<strings::String>& _col );

    /// Splits up all text fields in a particular data frame into individual
    /// words and puts them in a separate data frame.
    static void split_text_fields_on_df(
        const DataFrame& _df,
        std::vector<DataFrame>* _peripheral_dfs,
        logging::ProgressLogger* _progress_logger );
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_TEXTFIELDSPLITTER_HPP_

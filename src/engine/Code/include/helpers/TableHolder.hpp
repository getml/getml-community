#ifndef HELPERS_TABLEHOLDER_HPP_
#define HELPERS_TABLEHOLDER_HPP_

namespace helpers
{
// ----------------------------------------------------------------------------

struct TableHolder
{
    typedef typename DataFrame::AdditionalColumns AdditionalColumns;

    typedef typename RowIndexContainer::RowIndices RowIndices;

    typedef typename WordIndexContainer::WordIndices WordIndices;

    TableHolder(
        const Placeholder& _placeholder,
        const DataFrameView& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const std::optional<RowIndexContainer>& _row_index_container =
            std::nullopt,
        const std::optional<WordIndexContainer>& _word_index_container =
            std::nullopt,
        const std::optional<const MappedContainer>& _mapped = std::nullopt,
        const std::optional<const FeatureContainer>& _feature_container =
            std::nullopt );

    ~TableHolder();

    // ------------------------------

    /// Counts the number of peripheral tables that have been created from text
    /// fields.
    static size_t count_text( const std::vector<DataFrame>& _peripheral );

    /// Identifies the index for the associated peripheral table.
    static size_t find_peripheral_ix(
        const std::vector<std::string>& _peripheral_names,
        const std::string& _name );

    /// Creates the row indices for the subtables.
    static std::shared_ptr<const std::vector<size_t>> make_subrows(
        const DataFrameView& _population_subview,
        const DataFrame& _peripheral_subview );

    /// Creates the main tables during construction.
    static std::vector<DataFrameView> parse_main_tables(
        const Placeholder& _placeholder,
        const DataFrameView& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::optional<RowIndexContainer>& _row_index_container,
        const std::optional<WordIndexContainer>& _word_index_container,
        const std::optional<const FeatureContainer>& _feature_container );

    /// Creates the peripheral tables during construction.
    static std::vector<DataFrame> parse_peripheral_tables(
        const Placeholder& _placeholder,
        const DataFrameView& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const std::optional<RowIndexContainer>& _row_index_container,
        const std::optional<WordIndexContainer>& _word_index_container,
        const std::optional<const MappedContainer>& _mapped,
        const std::optional<const FeatureContainer>& _feature_container );

    /// Parses the propositionalization flag in the Placeholder
    static std::vector<bool> parse_propositionalization(
        const Placeholder& _placeholder, const size_t _expected_size );

    /// Creates the subtables during construction.
    static std::vector<std::optional<TableHolder>> parse_subtables(
        const Placeholder& _placeholder,
        const DataFrameView& _population,
        const std::vector<DataFrame>& _peripheral,
        const std::vector<std::string>& _peripheral_names,
        const std::optional<RowIndexContainer>& _row_index_container,
        const std::optional<WordIndexContainer>& _word_index_container,
        const std::optional<const MappedContainer>& _mapped );

    /// Extracts the wors indices from the tables.
    WordIndexContainer word_indices() const;

    // ------------------------------

    /// The TableHolder has a population table, which may or may not be
    /// identical with the actual population table.
    const std::vector<DataFrameView> main_tables_;

    /// The TableHolder can have peripheral tables.
    const std::vector<DataFrame> peripheral_tables_;

    /// Whether we want to use propsitionalization on a particular relationship.
    const std::vector<bool> propositionalization_;

    /// The TableHolder may or may not have subtables.
    const std::vector<std::optional<TableHolder>> subtables_;
};

// ----------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_TABLEHOLDER_HPP_

#ifndef HELPERS_ROWINDEXCONTAINER_HPP_
#define HELPERS_ROWINDEXCONTAINER_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class RowIndexContainer
{
   public:
    typedef typename VocabularyContainer::VocabForDf VocabForDf;

    typedef std::vector<std::shared_ptr<const textmining::RowIndex>> RowIndices;

    typedef typename WordIndexContainer::WordIndices WordIndices;

   public:
    RowIndexContainer( const WordIndexContainer& _word_index_container );

    RowIndexContainer(
        const RowIndices& _population,
        const std::vector<RowIndices>& _peripheral );

    ~RowIndexContainer();

   public:
    /// Trivial (const) accessor
    const std::vector<RowIndices>& peripheral() const { return peripheral_; }

    /// Trivial (const) accessor
    const RowIndices& population() const { return population_; }

   private:
    /// Generates the row indices for all text columns in a data frame.
    RowIndices make_row_indices( const WordIndices& _word_indices ) const;

   private:
    /// The vocabulary for the peripheral tables.
    std::vector<RowIndices> peripheral_;

    /// The vocabulary for the population table.
    RowIndices population_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_ROWINDEXCONTAINER_HPP_

#ifndef HELPERS_VOCABULARYTREE_HPP_
#define HELPERS_VOCABULARYTREE_HPP_

namespace helpers
{
// -------------------------------------------------------------------------

class VocabularyTree
{
   public:
    typedef std::vector<std::shared_ptr<const std::vector<strings::String>>>
        VocabForDf;

   public:
    VocabularyTree(
        const VocabForDf& _population,
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names );

    ~VocabularyTree();

   public:
    /// Trivial (const) accessor
    const std::vector<VocabForDf>& peripheral() const { return peripheral_; }

    /// Trivial (const) accessor
    const VocabForDf& population() const { return population_; }

    /// Trivial (const) accessor
    const std::vector<std::optional<VocabularyTree>>& subtrees() const
    {
        return subtrees_;
    }

   private:
    /// Identifies the index for the associated peripheral table.
    static VocabForDf find_peripheral(
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names );

    /// Parses the vocabulary for the peripheral tables.
    static std::vector<VocabForDf> parse_peripheral(
        const VocabForDf& _population,
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names );

    /// Parses the subtrees.
    static std::vector<std::optional<VocabularyTree>> parse_subtrees(
        const VocabForDf& _population,
        const std::vector<VocabForDf>& _peripheral,
        const Placeholder& _placeholder,
        const std::vector<std::string>& _peripheral_names );

   private:
    /// The vocabulary for the peripheral tables.
    const std::vector<VocabForDf> peripheral_;

    /// The vocabulary for the population table.
    const VocabForDf population_;

    /// Vocabulary used for any subholders.
    const std::vector<std::optional<VocabularyTree>> subtrees_;
};

// -------------------------------------------------------------------------
}  // namespace helpers

#endif  // HELPERS_VOCABULARYTREE_HPP_

